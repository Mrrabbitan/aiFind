import datetime as _dt
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from backend.database import get_db
from backend.models import CollectTask, FlowStep
from backend.services.ai_engine import (
    STEP_TEMPLATES,
    generate_hbase_create_cmd,
    generate_flink_config,
    generate_bulkload_cmd,
    calculate_pre_regions,
)

router = APIRouter(prefix="/api/flows", tags=["flows"])


class CreateTaskReq(BaseModel):
    name: str
    table_name: str
    task_type: str = "full+incremental"
    cluster: str = "hh-fed-sub18"
    namespace: str = "ctg363566671677_hh_fed_sub19_cjzh_cbss_hbase_lb19"
    field_count: int = 22
    pk_indexes: str = "0,4"
    file_size_gb: float = 1.0


@router.get("")
def list_tasks(db: Session = Depends(get_db)):
    tasks = db.query(CollectTask).order_by(CollectTask.created_at.desc()).all()
    return [
        {
            "id": t.id, "name": t.name, "table_name": t.table_name,
            "task_type": t.task_type, "status": t.status,
            "progress": t.progress, "current_step": t.current_step,
            "total_steps": t.total_steps, "cluster": t.cluster,
            "created_at": str(t.created_at) if t.created_at else None,
            "started_at": str(t.started_at) if t.started_at else None,
            "finished_at": str(t.finished_at) if t.finished_at else None,
        }
        for t in tasks
    ]


@router.post("")
def create_task(req: CreateTaskReq, db: Session = Depends(get_db)):
    pre_regions = calculate_pre_regions(int(req.file_size_gb * 1024**3))
    hbase_cmd = generate_hbase_create_cmd(
        req.table_name, "10.177.138.67,10.177.138.68,10.177.138.69",
        "/hbasesub19", pre_regions, "gz", req.namespace,
    )
    flink_cfg = generate_flink_config(
        req.table_name, [f"field_{i}" for i in range(req.field_count)],
        req.pk_indexes, req.namespace, req.cluster,
    )
    bulkload_cmd = generate_bulkload_cmd(
        req.table_name,
        f"/user/tenants/.../init/data/{req.table_name}/new",
        f"/user/tenants/.../sjml/hfileTable",
        f"{req.namespace}:{req.table_name}",
        req.field_count,
    )

    task = CollectTask(
        name=req.name, table_name=req.table_name,
        task_type=req.task_type, cluster=req.cluster,
        namespace=req.namespace,
        config_snapshot={
            "pre_regions": pre_regions,
            "hbase_cmd": hbase_cmd,
            "flink_config": flink_cfg,
            "bulkload_cmd": bulkload_cmd,
        },
    )
    db.add(task)
    db.flush()

    for tpl in STEP_TEMPLATES:
        step = FlowStep(
            task_id=task.id,
            step_order=tpl["step_order"],
            name=tpl["name"],
            description=tpl["description"],
            sub_steps=tpl["sub_steps"],
            automation=tpl["automation"],
        )
        db.add(step)

    db.commit()
    db.refresh(task)
    return {"id": task.id, "message": "采集任务已创建", "pre_regions": pre_regions}


@router.get("/{task_id}")
def get_task_detail(task_id: int, db: Session = Depends(get_db)):
    task = db.query(CollectTask).filter(CollectTask.id == task_id).first()
    if not task:
        raise HTTPException(404, "任务不存在")
    steps = (
        db.query(FlowStep)
        .filter(FlowStep.task_id == task_id)
        .order_by(FlowStep.step_order)
        .all()
    )
    return {
        "id": task.id, "name": task.name, "table_name": task.table_name,
        "task_type": task.task_type, "status": task.status,
        "progress": task.progress, "current_step": task.current_step,
        "total_steps": task.total_steps, "cluster": task.cluster,
        "namespace": task.namespace,
        "config_snapshot": task.config_snapshot or {},
        "error_message": task.error_message,
        "created_at": str(task.created_at) if task.created_at else None,
        "started_at": str(task.started_at) if task.started_at else None,
        "finished_at": str(task.finished_at) if task.finished_at else None,
        "steps": [
            {
                "id": s.id, "step_order": s.step_order, "name": s.name,
                "description": s.description, "sub_steps": s.sub_steps or [],
                "status": s.status, "automation": s.automation,
                "log": s.log or "",
            }
            for s in steps
        ],
    }


@router.post("/{task_id}/execute")
def execute_task(task_id: int, db: Session = Depends(get_db)):
    task = db.query(CollectTask).filter(CollectTask.id == task_id).first()
    if not task:
        raise HTTPException(404, "任务不存在")
    now = _dt.datetime.utcnow()
    task.status = "running"
    task.started_at = now
    task.current_step = 1
    task.progress = 10

    steps = (
        db.query(FlowStep).filter(FlowStep.task_id == task_id)
        .order_by(FlowStep.step_order).all()
    )
    for i, step in enumerate(steps):
        if i == 0:
            step.status = "running"
            step.started_at = now
        else:
            step.status = "pending"
    db.commit()
    return {"message": "任务开始执行", "status": "running"}


@router.post("/{task_id}/step/{step_order}/complete")
def complete_step(task_id: int, step_order: int, db: Session = Depends(get_db)):
    task = db.query(CollectTask).filter(CollectTask.id == task_id).first()
    if not task:
        raise HTTPException(404, "任务不存在")
    step = db.query(FlowStep).filter(
        FlowStep.task_id == task_id, FlowStep.step_order == step_order
    ).first()
    if not step:
        raise HTTPException(404, "步骤不存在")

    now = _dt.datetime.utcnow()
    step.status = "completed"
    step.finished_at = now
    step.log = f"步骤 {step_order} 于 {now.isoformat()} 完成"

    next_step = db.query(FlowStep).filter(
        FlowStep.task_id == task_id, FlowStep.step_order == step_order + 1
    ).first()
    if next_step:
        next_step.status = "running"
        next_step.started_at = now
        task.current_step = step_order + 1
        task.progress = min(95, int(step_order / task.total_steps * 100))
    else:
        task.status = "completed"
        task.progress = 100
        task.finished_at = now
        task.current_step = task.total_steps

    db.commit()
    return {"message": f"步骤 {step_order} 已完成", "task_status": task.status}
