import datetime
import math
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models import CollectTask, FlowStep
from backend.services.ai_engine import calculate_pre_regions

router = APIRouter(prefix="/api/flows", tags=["flows"])


class CreateTaskReq(BaseModel):
    name: str
    table_name: str
    task_type: str = "full+incremental"
    cluster: str = "hh-fed-sub18"
    namespace: str = "ctg363566671677_hh_fed_sub19_cjzh_cbss_hbase_lb19"
    tenant: str = "ctg363566671677"
    workspace: str = "hh_fed_sub18_cjzh_cbss_lb18"
    work_group: str = "cjzh_cbss_lb18_wg"
    zk_hosts: str = "10.177.138.67,10.177.138.68,10.177.138.69"
    zk_parent: str = "/hbasesub19"
    zk_port: int = 2181
    kafka_brokers: str = (
        "10.177.64.59:32001,10.177.64.58:32001,10.177.105.150:32003,"
        "10.177.105.152:32010,10.177.38.124:32003,10.177.38.118:32005"
    )
    kafka_topic: str = "tprds-dc-i-prods-new"
    field_count: int = 22
    field_list: str = ""
    pk_indexes: str = "0,4"
    file_size_gb: float = 1.0
    compression: str = "gz"
    init_date: str = ""
    interface_id: str = ""
    date_field_indexes: str = ""
    mc_table_name: str = ""


class ConfirmOpReq(BaseModel):
    operation_id: Optional[str] = None


STEP_COMMAND_TEMPLATES: List[Dict[str, Any]] = [
    {
        "step_order": 1,
        "name": "确定源端表结构及 MC 表结构",
        "automation": "auto",
        "operations": [
            {
                "id": "1.1",
                "name": "拉取源端元数据",
                "type": "auto",
                "command": None,
                "confirm_required": False,
            },
            {
                "id": "1.2",
                "name": "生成字段映射",
                "type": "auto",
                "command": None,
                "confirm_required": False,
            },
            {
                "id": "1.3",
                "name": "生成 MC 目标表 DDL",
                "type": "auto",
                "command": None,
                "confirm_required": True,
            },
        ],
    },
    {
        "step_order": 2,
        "name": "新建 HBase 表",
        "automation": "auto",
        "operations": [
            {
                "id": "2.1",
                "name": "计算预分区数",
                "type": "auto",
                "command": None,
                "confirm_required": False,
                "output_template": (
                    "预分区数 = ceil({file_size_gb} × 1024 / 3 / 4) = {pre_regions}"
                ),
            },
            {
                "id": "2.2",
                "name": "执行建表命令",
                "type": "auto",
                "command": (
                    "java -cp /data/disk01/shangyunOrder/lib/QueryHbaseTable.jar "
                    "cn.com.bonc.CreateTable {zk_hosts} {zk_parent} {pre_regions} "
                    "{compression} {namespace}:{table_name} {zk_port}"
                ),
                "confirm_required": True,
            },
        ],
    },
    {
        "step_order": 3,
        "name": "全量初始化（历史全量文件）",
        "automation": "semi-auto",
        "operations": [
            {
                "id": "3.1",
                "name": "创建 HDFS 初始化目录",
                "type": "auto",
                "command": (
                    "hadoop fs -mkdir -p /user/tenants/{tenant}/{workspace}/work/"
                    "{work_group}/init/data{init_date}/{table_name}/new"
                ),
                "confirm_required": False,
            },
            {
                "id": "3.2",
                "name": "调整 HDFS 目录权限",
                "type": "auto",
                "command": (
                    "hadoop fs -chmod -R 755 /user/tenants/{tenant}/{workspace}/work/"
                    "{work_group}/init/data{init_date}/{table_name}/new"
                ),
                "confirm_required": False,
            },
            {
                "id": "3.3",
                "name": "上传初始化文件到 HDFS",
                "type": "manual",
                "command": (
                    "hadoop fs -put /data/disk01/{workspace}/lh/chushihua/"
                    "d_cred_{table_name}_{init_date}.txt /user/tenants/{tenant}/"
                    "{workspace}/work/{work_group}/init/data{init_date}/"
                    "{table_name}/new"
                ),
                "confirm_required": True,
            },
            {
                "id": "3.4",
                "name": "执行 BulkLoad 初始化入库",
                "type": "semi-auto",
                "command": (
                    "nohup sh /data/disk01/{workspace}/shangyunOrder/chushihuaruku/"
                    "prepare_complete_bulkload_pb.sh /user/tenants/{tenant}/"
                    "{workspace}/work/{work_group}/init/data{init_date}/"
                    "{table_name}/new/ /user/tenants/{tenant}/{workspace}/work/"
                    "{work_group}/sjml/hfileTable/{table_name}/ *{table_name}* "
                    '{field_count} "{pk_indexes}" 0 NO {namespace}:{table_name} '
                    '"{all_field_indexes}" > /data/disk01/{workspace}/shangyunOrder/'
                    "chushihuaruku/log/{table_name}.log 2>&1 &"
                ),
                "confirm_required": True,
            },
        ],
    },
    {
        "step_order": 4,
        "name": "Flink 增量还原入库",
        "automation": "auto",
        "operations": [
            {
                "id": "4.1",
                "name": "生成 Flink 配置文件",
                "type": "auto",
                "command": None,
                "output_template": (
                    "checkPointPath=hdfs://{cluster}/user/tenants/{tenant}/"
                    "{workspace}/work/{work_group}/{workspace}/shangyunCheckpoint/"
                    "checkPoint0_new_{table_name}\n"
                    "sourceBroker={kafka_brokers}\n"
                    "resetState=earliest\n"
                    "groupId=cb2i_r_cjzh_new_{table_name}\n"
                    "userName=cbss_2i_k\n"
                    "password=******\n"
                    "jobName=new_{table_name}\n"
                    "sourceTopic={kafka_topic}\n"
                    "timesKafka={times_kafka}\n"
                    "hbaseInfo={table_name_upper}={pk_indexes}|{field_count}\n"
                    "tableIndexName={table_name_upper}|{field_list}\n"
                    "hbasezk={zk_hosts}\n"
                    "hbaseZookeeperPort={zk_port}\n"
                    "hbaseParent={zk_parent_clean}\n"
                    "namespace={namespace}\n"
                    "tableEnd=\n"
                    "defaultFS={cluster}"
                ),
                "confirm_required": True,
            },
            {
                "id": "4.2",
                "name": "在 BDI 上建立采集还原流程",
                "type": "semi-auto",
                "command": None,
                "output_template": (
                    "任务名称: new_{table_name}\n"
                    "运行平台: 采集整合联邦18集群1.11\n"
                    "运行模式: YARN_PER\n"
                    "配置文件: /data/disk01/{workspace}/shangyunOrder/conf/"
                    "{table_name}.properties"
                ),
                "confirm_required": True,
            },
        ],
    },
    {
        "step_order": 5,
        "name": "HBase 数据导出到 HDFS",
        "automation": "semi-auto",
        "operations": [
            {
                "id": "5.1",
                "name": "修改导出参数配置文件",
                "type": "manual",
                "command": None,
                "output_template": (
                    "{interface_id}: BC099 4096 4096 {tenant}_{workspace}"
                ),
                "confirm_required": True,
            },
            {
                "id": "5.2",
                "name": "新建 Groovy 字段映射脚本",
                "type": "semi-auto",
                "command": None,
                "output_template": (
                    "Class dateTransform = "
                    "bonc.cbss.hbase.scheme.transform.DateTransform\n\n"
                    "select {{\n"
                    "    transform([{date_field_indexes}], clazz: dateTransform)\n"
                    "}}\n\n"
                    'from("{table_name_upper}"){{\n'
                    '    table("{namespace}:{table_name}")\n'
                    "}}"
                ),
                "confirm_required": True,
            },
            {
                "id": "5.3",
                "name": "启动导出脚本",
                "type": "auto",
                "command": "sh etl-export-submit.sh {interface_id} {init_date}",
                "confirm_required": True,
            },
            {
                "id": "5.4",
                "name": "校验导出结果",
                "type": "manual",
                "command": None,
                "confirm_required": True,
            },
        ],
    },
    {
        "step_order": 6,
        "name": "HDFS 导出到 MC",
        "automation": "semi-auto",
        "operations": [
            {
                "id": "6.1",
                "name": "建立 MC 外表",
                "type": "semi-auto",
                "command": None,
                "confirm_required": True,
            },
            {
                "id": "6.2",
                "name": "建立 MC 内表",
                "type": "semi-auto",
                "command": None,
                "confirm_required": True,
            },
            {
                "id": "6.3",
                "name": "执行 HDFS→MC 导出脚本",
                "type": "auto",
                "command": (
                    "sh hdfscp.sh {interface_id} {init_date} /user/tenants/"
                    "{tenant}/{workspace}/work/{work_group}/cbssdata {init_month} "
                    "{init_day} {mc_table_name}"
                ),
                "confirm_required": True,
            },
            {
                "id": "6.4",
                "name": "验证 MC 数据量",
                "type": "manual",
                "command": None,
                "confirm_required": True,
            },
        ],
    },
]


def render_templates(
    templates: List[Dict[str, Any]], vars_dict: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Replace {var} placeholders in all command and output_template fields."""
    rendered: List[Dict[str, Any]] = []
    for step in templates:
        s = {**step, "operations": []}
        for op in step["operations"]:
            o = {**op}
            for key in ("command", "output_template"):
                val = o.get(key)
                if val:
                    try:
                        o[key] = val.format(**vars_dict)
                    except (KeyError, IndexError, ValueError):
                        pass
            s["operations"].append(o)
        rendered.append(s)
    return rendered


def _build_vars_dict(req: CreateTaskReq) -> Dict[str, Any]:
    pre_regions = calculate_pre_regions(
        int(req.file_size_gb * 1024**3), req.compression
    )
    init_date = req.init_date or ""
    base: Dict[str, Any]
    if hasattr(req, "model_dump"):
        base = req.model_dump()
    else:
        base = req.dict()
    return {
        **base,
        "pre_regions": pre_regions,
        "table_name_upper": req.table_name.upper(),
        "zk_parent_clean": req.zk_parent.lstrip("/"),
        "times_kafka": datetime.datetime.now().strftime("%Y%m%d%H%M"),
        "all_field_indexes": ",".join(str(i) for i in range(req.field_count)),
        "init_month": init_date[:6] if init_date else "",
        "init_day": init_date[6:8] if len(init_date) >= 8 else "",
    }


def _advance_task_to_next_step(
    task: CollectTask, step_order: int, db: Session, now: datetime.datetime
) -> None:
    next_row = (
        db.query(FlowStep)
        .filter(
            FlowStep.task_id == task.id,
            FlowStep.step_order == step_order + 1,
        )
        .first()
    )
    if next_row:
        next_row.status = "running"
        next_row.started_at = now
        task.current_step = step_order + 1
        denom = task.total_steps or 1
        task.progress = min(95, int(step_order / denom * 100))
    else:
        task.status = "completed"
        task.progress = 100
        task.finished_at = now
        task.current_step = task.total_steps


@router.get("")
def list_tasks(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=200),
    db: Session = Depends(get_db),
):
    q = db.query(CollectTask).order_by(CollectTask.created_at.desc())
    total = q.count()
    rows = q.offset((page - 1) * page_size).limit(page_size).all()
    total_pages = math.ceil(total / page_size) if page_size else 0
    return {
        "items": [
            {
                "id": t.id,
                "name": t.name,
                "table_name": t.table_name,
                "task_type": t.task_type,
                "status": t.status,
                "progress": t.progress,
                "current_step": t.current_step,
                "total_steps": t.total_steps,
                "cluster": t.cluster,
                "created_at": str(t.created_at) if t.created_at else None,
                "started_at": str(t.started_at) if t.started_at else None,
                "finished_at": str(t.finished_at) if t.finished_at else None,
            }
            for t in rows
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


@router.post("")
def create_task(req: CreateTaskReq, db: Session = Depends(get_db)):
    vars_dict = _build_vars_dict(req)
    rendered_steps = render_templates(STEP_COMMAND_TEMPLATES, vars_dict)

    task = CollectTask(
        name=req.name,
        table_name=req.table_name,
        task_type=req.task_type,
        cluster=req.cluster,
        namespace=req.namespace,
        config_snapshot={
            "vars_dict": vars_dict,
            "rendered_steps": rendered_steps,
        },
    )
    task.total_steps = len(STEP_COMMAND_TEMPLATES)
    db.add(task)
    db.flush()

    for step_tpl in rendered_steps:
        ops: List[Dict[str, Any]] = []
        for op in step_tpl["operations"]:
            ops.append({**op, "status": "pending"})
        step = FlowStep(
            task_id=task.id,
            step_order=step_tpl["step_order"],
            name=step_tpl["name"],
            description="",
            sub_steps=ops,
            automation=step_tpl["automation"],
        )
        db.add(step)

    db.commit()
    db.refresh(task)
    return {
        "id": task.id,
        "message": "采集任务已创建",
        "pre_regions": vars_dict["pre_regions"],
    }


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
        "id": task.id,
        "name": task.name,
        "table_name": task.table_name,
        "task_type": task.task_type,
        "status": task.status,
        "progress": task.progress,
        "current_step": task.current_step,
        "total_steps": task.total_steps,
        "cluster": task.cluster,
        "namespace": task.namespace,
        "config_snapshot": task.config_snapshot or {},
        "error_message": task.error_message,
        "created_at": str(task.created_at) if task.created_at else None,
        "started_at": str(task.started_at) if task.started_at else None,
        "finished_at": str(task.finished_at) if task.finished_at else None,
        "steps": [
            {
                "id": s.id,
                "step_order": s.step_order,
                "name": s.name,
                "description": s.description,
                "automation": s.automation,
                "status": s.status,
                "log": s.log or "",
                "operations": [
                    {
                        "id": op.get("id"),
                        "name": op.get("name"),
                        "type": op.get("type"),
                        "command": op.get("command"),
                        "output_template": op.get("output_template"),
                        "confirm_required": op.get("confirm_required", False),
                        "status": op.get("status", "pending"),
                    }
                    for op in (s.sub_steps or [])
                ],
            }
            for s in steps
        ],
    }


@router.post("/{task_id}/execute")
def execute_task(task_id: int, db: Session = Depends(get_db)):
    task = db.query(CollectTask).filter(CollectTask.id == task_id).first()
    if not task:
        raise HTTPException(404, "任务不存在")
    now = datetime.datetime.utcnow()
    task.status = "running"
    task.started_at = now
    task.current_step = 1
    task.progress = 10

    steps = (
        db.query(FlowStep)
        .filter(FlowStep.task_id == task_id)
        .order_by(FlowStep.step_order)
        .all()
    )
    for i, step in enumerate(steps):
        if i == 0:
            step.status = "running"
            step.started_at = now
        else:
            step.status = "pending"
    db.commit()
    return {"message": "任务开始执行", "status": "running"}


@router.post("/{task_id}/step/{step_order}/confirm")
def confirm_step_operation(
    task_id: int,
    step_order: int,
    body: ConfirmOpReq = ConfirmOpReq(),
    db: Session = Depends(get_db),
):
    task = db.query(CollectTask).filter(CollectTask.id == task_id).first()
    if not task:
        raise HTTPException(404, "任务不存在")
    step = (
        db.query(FlowStep)
        .filter(FlowStep.task_id == task_id, FlowStep.step_order == step_order)
        .first()
    )
    if not step:
        raise HTTPException(404, "步骤不存在")

    ops: List[Dict[str, Any]] = list(step.sub_steps or [])
    pending_indices = [i for i, o in enumerate(ops) if o.get("status") == "pending"]
    if not pending_indices:
        raise HTTPException(400, "没有待确认的操作")

    first_idx = pending_indices[0]
    if body.operation_id is not None and body.operation_id != ops[first_idx].get(
        "id"
    ):
        raise HTTPException(400, "只能按顺序确认当前待处理操作")

    now = datetime.datetime.utcnow()
    ops[first_idx] = {**ops[first_idx], "status": "confirmed"}
    step.sub_steps = ops

    remaining = [o for o in ops if o.get("status") == "pending"]
    if not remaining:
        step.status = "completed"
        step.finished_at = now
        step.log = f"步骤 {step_order} 所有子操作已确认于 {now.isoformat()}"
        _advance_task_to_next_step(task, step_order, db, now)

    db.commit()
    return {
        "message": f"操作 {ops[first_idx].get('id')} 已确认",
        "task_status": task.status,
        "step_status": step.status,
    }


@router.post("/{task_id}/step/{step_order}/complete")
def complete_step(task_id: int, step_order: int, db: Session = Depends(get_db)):
    task = db.query(CollectTask).filter(CollectTask.id == task_id).first()
    if not task:
        raise HTTPException(404, "任务不存在")
    step = (
        db.query(FlowStep)
        .filter(FlowStep.task_id == task_id, FlowStep.step_order == step_order)
        .first()
    )
    if not step:
        raise HTTPException(404, "步骤不存在")

    now = datetime.datetime.utcnow()
    step.status = "completed"
    step.finished_at = now
    step.log = f"步骤 {step_order} 于 {now.isoformat()} 完成"

    next_step = (
        db.query(FlowStep)
        .filter(
            FlowStep.task_id == task_id,
            FlowStep.step_order == step_order + 1,
        )
        .first()
    )
    if next_step:
        next_step.status = "running"
        next_step.started_at = now
        task.current_step = step_order + 1
        denom = task.total_steps or 1
        task.progress = min(95, int(step_order / denom * 100))
    else:
        task.status = "completed"
        task.progress = 100
        task.finished_at = now
        task.current_step = task.total_steps

    db.commit()
    return {"message": f"步骤 {step_order} 已完成", "task_status": task.status}
