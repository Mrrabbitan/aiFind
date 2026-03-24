from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models import MonitorEvent, SystemMetric
from backend.services.ai_engine import analyze_anomaly

router = APIRouter(prefix="/api/monitor", tags=["monitor"])

# task_id -> SLA 定义
SLA_REGISTRY: Dict[int, Dict[str, Any]] = {}


class AnomalyReq(BaseModel):
    metrics: List[float]


class SlaUpsertReq(BaseModel):
    task_id: int
    sla_name: str
    deadline_minutes: int = Field(..., ge=1)
    alert_channels: List[str] = Field(default_factory=list)


class SlaDefinitionOut(BaseModel):
    task_id: int
    sla_name: str
    deadline_minutes: int
    alert_channels: List[str]


class BaselineCheckReq(BaseModel):
    task_id: int
    elapsed_minutes: int = Field(..., ge=0)


class BaselineCheckResp(BaseModel):
    status: str
    remaining_minutes: int
    sla_name: str


def _dim_label(score: int) -> str:
    if score >= 80:
        return "good"
    if score >= 60:
        return "warning"
    return "critical"


def _compute_health_from_metrics(db: Session) -> Dict[str, Any]:
    """基于 SystemMetric 与未解决监控事件计算各维度得分。"""
    rows = (
        db.query(SystemMetric)
        .order_by(SystemMetric.recorded_at.desc())
        .limit(400)
        .all()
    )
    buckets: Dict[str, Dict[str, List[float]]] = {}
    for m in rows:
        src = m.source_system or "unknown"
        name = m.metric_name or ""
        buckets.setdefault(src, {}).setdefault(name, []).append(float(m.metric_value))

    def avg_metric(src: str, metric: str, default: float = 50.0) -> float:
        vals = buckets.get(src, {}).get(metric)
        if not vals:
            return default
        return sum(vals) / len(vals)

    # storage: HDFS 资源压力（用量类指标越低越好）
    hdfs_cpu = avg_metric("HDFS", "cpu_usage", 45)
    hdfs_mem = avg_metric("HDFS", "memory_usage", 48)
    storage_score = int(max(0, min(100, 100 - (hdfs_cpu + hdfs_mem) / 2)))

    # compute: Flink + HBase
    flink_cpu = avg_metric("Flink", "cpu_usage", 50)
    hbase_cpu = avg_metric("HBase", "cpu_usage", 52)
    compute_score = int(max(0, min(100, 100 - (flink_cpu + hbase_cpu) / 2)))

    # network: 全系统延迟 P99（ms），越低越好
    latencies: List[float] = []
    for src in buckets:
        for v in buckets[src].get("latency_p99", []):
            latencies.append(v)
    lat_avg = sum(latencies) / len(latencies) if latencies else 180.0
    network_score = int(max(0, min(100, 100 - min(lat_avg / 25.0, 55))))

    # stability: 未解决告警扣分
    unresolved = (
        db.query(MonitorEvent)
        .filter(MonitorEvent.resolved == 0)
        .all()
    )
    crit = sum(1 for e in unresolved if (e.severity or "").lower() == "critical")
    warn = sum(1 for e in unresolved if (e.severity or "").lower() == "warning")
    stability_score = int(max(0, min(100, 100 - crit * 12 - warn * 4)))

    if not rows:
        return {
            "overall": 87,
            "dimensions": {
                "storage": {"score": 92, "status": "good"},
                "compute": {"score": 85, "status": "good"},
                "network": {"score": 88, "status": "good"},
                "stability": {"score": 83, "status": "warning"},
            },
            "updated_at": datetime.utcnow().isoformat(),
        }

    overall = int(
        round(
            (storage_score + compute_score + network_score + stability_score) / 4.0
        )
    )

    return {
        "overall": overall,
        "dimensions": {
            "storage": {"score": storage_score, "status": _dim_label(storage_score)},
            "compute": {"score": compute_score, "status": _dim_label(compute_score)},
            "network": {"score": network_score, "status": _dim_label(network_score)},
            "stability": {
                "score": stability_score,
                "status": _dim_label(stability_score),
            },
        },
        "updated_at": datetime.utcnow().isoformat(),
    }


@router.get("/events")
def list_events(
    severity: Optional[str] = None,
    page: int = 1,
    page_size: int = 10,
    db: Session = Depends(get_db),
):
    q = db.query(MonitorEvent)
    if severity:
        q = q.filter(MonitorEvent.severity == severity)
    total = q.count()
    total_pages = max(1, -(-total // page_size))
    events = q.order_by(MonitorEvent.created_at.desc()).offset((page - 1) * page_size).limit(page_size).all()
    items = [
        {
            "id": e.id,
            "task_id": e.task_id,
            "event_type": e.event_type,
            "severity": e.severity,
            "title": e.title,
            "detail": e.detail,
            "source_system": e.source_system,
            "resolved": bool(e.resolved),
            "created_at": str(e.created_at) if e.created_at else None,
        }
        for e in events
    ]
    return {"items": items, "total": total, "page": page, "page_size": page_size, "total_pages": total_pages}


@router.get("/metrics")
def list_metrics(
    source: Optional[str] = None,
    limit: int = 50,
    db: Session = Depends(get_db),
):
    q = db.query(SystemMetric)
    if source:
        q = q.filter(SystemMetric.source_system == source)
    rows = q.order_by(SystemMetric.recorded_at.desc()).limit(limit).all()
    return [
        {
            "id": m.id,
            "metric_name": m.metric_name,
            "metric_value": m.metric_value,
            "unit": m.unit,
            "source_system": m.source_system,
            "recorded_at": str(m.recorded_at) if m.recorded_at else None,
        }
        for m in rows
    ]


@router.post("/anomaly-detect")
def detect_anomaly(req: AnomalyReq):
    return analyze_anomaly(req.metrics)


@router.post("/events/{event_id}/resolve")
def resolve_event(event_id: int, db: Session = Depends(get_db)):
    evt = db.query(MonitorEvent).filter(MonitorEvent.id == event_id).first()
    if evt:
        evt.resolved = 1
        db.commit()
    return {"message": "已标记为已解决"}


@router.post("/sla")
def upsert_sla(req: SlaUpsertReq):
    SLA_REGISTRY[req.task_id] = {
        "sla_name": req.sla_name,
        "deadline_minutes": req.deadline_minutes,
        "alert_channels": list(req.alert_channels),
    }
    return {"ok": True, "task_id": req.task_id}


@router.get("/sla", response_model=List[SlaDefinitionOut])
def list_sla_definitions():
    return [
        SlaDefinitionOut(
            task_id=tid,
            sla_name=data["sla_name"],
            deadline_minutes=data["deadline_minutes"],
            alert_channels=list(data.get("alert_channels", [])),
        )
        for tid, data in SLA_REGISTRY.items()
    ]


@router.post("/baseline-check", response_model=BaselineCheckResp)
def baseline_check(req: BaselineCheckReq):
    data = SLA_REGISTRY.get(req.task_id)
    if not data:
        raise HTTPException(status_code=404, detail="该任务未配置 SLA")
    deadline = int(data["deadline_minutes"])
    sla_name = str(data["sla_name"])
    remaining = max(0, deadline - req.elapsed_minutes)
    if req.elapsed_minutes >= deadline:
        status = "breached"
    elif deadline > 0 and remaining <= max(1, int(deadline * 0.2)):
        status = "warning"
    else:
        status = "on_track"
    return BaselineCheckResp(
        status=status,
        remaining_minutes=remaining,
        sla_name=sla_name,
    )


@router.get("/dependency-chain/{task_id}")
def dependency_chain(task_id: int):
    return {
        "task_id": task_id,
        "chain": [
            {
                "node": "Kafka Topic",
                "system": "Kafka",
                "status": "healthy",
                "latency_ms": 120,
            },
            {
                "node": "Flink Consumer",
                "system": "Flink",
                "status": "healthy",
                "latency_ms": 450,
            },
            {
                "node": "HBase Write",
                "system": "HBase",
                "status": "healthy",
                "latency_ms": 230,
            },
            {
                "node": "HDFS Export",
                "system": "HDFS",
                "status": "healthy",
                "latency_ms": 1800,
            },
            {
                "node": "MC Push",
                "system": "MC",
                "status": "healthy",
                "latency_ms": 3200,
            },
        ],
    }


@router.get("/health-score")
def health_score(db: Session = Depends(get_db)):
    return _compute_health_from_metrics(db)
