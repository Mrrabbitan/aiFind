from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import List, Optional
from backend.database import get_db
from backend.models import MonitorEvent, SystemMetric
from backend.services.ai_engine import analyze_anomaly

router = APIRouter(prefix="/api/monitor", tags=["monitor"])


class AnomalyReq(BaseModel):
    metrics: List[float]


@router.get("/events")
def list_events(severity: Optional[str] = None, limit: int = 30,
                db: Session = Depends(get_db)):
    q = db.query(MonitorEvent)
    if severity:
        q = q.filter(MonitorEvent.severity == severity)
    events = q.order_by(MonitorEvent.created_at.desc()).limit(limit).all()
    return [
        {
            "id": e.id, "task_id": e.task_id, "event_type": e.event_type,
            "severity": e.severity, "title": e.title, "detail": e.detail,
            "source_system": e.source_system, "resolved": bool(e.resolved),
            "created_at": str(e.created_at) if e.created_at else None,
        }
        for e in events
    ]


@router.get("/metrics")
def list_metrics(source: Optional[str] = None, limit: int = 50,
                 db: Session = Depends(get_db)):
    q = db.query(SystemMetric)
    if source:
        q = q.filter(SystemMetric.source_system == source)
    rows = q.order_by(SystemMetric.recorded_at.desc()).limit(limit).all()
    return [
        {
            "id": m.id, "metric_name": m.metric_name,
            "metric_value": m.metric_value, "unit": m.unit,
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
