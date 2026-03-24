from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
from backend.database import get_db
from backend.models import CollectTask, MonitorEvent, KnowledgeArticle, FlowStep

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("")
def get_dashboard(db: Session = Depends(get_db)):
    total_tasks = db.query(CollectTask).count()
    running = db.query(CollectTask).filter(CollectTask.status == "running").count()
    completed = db.query(CollectTask).filter(CollectTask.status == "completed").count()
    failed = db.query(CollectTask).filter(CollectTask.status == "failed").count()
    pending = db.query(CollectTask).filter(CollectTask.status == "pending").count()

    alerts = db.query(MonitorEvent).filter(
        MonitorEvent.severity.in_(["warning", "critical"]),
        MonitorEvent.resolved == 0,
    ).count()

    automation_rate = 0.0
    steps = db.query(FlowStep).all()
    if steps:
        auto_steps = sum(1 for s in steps if s.automation == "auto")
        automation_rate = round(auto_steps / len(steps) * 100, 1)

    success_rate = round(completed / total_tasks * 100, 1) if total_tasks else 0

    recent_tasks = (
        db.query(CollectTask)
        .order_by(CollectTask.created_at.desc())
        .limit(8)
        .all()
    )
    recent_events = (
        db.query(MonitorEvent)
        .order_by(MonitorEvent.created_at.desc())
        .limit(8)
        .all()
    )

    return {
        "stats": {
            "total_tasks": total_tasks,
            "running": running,
            "completed": completed,
            "failed": failed,
            "pending": pending,
            "active_alerts": alerts,
            "automation_rate": automation_rate,
            "success_rate": success_rate,
        },
        "recent_tasks": [
            {
                "id": t.id,
                "name": t.name,
                "table_name": t.table_name,
                "status": t.status,
                "progress": t.progress,
                "current_step": t.current_step,
                "total_steps": t.total_steps,
                "created_at": str(t.created_at) if t.created_at else None,
            }
            for t in recent_tasks
        ],
        "recent_events": [
            {
                "id": e.id,
                "event_type": e.event_type,
                "severity": e.severity,
                "title": e.title,
                "source_system": e.source_system,
                "created_at": str(e.created_at) if e.created_at else None,
            }
            for e in recent_events
        ],
    }
