import datetime as _dt
from sqlalchemy import Column, Integer, String, Text, Float, DateTime, JSON, Enum as SAEnum
from backend.database import Base

_now = _dt.datetime.utcnow


class CollectTask(Base):
    __tablename__ = "collect_tasks"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(200), nullable=False)
    table_name = Column(String(200), nullable=False)
    task_type = Column(String(50), default="full+incremental")
    status = Column(String(30), default="pending")
    progress = Column(Integer, default=0)
    current_step = Column(Integer, default=0)
    total_steps = Column(Integer, default=6)
    cluster = Column(String(100), default="hh-fed-sub18")
    namespace = Column(String(200), default="")
    config_snapshot = Column(JSON, default=dict)
    error_message = Column(Text, default="")
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=_now)


class FlowStep(Base):
    __tablename__ = "flow_steps"

    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(Integer, index=True)
    step_order = Column(Integer)
    name = Column(String(200))
    description = Column(Text, default="")
    sub_steps = Column(JSON, default=list)
    status = Column(String(30), default="pending")
    automation = Column(String(20), default="auto")
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    log = Column(Text, default="")


class ParamTemplate(Base):
    __tablename__ = "param_templates"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(200), nullable=False)
    category = Column(String(50))
    table_pattern = Column(String(200), default="*")
    params = Column(JSON, default=dict)
    description = Column(Text, default="")
    version = Column(Integer, default=1)
    created_at = Column(DateTime, default=_now)
    updated_at = Column(DateTime, default=_now, onupdate=_now)


class MonitorEvent(Base):
    __tablename__ = "monitor_events"

    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(Integer, index=True, nullable=True)
    event_type = Column(String(50))
    severity = Column(String(20), default="info")
    title = Column(String(300))
    detail = Column(Text, default="")
    source_system = Column(String(50), default="")
    resolved = Column(Integer, default=0)
    created_at = Column(DateTime, default=_now)


class KnowledgeArticle(Base):
    __tablename__ = "knowledge_articles"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(300), nullable=False)
    category = Column(String(50))
    tags = Column(String(500), default="")
    content = Column(Text, nullable=False)
    source = Column(String(100), default="manual")
    views = Column(Integer, default=0)
    helpful = Column(Integer, default=0)
    created_at = Column(DateTime, default=_now)


class SystemMetric(Base):
    __tablename__ = "system_metrics"

    id = Column(Integer, primary_key=True, index=True)
    metric_name = Column(String(100))
    metric_value = Column(Float)
    unit = Column(String(30), default="")
    source_system = Column(String(50), default="")
    recorded_at = Column(DateTime, default=_now)
