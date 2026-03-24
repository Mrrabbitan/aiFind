from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Dict, Any, Optional
from backend.database import get_db
from backend.models import ParamTemplate
from backend.services.ai_engine import validate_params, recommend_params

router = APIRouter(prefix="/api/params", tags=["params"])


class CreateTemplateReq(BaseModel):
    name: str
    category: str = "flink"
    table_pattern: str = "*"
    params: Dict[str, Any] = {}
    description: str = ""


class ValidateReq(BaseModel):
    params: Dict[str, str]


class RecommendReq(BaseModel):
    table_name: str


@router.get("")
def list_templates(page: int = 1, page_size: int = 10, db: Session = Depends(get_db)):
    total = db.query(ParamTemplate).count()
    total_pages = max(1, -(-total // page_size))
    tpls = (
        db.query(ParamTemplate)
        .order_by(ParamTemplate.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    items = [
        {
            "id": t.id, "name": t.name, "category": t.category,
            "table_pattern": t.table_pattern, "params": t.params or {},
            "description": t.description, "version": t.version,
            "created_at": str(t.created_at) if t.created_at else None,
        }
        for t in tpls
    ]
    return {"items": items, "total": total, "page": page, "page_size": page_size, "total_pages": total_pages}


@router.post("")
def create_template(req: CreateTemplateReq, db: Session = Depends(get_db)):
    tpl = ParamTemplate(
        name=req.name, category=req.category,
        table_pattern=req.table_pattern, params=req.params,
        description=req.description,
    )
    db.add(tpl)
    db.commit()
    db.refresh(tpl)
    return {"id": tpl.id, "message": "模板已创建"}


@router.get("/{tpl_id}")
def get_template(tpl_id: int, db: Session = Depends(get_db)):
    tpl = db.query(ParamTemplate).filter(ParamTemplate.id == tpl_id).first()
    if not tpl:
        raise HTTPException(404, "模板不存在")
    return {
        "id": tpl.id, "name": tpl.name, "category": tpl.category,
        "table_pattern": tpl.table_pattern, "params": tpl.params or {},
        "description": tpl.description, "version": tpl.version,
    }


@router.post("/validate")
def validate(req: ValidateReq):
    results = validate_params(req.params)
    return {"results": results}


@router.post("/recommend")
def recommend(req: RecommendReq):
    rec = recommend_params(req.table_name)
    return rec
