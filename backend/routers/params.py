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
def list_templates(db: Session = Depends(get_db)):
    tpls = db.query(ParamTemplate).order_by(ParamTemplate.created_at.desc()).all()
    return [
        {
            "id": t.id, "name": t.name, "category": t.category,
            "table_pattern": t.table_pattern, "params": t.params or {},
            "description": t.description, "version": t.version,
            "created_at": str(t.created_at) if t.created_at else None,
        }
        for t in tpls
    ]


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
