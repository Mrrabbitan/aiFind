from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Optional
from backend.database import get_db
from backend.models import KnowledgeArticle
from backend.services.ai_engine import search_knowledge

router = APIRouter(prefix="/api/knowledge", tags=["knowledge"])


class CreateArticleReq(BaseModel):
    title: str
    category: str = "operation"
    tags: str = ""
    content: str


@router.get("")
def list_articles(category: Optional[str] = None, q: Optional[str] = None,
                  db: Session = Depends(get_db)):
    query = db.query(KnowledgeArticle)
    if category:
        query = query.filter(KnowledgeArticle.category == category)
    articles = query.order_by(KnowledgeArticle.created_at.desc()).all()
    items = [
        {
            "id": a.id, "title": a.title, "category": a.category,
            "tags": a.tags, "content": a.content, "source": a.source,
            "views": a.views, "helpful": a.helpful,
            "created_at": str(a.created_at) if a.created_at else None,
        }
        for a in articles
    ]
    if q:
        items = search_knowledge(q, items)
    return items


@router.post("")
def create_article(req: CreateArticleReq, db: Session = Depends(get_db)):
    article = KnowledgeArticle(
        title=req.title, category=req.category,
        tags=req.tags, content=req.content,
    )
    db.add(article)
    db.commit()
    db.refresh(article)
    return {"id": article.id, "message": "知识文章已创建"}


@router.get("/{article_id}")
def get_article(article_id: int, db: Session = Depends(get_db)):
    a = db.query(KnowledgeArticle).filter(KnowledgeArticle.id == article_id).first()
    if not a:
        raise HTTPException(404, "文章不存在")
    a.views += 1
    db.commit()
    return {
        "id": a.id, "title": a.title, "category": a.category,
        "tags": a.tags, "content": a.content, "source": a.source,
        "views": a.views, "helpful": a.helpful,
        "created_at": str(a.created_at) if a.created_at else None,
    }


@router.post("/{article_id}/helpful")
def mark_helpful(article_id: int, db: Session = Depends(get_db)):
    a = db.query(KnowledgeArticle).filter(KnowledgeArticle.id == article_id).first()
    if a:
        a.helpful += 1
        db.commit()
    return {"message": "已标记为有帮助"}
