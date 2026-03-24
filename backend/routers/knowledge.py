from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Optional
from backend.database import get_db
from backend.models import KnowledgeArticle
from backend.services.ai_engine import search_knowledge
from backend.routers.manual import OPERATIONS_MANUAL

router = APIRouter(prefix="/api/knowledge", tags=["knowledge"])


class CreateArticleReq(BaseModel):
    title: str
    category: str = "operation"
    tags: str = ""
    content: str


@router.get("")
def list_articles(category: Optional[str] = None, q: Optional[str] = None,
                  page: int = 1, page_size: int = 10,
                  db: Session = Depends(get_db)):
    # 懒加载补充：确保知识库包含“第二个 sheet 全量内容”
    target_title = "订单采集场景第二个Sheet全量内容"
    exists = db.query(KnowledgeArticle).filter(KnowledgeArticle.title == target_title).first()
    if not exists:
        lines = ["## 订单采集场景（第二个Sheet）全量步骤明细", ""]
        for step in OPERATIONS_MANUAL:
            lines.append(f"### Step {step['step']}：{step['name']}")
            lines.append(f"- 分类：{step.get('category', '—')}")
            lines.append(f"- 系统：{', '.join(step.get('system', []))}")
            lines.append(f"- 自动化：{step.get('automation', '—')}")
            lines.append(f"- 说明：{step.get('description', '')}")
            lines.append("")
            for op in step.get("operations", []):
                lines.append(f"- `{op.get('id', '')}` {op.get('name', '')}（{op.get('type', '—')}）")
                if op.get("command"):
                    lines.append(f"  - 命令：`{op.get('command')}`")
                tips = op.get("tips") or []
                if tips:
                    lines.append(f"  - 注意：{'；'.join(tips)}")
            lines.append("")
        db.add(
            KnowledgeArticle(
                title=target_title,
                category="reference",
                tags="订单采集,Sheet2,全流程,16项操作,自动化",
                content="\n".join(lines),
                source="system",
            )
        )
        db.commit()

    query_obj = db.query(KnowledgeArticle)
    if category:
        query_obj = query_obj.filter(KnowledgeArticle.category == category)

    if q:
        articles = query_obj.order_by(KnowledgeArticle.created_at.desc()).all()
        all_items = [
            {
                "id": a.id, "title": a.title, "category": a.category,
                "tags": a.tags, "content": a.content, "source": a.source,
                "views": a.views, "helpful": a.helpful,
                "created_at": str(a.created_at) if a.created_at else None,
            }
            for a in articles
        ]
        all_items = search_knowledge(q, all_items)
        total = len(all_items)
        total_pages = max(1, -(-total // page_size))
        start = (page - 1) * page_size
        items = all_items[start:start + page_size]
    else:
        total = query_obj.count()
        total_pages = max(1, -(-total // page_size))
        articles = query_obj.order_by(KnowledgeArticle.created_at.desc()).offset((page - 1) * page_size).limit(page_size).all()
        items = [
            {
                "id": a.id, "title": a.title, "category": a.category,
                "tags": a.tags, "content": a.content, "source": a.source,
                "views": a.views, "helpful": a.helpful,
                "created_at": str(a.created_at) if a.created_at else None,
            }
            for a in articles
        ]

    return {"items": items, "total": total, "page": page, "page_size": page_size, "total_pages": total_pages}


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
