"""智能订单采集运营平台 — 入口"""
import os, sys

sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from backend.database import engine, Base
from backend.models import *  # noqa: ensure all models registered
from backend.seed import seed
from backend.routers import dashboard, flows, params, monitor, knowledge, manual, skills, auth

Base.metadata.create_all(bind=engine)
seed()

app = FastAPI(title="智能订单采集运营平台", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health():
    return {"status": "ok", "version": "1.0.0"}


app.include_router(dashboard.router)
app.include_router(flows.router)
app.include_router(params.router)
app.include_router(monitor.router)
app.include_router(knowledge.router)
app.include_router(manual.router)
app.include_router(skills.router)
app.include_router(auth.router)


class SPAStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope) -> Response:
        try:
            return await super().get_response(path, scope)
        except Exception as exc:
            # SPA 深链路刷新时，回退到 index.html 交给前端路由处理。
            if getattr(exc, "status_code", None) == 404:
                return await super().get_response("index.html", scope)
            raise


dist = os.path.join(os.path.dirname(__file__), "frontend", "dist")
if os.path.isdir(dist):
    app.mount("/", SPAStaticFiles(directory=dist, html=True), name="spa")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
