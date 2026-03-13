import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.demo_api import router as demo_router


# Demo Backend 主应用：统一挂载 /api/demo 相关路由。
app = FastAPI(title="317_DFS_Demo_Backend", version="0.2-p00")

# 允许前端开发站点跨域访问 Demo API，避免浏览器拦截请求。
raw_origins = os.getenv(
    "DEMO_CORS_ALLOW_ORIGINS",
    "http://127.0.0.1:5173,http://localhost:5173",
)
allow_origins = [origin.strip() for origin in raw_origins.split(",") if origin.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(demo_router)
