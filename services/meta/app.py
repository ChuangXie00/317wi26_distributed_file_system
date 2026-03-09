import time

from fastapi import FastAPI

from core.repository_pg import init_repository_schema
from routers.client_api import router as client_router

# main app for init router, logic is in routers and core
# 0.1p03 - storage heartbeat refresh + membership
app = FastAPI(title="317_DFS_Meta", version="0.1-p3.1")
app.include_router(client_router)

# init postgres schema，重试30等待数据库连接
# 3.2 切换 follwer，添加机制
@app.on_event("startup")
def startup_init_repository() -> None:
    max_retries = 30
    sleep_sec = 1.0

    for attempt in range(1, max_retries + 1):
        try:
            init_repository_schema()
            print("[meta] repository schema initialized.")
            return
        except Exception as exc:
            print(f"[meta] repository init failed (attempt={attempt}/{max_retries}): {exc}")
            time.sleep(sleep_sec)

    raise RuntimeError("failed to initialize postgres repository schema after retries")