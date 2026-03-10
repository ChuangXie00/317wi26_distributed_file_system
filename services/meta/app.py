import time

from fastapi import FastAPI

from core.config import ROLE
from core.replication import push_state_to_followers, start_replication_runtime, stop_replication_runtime

from api import router as api_router
from repository import init_repository_schema

# main app for init router, logic is in api and core
# 0.1p03 - storage heartbeat refresh + membership
app = FastAPI(title="317_DFS_Meta", version="0.1-p4")
app.include_router(api_router)


@app.on_event("startup")
def startup_init_repository() -> None:
    # init postgres schema，重试30等待数据库连接
    max_retries = 30
    sleep_sec = 1.0

    for attempt in range(1, max_retries + 1):
        try:
            init_repository_schema()
            print("[meta] repository schema initialized.")
            break
        except Exception as exc:
            print(f"[meta] repository init failed (attempt={attempt}/{max_retries}): {exc}")
            time.sleep(sleep_sec)
    else:
        raise RuntimeError("failed to initialize postgres repository schema after retries")

    # init membership
    start_replication_runtime()

    # leader 启动后主动推一次snap，避免 follower 长时间等待首次同步
    if ROLE == "leader":
        push_state_to_followers(reason="leader_startup")



@app.on_event("shutdown")
def shutdown_replication_runtime() -> None:
    stop_replication_runtime()
