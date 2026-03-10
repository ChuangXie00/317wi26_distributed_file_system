import time

from fastapi import FastAPI

from core.replication import push_state_to_followers, start_replication_runtime, stop_replication_runtime
from core.runtime import get_runtime_snapshot, is_writable_leader

from api import router as api_router
from repository import init_repository_schema

# meta 服务主应用；路由在 api 层，状态与选主逻辑在 core 层。
app = FastAPI(title="317_DFS_Meta", version="0.1-p5")
app.include_router(api_router)


@app.on_event("startup")
def startup_init_repository() -> None:
    # 启动阶段初始化 PostgreSQL schema，最多重试 30 次等待数据库就绪。
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

    # 启动复制/接管运行时线程（leader 发送 heartbeat，follower 监控超时）。
    start_replication_runtime()

    # 可写 leader 启动后主动推一次快照，避免 follower 长时间等待首次同步。
    if is_writable_leader():
        push_state_to_followers(reason="leader_startup")

    # 打印启动后的运行时角色，便于快速确认是否进入预期状态。
    runtime = get_runtime_snapshot()
    print(
        "[meta] runtime initialized:",
        f"role={runtime.get('role')},",
        f"leader={runtime.get('current_leader_id')},",
        f"epoch={runtime.get('leader_epoch')},",
        f"election_mode={runtime.get('election_mode')}",
    )


@app.on_event("shutdown")
def shutdown_replication_runtime() -> None:
    # 优雅停止后台线程，避免容器退出时残留任务。
    stop_replication_runtime()
