import time

from fastapi import FastAPI

from core.config import META_NODE_ID
from core.replication import (
    probe_cluster_leader_for_rejoin,
    push_state_to_followers,
    start_replication_runtime,
    stop_replication_runtime,
)
from core.runtime import force_rejoin_as_follower, get_runtime_snapshot, is_writable_leader

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

    # 启动前做一次重入探测：若本节点是“恢复的旧 leader”，先回归 follower，避免立即抢主。
    rejoin_probe = probe_cluster_leader_for_rejoin()
    # 只要能探测到 peer 且“本地可写或已观测到 leader”，就执行重入回归与冷却保护。
    reachable_peer_count = int(rejoin_probe.get("reachable_peer_count", 0))
    observed_leader_id = str(rejoin_probe.get("leader_id", "")).strip().lower()
    observed_leader_epoch = int(rejoin_probe.get("leader_epoch", 0))
    should_apply_rejoin = bool(reachable_peer_count > 0 and (is_writable_leader() or bool(observed_leader_id)))
    if should_apply_rejoin:

        # peers 报告 leader 是自己时，仍按“恢复节点默认 follower”处理，避免旧主自认定写入。
        if observed_leader_id == META_NODE_ID:
            observed_leader_id = ""

        rejoin_reason = "startup_rejoin_observed_leader" if observed_leader_id else "startup_rejoin_peer_reachable"
        rejoin_state = force_rejoin_as_follower(
            reason=rejoin_reason,
            observed_leader_id=observed_leader_id,
            observed_leader_epoch=observed_leader_epoch if observed_leader_epoch > 0 else None,
        )
        print(
            "[meta] startup rejoin applied:",
            f"reason={rejoin_reason},",
            f"leader={rejoin_state.get('leader_id')},",
            f"epoch={rejoin_state.get('leader_epoch')},",
            f"reachable_peers={reachable_peer_count},",
            f"holdoff_until={rejoin_state.get('rejoin_holdoff_until')}",
        )

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
