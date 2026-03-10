from fastapi import APIRouter

from core.replication import get_replication_status
from core.runtime import get_runtime_snapshot, is_writable_leader
from core.state import get_membership_snapshot, load_state, persist_state, refresh_storage_membership
from repository import get_repository

router = APIRouter()
REPO = get_repository()


@router.get("/debug/leader")
def debug_leader() -> dict:
    # 中文：输出运行时 leader 视图，替代旧版静态 ROLE 观测。
    runtime = get_runtime_snapshot()
    return {
        "node_id": runtime.get("node_id"),
        "role": runtime.get("role"),
        "leader": runtime.get("current_leader_id"),
        "leader_epoch": runtime.get("leader_epoch"),
        "lamport": runtime.get("lamport_clock"),
        "last_applied_lamport": runtime.get("last_applied_lamport"),
        "writable_leader": is_writable_leader(),
        "election_mode": runtime.get("election_mode"),
    }


@router.get("/debug/membership")
def debug_membership() -> dict:
    state = load_state()
    refreshed = False

    # 中文：仅可写 leader 主动刷新超时，保证 debug 输出反映最新 membership。
    if is_writable_leader():
        refreshed = refresh_storage_membership(state)
        if refreshed:
            persist_state(state)

    snapshot = get_membership_snapshot(state)
    alive_count = 0
    dead_count = 0
    suspected_count = 0

    for entry in snapshot.values():
        status = str(entry.get("status", "dead"))
        if status == "alive":
            alive_count += 1
        elif status == "suspected":
            suspected_count += 1
        else:
            dead_count += 1

    return {
        "membership": snapshot,
        "summary": {
            "alive": alive_count,
            "suspected": suspected_count,
            "dead": dead_count,
            "total": len(snapshot),
        },
        "runtime": get_runtime_snapshot(),
        "refreshed_by_writable_leader": refreshed,
    }


@router.get("/debug/repository")
def debug_repository() -> dict:
    # 中文：用于快速观测 PostgreSQL 表健康和数据规模。
    return REPO.db_health()


@router.get("/debug/replication")
def debug_replication() -> dict:
    # 中文：输出复制、心跳、接管、Lamport 的完整运行时信息。
    return get_replication_status()
