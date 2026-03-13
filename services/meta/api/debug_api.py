import os
import threading
import time

from fastapi import APIRouter

from core.config import STORAGE_NODES
from core.replication import get_replication_status
from core.runtime import get_runtime_snapshot, is_writable_leader
from core.state import get_membership_snapshot, load_state, persist_state, refresh_cluster_membership
from repository import get_repository

from .debug_view_builder import build_meta_cluster_summary, build_meta_cluster_view

router = APIRouter()
REPO = get_repository()

# debug 刷新闸门：同一时间窗口内只允许一次 leader 侧 membership 刷新，
# 避免 /debug/leader、/debug/membership、/debug/replication 并发请求重复探测 peers。
_DEBUG_REFRESH_LOCK = threading.RLock()
_DEBUG_REFRESH_MIN_INTERVAL_SEC = max(0.2, float(os.getenv("META_DEBUG_REFRESH_MIN_INTERVAL_SEC", "1.0")))
_DEBUG_LAST_REFRESH_TS = 0.0


def _refresh_membership_for_debug(state: dict) -> bool:
    global _DEBUG_LAST_REFRESH_TS
    if not is_writable_leader():
        return False

    now_ts = time.time()
    with _DEBUG_REFRESH_LOCK:
        if now_ts - _DEBUG_LAST_REFRESH_TS < _DEBUG_REFRESH_MIN_INTERVAL_SEC:
            return False

        refreshed = refresh_cluster_membership(state)
        if refreshed:
            persist_state(state)
        _DEBUG_LAST_REFRESH_TS = time.time()
        return True


@router.get("/debug/leader")
def debug_leader() -> dict:
    # 输出运行时 leader 视图 + meta 集群总览，便于三节点场景下直接观察收敛状态。
    state = load_state()
    refreshed = _refresh_membership_for_debug(state)

    snapshot = get_membership_snapshot(state)
    runtime = get_runtime_snapshot()
    meta_cluster = build_meta_cluster_view(snapshot)
    meta_cluster_summary = build_meta_cluster_summary(meta_cluster, local_node_id=str(runtime.get("node_id", "")))

    return {
        "node_id": runtime.get("node_id"),
        "role": runtime.get("role"),
        "leader": runtime.get("current_leader_id"),
        "leader_epoch": runtime.get("leader_epoch"),
        "current_term": runtime.get("current_term"),
        "voted_for": runtime.get("voted_for"),
        "lamport": runtime.get("lamport_clock"),
        "last_applied_lamport": runtime.get("last_applied_lamport"),
        "writable_leader": is_writable_leader(),
        "election_mode": runtime.get("election_mode"),
        "meta_cluster": meta_cluster,
        "meta_cluster_summary": meta_cluster_summary,
        "membership_refreshed_by_writable_leader": refreshed,
    }


@router.get("/debug/membership")
def debug_membership() -> dict:
    state = load_state()
    refreshed = _refresh_membership_for_debug(state)

    snapshot = get_membership_snapshot(state)
    try:
        replica_counts = REPO.get_replica_counts_by_node(STORAGE_NODES)
    except Exception:
        # 副本计数仅用于演示增强，不阻断 membership 主链路。
        replica_counts = {}

    runtime = get_runtime_snapshot()
    meta_cluster = build_meta_cluster_view(snapshot)
    meta_cluster_summary = build_meta_cluster_summary(meta_cluster, local_node_id=str(runtime.get("node_id", "")))
    alive_count = 0
    dead_count = 0
    suspected_count = 0
    meta_count = 0
    storage_count = 0

    for node_id, entry in snapshot.items():
        status = str(entry.get("status", "dead"))
        node_type = str(entry.get("node_type", "storage")).strip().lower()

        if node_type == "meta":
            meta_count += 1
        else:
            storage_count += 1
            replica_chunks = int(replica_counts.get(str(node_id), 0))
            entry["replica_chunks"] = replica_chunks
            # 兼容旧前端字段名，后续统一迁移到 replica_chunks。
            entry["replicas"] = replica_chunks

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
        "node_type_summary": {
            "meta": meta_count,
            "storage": storage_count,
            "total": len(snapshot),
        },
        "meta_cluster": meta_cluster,
        "meta_cluster_summary": meta_cluster_summary,
        "runtime": runtime,
        "refreshed_by_writable_leader": refreshed,
    }


@router.get("/debug/repository")
def debug_repository() -> dict:
    # 用于快速观察 PostgreSQL 表健康和数据规模。
    return REPO.db_health()


@router.get("/debug/replication")
def debug_replication() -> dict:
    # 输出复制、心跳、接管、Lamport 的完整运行时信息，并补充 meta 集群观察面。
    state = load_state()
    refreshed = _refresh_membership_for_debug(state)

    snapshot = get_membership_snapshot(state)
    runtime = get_runtime_snapshot()
    meta_cluster = build_meta_cluster_view(snapshot)
    meta_cluster_summary = build_meta_cluster_summary(meta_cluster, local_node_id=str(runtime.get("node_id", "")))

    status = get_replication_status()
    status["meta_cluster"] = meta_cluster
    status["meta_cluster_summary"] = meta_cluster_summary
    status["membership_refreshed_by_writable_leader"] = refreshed
    return status
