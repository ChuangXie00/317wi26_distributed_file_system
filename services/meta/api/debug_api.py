from typing import Dict

from fastapi import APIRouter

from core.replication import get_replication_status
from core.runtime import get_runtime_snapshot, is_writable_leader
from core.state import get_membership_snapshot, load_state, persist_state, refresh_cluster_membership
from repository import get_repository

from .debug_view_builder import build_meta_cluster_summary, build_meta_cluster_view

router = APIRouter()
REPO = get_repository()


@router.get("/debug/leader")
def debug_leader() -> dict:
    # 输出运行时 leader 视图 + meta 集群总览，便于三节点场景下直接观察收敛状态。
    state = load_state()
    refreshed = False
    if is_writable_leader():
        refreshed = refresh_cluster_membership(state)
        if refreshed:
            persist_state(state)

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
    refreshed = False

    # 仅可写 leader 主动刷新完整 cluster membership，保证 debug 输出包含 meta/storage 最新状态。
    if is_writable_leader():
        refreshed = refresh_cluster_membership(state)
        if refreshed:
            persist_state(state)

    snapshot = get_membership_snapshot(state)
    runtime = get_runtime_snapshot()
    meta_cluster = build_meta_cluster_view(snapshot)
    meta_cluster_summary = build_meta_cluster_summary(meta_cluster, local_node_id=str(runtime.get("node_id", "")))
    alive_count = 0
    dead_count = 0
    suspected_count = 0
    meta_count = 0
    storage_count = 0

    for entry in snapshot.values():
        status = str(entry.get("status", "dead"))
        node_type = str(entry.get("node_type", "storage")).strip().lower()

        if node_type == "meta":
            meta_count += 1
        else:
            storage_count += 1

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
    # 用于快速观测 PostgreSQL 表健康和数据规模。
    return REPO.db_health()


@router.get("/debug/replication")
def debug_replication() -> dict:
    # 输出复制、心跳、接管、Lamport 的完整运行时信息，并补充 meta 集群观测面。
    state = load_state()
    refreshed = False
    if is_writable_leader():
        refreshed = refresh_cluster_membership(state)
        if refreshed:
            persist_state(state)

    snapshot = get_membership_snapshot(state)
    runtime = get_runtime_snapshot()
    meta_cluster = build_meta_cluster_view(snapshot)
    meta_cluster_summary = build_meta_cluster_summary(meta_cluster, local_node_id=str(runtime.get("node_id", "")))

    status = get_replication_status()
    status["meta_cluster"] = meta_cluster
    status["meta_cluster_summary"] = meta_cluster_summary
    status["membership_refreshed_by_writable_leader"] = refreshed
    return status
