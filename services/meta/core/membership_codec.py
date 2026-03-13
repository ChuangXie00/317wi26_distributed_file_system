from datetime import datetime, timezone
from typing import Any, Dict


MembershipEntry = Dict[str, Any]


def _timestamp_to_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_status(raw_status: str) -> str:
    normalized = str(raw_status or "").strip().lower()
    if normalized in {"alive", "dead", "suspected"}:
        return normalized
    return "dead"


# 规范化 runtime role 字段，避免非法角色值进入 membership。
def normalize_meta_role(raw_role: str) -> str:
    role = str(raw_role or "").strip().lower()
    if role in {"leader", "follower", "candidate"}:
        return role
    return "follower"


# 规范化 voted_for 字段，统一节点 ID 表达并兼容空值。
def normalize_meta_voted_for(raw_voted_for: str) -> str:
    return str(raw_voted_for or "").strip().lower()


# 生成 meta 节点 membership 条目（包含角色、leader 视图与 Lamport 观测）。
def new_meta_membership_entry(
    node_id: str,
    now_ts: float,
    status: str = "alive",
    role: str = "follower",
    current_leader_id: str = "",
    leader_epoch: int = 0,
    current_term: int = 0,
    voted_for: str = "",
    lamport: int = 0,
    writable_leader: bool = False,
    source: str = "meta_runtime",
) -> MembershipEntry:
    base_entry: MembershipEntry = {
        "node_type": "meta",
        "status": _normalize_status(status),
        "last_heartbeat_ts": float(now_ts),
        "last_heartbeat_at": _timestamp_to_iso(float(now_ts)),
    }
    base_entry.update(
        {
            "role": normalize_meta_role(role),
            "current_leader_id": str(current_leader_id or "").strip().lower(),
            "leader_epoch": max(0, int(leader_epoch)),
            # quorum 任期号，优先使用 current_term；历史数据回落到 leader_epoch。
            "current_term": max(0, int(current_term)),
            # quorum 当前任期已投票对象（空字符串表示未投票）。
            "voted_for": normalize_meta_voted_for(voted_for),
            "lamport": max(0, int(lamport)),
            "writable_leader": bool(writable_leader),
            "source": str(source or "").strip() or "meta_runtime",
            "node_id": str(node_id or "").strip().lower(),
        }
    )
    return base_entry
