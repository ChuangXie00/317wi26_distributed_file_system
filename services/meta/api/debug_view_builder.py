from typing import Any, Dict, List


# 安全解析整型字段，避免 debug 展示被脏数据打断。
def safe_int(raw_value: Any, default: int = 0) -> int:
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return int(default)


# 安全解析布尔字段，兼容字符串/数字/布尔混合输入。
def safe_bool(raw_value: Any, default: bool = False) -> bool:
    if isinstance(raw_value, bool):
        return raw_value
    if isinstance(raw_value, (int, float)):
        return bool(raw_value)
    if isinstance(raw_value, str):
        return raw_value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(default)


# 从 membership 快照提取 meta 集群视图，统一展示 role/leader/epoch/term/vote/lamport 等核心字段。
def build_meta_cluster_view(snapshot: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for node_id in sorted(snapshot.keys()):
        entry = snapshot.get(node_id, {})
        if str(entry.get("node_type", "storage")).strip().lower() != "meta":
            continue
        out.append(
            {
                "node_id": str(node_id).strip().lower(),
                "status": str(entry.get("status", "dead")).strip().lower(),
                "role": str(entry.get("role", "follower")).strip().lower(),
                "current_leader_id": str(entry.get("current_leader_id", "")).strip().lower(),
                "leader_epoch": max(0, safe_int(entry.get("leader_epoch", 0))),
                # 兼容旧数据，若缺少 current_term 则回退到 leader_epoch。
                "current_term": max(0, safe_int(entry.get("current_term", entry.get("leader_epoch", 0)))),
                "voted_for": str(entry.get("voted_for", "")).strip().lower(),
                "lamport": max(0, safe_int(entry.get("lamport", 0))),
                "writable_leader": safe_bool(entry.get("writable_leader", False), default=False),
                "last_heartbeat_at": str(entry.get("last_heartbeat_at", "")),
                "source": str(entry.get("source", "")),
            }
        )
    return out


# 对 meta 集群视图生成摘要，帮助快速判断是否已收敛到唯一 leader。
def build_meta_cluster_summary(meta_cluster: List[Dict[str, Any]], local_node_id: str = "") -> Dict[str, Any]:
    role_summary = {"leader": 0, "follower": 0, "candidate": 0, "unknown": 0}
    alive_count = 0
    dead_count = 0
    suspected_count = 0
    leader_ids = set()
    leader_epoch_set = set()
    current_term_set = set()
    voted_for_summary: Dict[str, List[str]] = {}
    writable_leader_nodes = []
    local_role = "unknown"

    normalized_local_node_id = str(local_node_id or "").strip().lower()

    for node in meta_cluster:
        role = str(node.get("role", "unknown")).strip().lower()
        status = str(node.get("status", "dead")).strip().lower()
        current_leader_id = str(node.get("current_leader_id", "")).strip().lower()
        leader_epoch = max(0, safe_int(node.get("leader_epoch", 0)))
        current_term = max(0, safe_int(node.get("current_term", node.get("leader_epoch", 0))))
        voted_for = str(node.get("voted_for", "")).strip().lower()
        node_id = str(node.get("node_id", "")).strip().lower()

        if role in {"leader", "follower", "candidate"}:
            role_summary[role] += 1
        else:
            role_summary["unknown"] += 1

        if status == "alive":
            alive_count += 1
        elif status == "suspected":
            suspected_count += 1
        else:
            dead_count += 1

        if current_leader_id:
            leader_ids.add(current_leader_id)
        if leader_epoch > 0:
            leader_epoch_set.add(leader_epoch)
        if current_term > 0:
            current_term_set.add(current_term)
        if voted_for:
            voted_for_summary.setdefault(voted_for, []).append(node_id)
        if safe_bool(node.get("writable_leader", False), default=False):
            writable_leader_nodes.append(node_id)
        if node_id == normalized_local_node_id:
            local_role = role

    unique_leader = next(iter(leader_ids)) if len(leader_ids) == 1 else ""
    has_single_observed_leader = len(leader_ids) == 1 and unique_leader != ""
    has_single_writable_leader = len(writable_leader_nodes) == 1

    return {
        "meta_total": len(meta_cluster),
        "alive": alive_count,
        "suspected": suspected_count,
        "dead": dead_count,
        "role_summary": role_summary,
        "observed_leader_ids": sorted(leader_ids),
        "leader_epoch_set": sorted(leader_epoch_set),
        "current_term_set": sorted(current_term_set),
        "voted_for_summary": {k: sorted(v) for k, v in sorted(voted_for_summary.items())},
        "single_observed_leader": bool(has_single_observed_leader),
        "single_writable_leader": bool(has_single_writable_leader),
        "unique_leader_id": unique_leader,
        "writable_leader_nodes": sorted(writable_leader_nodes),
        "local_node_id": normalized_local_node_id,
        "local_role": local_role,
    }
