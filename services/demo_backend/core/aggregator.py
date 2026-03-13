import threading
from dataclasses import dataclass
from typing import Any

from infra.meta_entry_client import MetaEntryClient


@dataclass
class AggregationResult:
    # 聚合返回模型：成功返回 data，失败返回标准错误信息。
    ok: bool
    data: dict[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None
    error_details: dict[str, Any] | None = None


class StateAggregator:
    def __init__(self, meta_entry_client: MetaEntryClient) -> None:
        # 上游客户端：负责并发拉取 debug 源数据。
        self._client = meta_entry_client
        # 保护 snapshot_seq 自增的线程安全。
        self._lock = threading.Lock()
        # 快照序号：每次聚合成功返回时递增，供前端比对刷新。
        self._snapshot_seq = 0
        # 每个 source 最近一次有效快照，用于降级回退。
        self._last_valid: dict[str, dict[str, Any]] = {}

    def aggregate(self) -> AggregationResult:
        # 并发拉取四个上游源。
        upstream = self._client.fetch_all_parallel()
        source_health: dict[str, str] = {}
        warnings: list[str] = []
        merged: dict[str, dict[str, Any]] = {}

        for source, result in upstream.items():
            if result.ok and isinstance(result.data, dict):
                # 拉取成功：直接使用新值并刷新缓存。
                merged[source] = result.data
                source_health[source] = "ok"
                self._last_valid[source] = result.data
                continue

            # 拉取失败：优先回退到该 source 的最近有效值。
            source_health[source] = "error"
            if source in self._last_valid:
                merged[source] = self._last_valid[source]
                warnings.append(f"{source} upstream failed; fallback to last valid snapshot")
            else:
                # 首次即失败且无缓存时，先给空对象并记录 warning。
                merged[source] = {}
                warnings.append(f"{source} upstream failed; no cached snapshot")

        # 全源不可用且历史缓存也为空时，返回 503 级别错误。
        if all(status == "error" for status in source_health.values()) and not self._last_valid:
            return AggregationResult(
                ok=False,
                error_code="DEMO-UPS-002",
                error_message="all upstream debug sources unavailable",
                error_details={"source_health": source_health},
            )

        with self._lock:
            # 每次成功聚合输出一个新序号，确保调用方可判定新旧快照。
            self._snapshot_seq += 1
            snapshot_seq = self._snapshot_seq

        # 将不同上游格式标准化为统一 API 契约结构。
        entry = _normalize_entry(merged.get("entry") or {})
        leader_view = _normalize_leader(merged.get("leader") or {})
        membership_view = _normalize_membership(merged.get("membership") or {})
        replication_view = _normalize_replication(merged.get("replication") or {})
        # 计算前端直接消费的衍生字段，避免 UI 重复推导。
        derived = _derive(entry, leader_view, membership_view, source_health)

        return AggregationResult(
            ok=True,
            data={
                "snapshot_seq": snapshot_seq,
                "entry": entry,
                "leader_view": leader_view,
                "membership_view": membership_view,
                "replication_view": replication_view,
                "derived": derived,
                "source_health": source_health,
                "warnings": warnings,
            },
        )


def _normalize_entry(payload: dict[str, Any]) -> dict[str, Any]:
    # entry 视图字段归一化：缺失字段回退为契约默认值。
    return {
        "active_leader_id": _as_str(payload.get("active_leader_id")),
        "pending_leader_id": _as_str(payload.get("pending_leader_id")),
        "last_decision": _as_str(payload.get("last_decision")),
        "switch_count": _as_int(payload.get("switch_count")),
    }


def _normalize_leader(payload: dict[str, Any]) -> dict[str, Any]:
    # leader 视图归一化：meta_cluster 非 list 时兜底为空列表。
    meta_cluster = payload.get("meta_cluster")
    if not isinstance(meta_cluster, list):
        meta_cluster = []

    return {
        "leader": _as_str(payload.get("leader")),
        "leader_epoch": _as_int(payload.get("leader_epoch")),
        "current_term": _as_int(payload.get("current_term")),
        "lamport": _as_int(payload.get("lamport")),
        "last_applied_lamport": _as_int(payload.get("last_applied_lamport")),
        "writable_leader": bool(payload.get("writable_leader", False)),
        "meta_cluster": meta_cluster,
    }


def _normalize_membership(payload: dict[str, Any]) -> dict[str, Any]:
    # membership 主体兜底为空对象，保证前端可安全遍历。
    membership = payload.get("membership")
    if not isinstance(membership, dict):
        membership = {}

    # 若上游未给 summary，则根据 membership 现算。
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        summary = _build_summary(membership)

    # 若上游未给 node_type_summary，则按节点前缀现算。
    node_type_summary = payload.get("node_type_summary")
    if not isinstance(node_type_summary, dict):
        node_type_summary = _build_node_type_summary(membership)

    return {
        "membership": membership,
        "summary": {
            "alive": _as_int(summary.get("alive")),
            "suspected": _as_int(summary.get("suspected")),
            "dead": _as_int(summary.get("dead")),
            "total": _as_int(summary.get("total")),
        },
        "node_type_summary": {
            "meta": _as_int(node_type_summary.get("meta")),
            "storage": _as_int(node_type_summary.get("storage")),
            "total": _as_int(node_type_summary.get("total")),
        },
    }


def _normalize_replication(payload: dict[str, Any]) -> dict[str, Any]:
    # replication 子视图统一保证四个对象字段存在。
    return {
        "heartbeat": _as_dict(payload.get("heartbeat")),
        "snapshot": _as_dict(payload.get("snapshot")),
        "sync": _as_dict(payload.get("sync")),
        "takeover": _as_dict(payload.get("takeover")),
    }


def _derive(
    entry: dict[str, Any],
    leader_view: dict[str, Any],
    membership_view: dict[str, Any],
    source_health: dict[str, str],
) -> dict[str, Any]:
    # derived 字段用于前端一屏展示与健康状态判断。
    membership = membership_view.get("membership", {})
    meta_nodes = sorted([node_id for node_id in membership.keys() if str(node_id).startswith("meta-")])
    storage_nodes = sorted(
        [node_id for node_id in membership.keys() if str(node_id).startswith("storage-")]
    )

    single_observed_leader = bool(leader_view.get("leader"))
    single_writable_leader = bool(leader_view.get("writable_leader"))
    poll_health = "ok" if all(v == "ok" for v in source_health.values()) else "degraded"

    # 无 membership 时回退使用 entry.active_leader_id，避免 meta_nodes 为空。
    if not meta_nodes:
        fallback_meta = _as_str(entry.get("active_leader_id"))
        if fallback_meta:
            meta_nodes = [fallback_meta]

    return {
        "single_observed_leader": single_observed_leader,
        "single_writable_leader": single_writable_leader,
        "meta_nodes": meta_nodes,
        "storage_nodes": storage_nodes,
        "poll_health": poll_health,
    }


def _build_summary(membership: dict[str, Any]) -> dict[str, int]:
    # 统计 alive/suspected/dead/total，兼容缺失 status 的节点。
    alive = 0
    suspected = 0
    dead = 0
    for node in membership.values():
        if isinstance(node, dict):
            status = str(node.get("status", "")).lower()
        else:
            status = ""
        if status == "alive":
            alive += 1
        elif status == "suspected":
            suspected += 1
        elif status == "dead":
            dead += 1
    return {
        "alive": alive,
        "suspected": suspected,
        "dead": dead,
        "total": len(membership),
    }


def _build_node_type_summary(membership: dict[str, Any]) -> dict[str, int]:
    # 按节点 ID 前缀区分 meta 与 storage 数量。
    meta_count = 0
    storage_count = 0
    for node_id in membership.keys():
        node_id_s = str(node_id)
        if node_id_s.startswith("meta-"):
            meta_count += 1
        elif node_id_s.startswith("storage-"):
            storage_count += 1
    return {
        "meta": meta_count,
        "storage": storage_count,
        "total": len(membership),
    }


def _as_str(value: Any) -> str:
    # 通用字符串兜底转换。
    if value is None:
        return ""
    return str(value)


def _as_int(value: Any) -> int:
    # 通用整数兜底转换，异常时返回 0。
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _as_dict(value: Any) -> dict[str, Any]:
    # 通用对象兜底转换，保证返回 dict。
    if isinstance(value, dict):
        return value
    return {}
