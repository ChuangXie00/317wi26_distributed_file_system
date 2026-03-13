import threading
import time
import os
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
        # 演示优化：动作成功后短时间内对已 stop 的 meta 节点做本地状态覆盖。
        self._optimistic_dead_meta_until: dict[str, float] = {}
        self._optimistic_ttl_sec = max(1.0, float(os.getenv("DEMO_OPTIMISTIC_STOP_TTL_SEC", "20")))

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
        self._apply_optimistic_overlay(entry, leader_view, membership_view)
        leader_view = _reconcile_leader_view(entry, leader_view, membership_view)
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

    def apply_optimistic_action(self, *, normalized_action: str, target: str) -> None:
        action = str(normalized_action or "").strip().lower()
        node_id = str(target or "").strip().lower()
        if not node_id.startswith("meta-"):
            return

        with self._lock:
            if action == "stop":
                self._optimistic_dead_meta_until[node_id] = time.time() + self._optimistic_ttl_sec
                return
            if action in {"start", "reload"}:
                self._optimistic_dead_meta_until.pop(node_id, None)

    def _apply_optimistic_overlay(
        self,
        entry: dict[str, Any],
        leader_view: dict[str, Any],
        membership_view: dict[str, Any],
    ) -> None:
        now = time.time()
        with self._lock:
            active_dead_meta_nodes = {
                node_id
                for node_id, expires_at in self._optimistic_dead_meta_until.items()
                if expires_at > now
            }
            # 清理已过期覆盖，避免状态无限累积。
            self._optimistic_dead_meta_until = {
                node_id: expires_at
                for node_id, expires_at in self._optimistic_dead_meta_until.items()
                if expires_at > now
            }

        if not active_dead_meta_nodes:
            return

        membership = membership_view.get("membership", {})
        if not isinstance(membership, dict):
            membership = {}
            membership_view["membership"] = membership

        for node_id in active_dead_meta_nodes:
            node_entry = membership.get(node_id)
            if not isinstance(node_entry, dict):
                node_entry = {"node_type": "meta"}
                membership[node_id] = node_entry

            node_entry["node_type"] = "meta"
            node_entry["status"] = "dead"
            node_entry["role"] = "unknown"
            node_entry["current_leader_id"] = ""
            node_entry["writable_leader"] = False

        # 若存活节点仍观测到已 stop 的 leader，覆盖为未知，避免 Observed Leader 继续显示离线节点。
        for node_id, node_entry in membership.items():
            if not isinstance(node_entry, dict):
                continue
            node_type = str(node_entry.get("node_type", "")).strip().lower()
            if node_type != "meta" and not str(node_id).strip().lower().startswith("meta-"):
                continue
            observed_leader = str(node_entry.get("current_leader_id", "")).strip().lower()
            if observed_leader in active_dead_meta_nodes:
                node_entry["current_leader_id"] = ""

        # 同步覆盖 leader meta_cluster 视图，避免 Meta Table 与 leader_view 不一致。
        meta_cluster = leader_view.get("meta_cluster")
        if isinstance(meta_cluster, list):
            for item in meta_cluster:
                if not isinstance(item, dict):
                    continue
                node_id = str(item.get("node_id", "")).strip().lower()
                if node_id not in active_dead_meta_nodes:
                    continue
                item["status"] = "dead"
                item["role"] = "unknown"
                item["current_leader_id"] = ""
                item["writable_leader"] = False
                continue

            for item in meta_cluster:
                if not isinstance(item, dict):
                    continue
                observed_leader = str(item.get("current_leader_id", "")).strip().lower()
                if observed_leader in active_dead_meta_nodes:
                    item["current_leader_id"] = ""

        current_leader = _as_str(leader_view.get("leader")).strip().lower()
        if current_leader in active_dead_meta_nodes:
            leader_view["leader"] = ""
            leader_view["writable_leader"] = False

        active_leader_id = _as_str(entry.get("active_leader_id")).strip().lower()
        if active_leader_id in active_dead_meta_nodes:
            entry["active_leader_id"] = ""
            entry["pending_leader_id"] = ""
            entry["last_decision"] = "pending_failover"

        pending_leader_id = _as_str(entry.get("pending_leader_id")).strip().lower()
        if pending_leader_id in active_dead_meta_nodes:
            entry["pending_leader_id"] = ""

        membership_view["summary"] = _build_summary(membership)
        membership_view["node_type_summary"] = _build_node_type_summary(membership)


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
    # Support two upstream formats:
    # 1) legacy: heartbeat/snapshot/sync/takeover
    # 2) current: leader_heartbeat + replication + takeover
    heartbeat = _as_dict(payload.get("heartbeat"))
    snapshot = _as_dict(payload.get("snapshot"))
    sync = _as_dict(payload.get("sync"))

    leader_heartbeat = _as_dict(payload.get("leader_heartbeat"))
    replication = _as_dict(payload.get("replication"))

    # Fill heartbeat when only leader_heartbeat is provided.
    if not heartbeat and leader_heartbeat:
        heartbeat = leader_heartbeat

    # Split replication composite object into snapshot/sync views.
    if not snapshot and replication:
        snapshot = {
            "last_snapshot_sent_at": _as_str(replication.get("last_snapshot_sent_at")),
            "last_snapshot_success_at": _as_str(replication.get("last_snapshot_success_at")),
        }
    if not sync and replication:
        sync = {
            "last_sync_applied_at": _as_str(replication.get("last_sync_applied_at")),
            "last_sync_source": _as_str(replication.get("last_sync_source")),
            "last_sync_reason": _as_str(replication.get("last_sync_reason")),
            "last_sync_lamport": _as_int(replication.get("last_sync_lamport")),
            "last_applied_lamport": _as_int(replication.get("last_applied_lamport")),
        }

    return {
        "heartbeat": heartbeat,
        "snapshot": snapshot,
        "sync": sync,
        "takeover": _as_dict(payload.get("takeover")),
    }


def _reconcile_leader_view(
    entry: dict[str, Any],
    leader_view: dict[str, Any],
    membership_view: dict[str, Any],
) -> dict[str, Any]:
    # leader 与 membership 来自并发拉取，故障切换窗口可能出现短时不一致；这里做最小兜底对齐。
    merged = dict(leader_view)
    membership = membership_view.get("membership", {})
    if not isinstance(membership, dict):
        return merged

    alive_meta_nodes: list[tuple[str, dict[str, Any]]] = []
    for node_id, entry in membership.items():
        if not isinstance(entry, dict):
            continue
        node_id_s = str(node_id).strip().lower()
        node_type = str(entry.get("node_type", "")).strip().lower()
        if node_type != "meta" and not node_id_s.startswith("meta-"):
            continue
        if str(entry.get("status", "dead")).strip().lower() != "alive":
            continue
        alive_meta_nodes.append((node_id_s, entry))

    observed_leader_ids = {
        str(entry.get("current_leader_id", "")).strip().lower()
        for _, entry in alive_meta_nodes
        if str(entry.get("current_leader_id", "")).strip()
    }
    writable_nodes = sorted(
        node_id
        for node_id, entry in alive_meta_nodes
        if bool(entry.get("writable_leader", False))
    )

    unique_observed_leader = next(iter(observed_leader_ids)) if len(observed_leader_ids) == 1 else ""
    unique_writable_leader = writable_nodes[0] if len(writable_nodes) == 1 else ""
    current_leader = _as_str(merged.get("leader")).strip().lower()

    if not current_leader:
        if unique_writable_leader:
            merged["leader"] = unique_writable_leader
            current_leader = unique_writable_leader
        elif unique_observed_leader:
            merged["leader"] = unique_observed_leader
            current_leader = unique_observed_leader
        else:
            # leader/membership 两路都缺失时，回退使用 entry 的 active_leader_id，避免前端 Observed Leader 长时间 "--"。
            entry_leader = _as_str(entry.get("active_leader_id")).strip().lower()
            if entry_leader:
                merged["leader"] = entry_leader
                current_leader = entry_leader

    if not bool(merged.get("writable_leader", False)) and current_leader and unique_writable_leader == current_leader:
        merged["writable_leader"] = True

    return merged


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
