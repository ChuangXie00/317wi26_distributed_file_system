import threading
from dataclasses import dataclass
from typing import Any

from infra.meta_entry_client import MetaEntryClient


@dataclass
class AggregationResult:
    ok: bool
    data: dict[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None
    error_details: dict[str, Any] | None = None


class StateAggregator:
    def __init__(self, meta_entry_client: MetaEntryClient) -> None:
        self._client = meta_entry_client
        self._lock = threading.Lock()
        self._snapshot_seq = 0
        self._last_valid: dict[str, dict[str, Any]] = {}

    def aggregate(self) -> AggregationResult:
        upstream = self._client.fetch_all_parallel()
        source_health: dict[str, str] = {}
        warnings: list[str] = []
        merged: dict[str, dict[str, Any]] = {}

        for source, result in upstream.items():
            if result.ok and isinstance(result.data, dict):
                merged[source] = result.data
                source_health[source] = "ok"
                self._last_valid[source] = result.data
                continue

            source_health[source] = "error"
            if source in self._last_valid:
                merged[source] = self._last_valid[source]
                warnings.append(f"{source} upstream failed; fallback to last valid snapshot")
            else:
                merged[source] = {}
                warnings.append(f"{source} upstream failed; no cached snapshot")

        if all(status == "error" for status in source_health.values()) and not self._last_valid:
            return AggregationResult(
                ok=False,
                error_code="DEMO-UPS-002",
                error_message="all upstream debug sources unavailable",
                error_details={"source_health": source_health},
            )

        with self._lock:
            self._snapshot_seq += 1
            snapshot_seq = self._snapshot_seq

        entry = _normalize_entry(merged.get("entry") or {})
        leader_view = _normalize_leader(merged.get("leader") or {})
        membership_view = _normalize_membership(merged.get("membership") or {})
        replication_view = _normalize_replication(merged.get("replication") or {})
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
    return {
        "active_leader_id": _as_str(payload.get("active_leader_id")),
        "pending_leader_id": _as_str(payload.get("pending_leader_id")),
        "last_decision": _as_str(payload.get("last_decision")),
        "switch_count": _as_int(payload.get("switch_count")),
    }


def _normalize_leader(payload: dict[str, Any]) -> dict[str, Any]:
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
    membership = payload.get("membership")
    if not isinstance(membership, dict):
        membership = {}

    summary = payload.get("summary")
    if not isinstance(summary, dict):
        summary = _build_summary(membership)

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
    membership = membership_view.get("membership", {})
    meta_nodes = sorted([node_id for node_id in membership.keys() if str(node_id).startswith("meta-")])
    storage_nodes = sorted(
        [node_id for node_id in membership.keys() if str(node_id).startswith("storage-")]
    )

    single_observed_leader = bool(leader_view.get("leader"))
    single_writable_leader = bool(leader_view.get("writable_leader"))
    poll_health = "ok" if all(v == "ok" for v in source_health.values()) else "degraded"

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
    if value is None:
        return ""
    return str(value)


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}
