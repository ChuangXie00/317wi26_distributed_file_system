import copy
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request as UrlRequest, urlopen

from repository import get_repository

from ..config import (
    REREPLICATION_BATCH_LIMIT,
    REREPLICATION_DEAD_GRACE_SEC,
    REREPLICATION_ENABLE,
    REREPLICATION_HTTP_TIMEOUT_SEC,
    REREPLICATION_RETRY_BACKOFF_MAX_SEC,
    REREPLICATION_RETRY_BACKOFF_SEC,
    REREPLICATION_RETRY_MAX,
    REREPLICATION_SCAN_CHUNK_LIMIT,
    REREPLICATION_SCAN_INTERVAL_SEC,
    REPLICATION_FACTOR,
    STORAGE_NODES,
    STORAGE_PORT,
)
from ..runtime import is_writable_leader
from ..state import get_alive_storage_nodes, get_membership_snapshot, load_state, persist_state, refresh_storage_membership
from .state_store import now_iso, record_error, stop_event, update_runtime

REPO = get_repository()

_REREPL_LOCK = threading.RLock()
_REREPL_THREAD: Optional[threading.Thread] = None

# fingerprint -> retry state
_RETRY_STATE: Dict[str, Dict[str, Any]] = {}

_STATUS: Dict[str, Any] = {
    "enabled": bool(REREPLICATION_ENABLE),
    "running": False,
    "last_scan_at": "",
    "last_scan_candidates": 0,
    "last_scan_processed": 0,
    "last_scan_skipped_retry": 0,
    "last_scan_dead_nodes": [],
    "last_scan_alive_nodes": [],
    "queue_size": 0,
    "succeeded_total": 0,
    "failed_total": 0,
    "retry_total": 0,
    "last_success": {},
    "last_error": {},
}


def _sync_status() -> None:
    with _REREPL_LOCK:
        update_runtime(re_replication=copy.deepcopy(_STATUS))


def _set_status(**fields: Any) -> None:
    with _REREPL_LOCK:
        _STATUS.update(fields)
    _sync_status()


def _inc_status(field: str, delta: int = 1) -> None:
    with _REREPL_LOCK:
        _STATUS[field] = int(_STATUS.get(field, 0) or 0) + int(delta)
    _sync_status()


def _reset_runtime_status() -> None:
    with _REREPL_LOCK:
        _RETRY_STATE.clear()
        _STATUS.update(
            {
                "enabled": bool(REREPLICATION_ENABLE),
                "running": False,
                "last_scan_at": "",
                "last_scan_candidates": 0,
                "last_scan_processed": 0,
                "last_scan_skipped_retry": 0,
                "last_scan_dead_nodes": [],
                "last_scan_alive_nodes": [],
                "queue_size": 0,
                "succeeded_total": 0,
                "failed_total": 0,
                "retry_total": 0,
                "last_success": {},
                "last_error": {},
            }
        )
    _sync_status()


def _storage_base_url(node_id: str) -> str:
    return f"http://{str(node_id).strip()}:{STORAGE_PORT}"


def _unique_nodes(nodes: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for node in nodes:
        value = str(node).strip()
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _read_chunk_from_sources(fingerprint: str, source_nodes: List[str]) -> Tuple[bytes, str]:
    errors: List[str] = []
    for node_id in source_nodes:
        request = UrlRequest(
            f"{_storage_base_url(node_id)}/chunk/{quote(fingerprint)}",
            method="GET",
            headers={"Accept": "application/octet-stream"},
        )
        try:
            with urlopen(request, timeout=REREPLICATION_HTTP_TIMEOUT_SEC) as response:
                if response.status != 200:
                    errors.append(f"{node_id}: status={response.status}")
                    continue
                return response.read(), node_id
        except (HTTPError, URLError, OSError, TimeoutError) as exc:
            errors.append(f"{node_id}: {exc}")

    raise RuntimeError(f"all source replicas unavailable for {fingerprint}: {errors}")


def _upload_chunk_to_target(fingerprint: str, payload: bytes, target_node: str) -> None:
    request = UrlRequest(
        f"{_storage_base_url(target_node)}/chunk/upload",
        method="PUT",
        data=payload,
        headers={
            "fingerprint": fingerprint,
            "Content-Type": "application/octet-stream",
            "Accept": "application/json",
        },
    )
    with urlopen(request, timeout=REREPLICATION_HTTP_TIMEOUT_SEC) as response:
        if response.status != 200:
            raise RuntimeError(f"upload failed to {target_node}: status={response.status}")


def _pick_targets(alive_nodes: List[str], existing_replicas: List[str], needed: int) -> List[str]:
    candidates = [node for node in alive_nodes if node not in existing_replicas]
    if len(candidates) < needed:
        return []

    replica_counts = REPO.get_replica_counts_by_node(alive_nodes)
    candidates.sort(key=lambda node_id: (int(replica_counts.get(node_id, 0)), node_id))
    return candidates[:needed]


def _repair_chunk_once(fingerprint: str, alive_nodes: List[str]) -> Dict[str, Any]:
    if not is_writable_leader():
        raise RuntimeError("leader changed during chunk repair")

    alive_set = set(alive_nodes)
    current_replicas = _unique_nodes(REPO.get_chunk_replicas(fingerprint))
    alive_replicas = [node for node in current_replicas if node in alive_set]
    if len(alive_replicas) >= REPLICATION_FACTOR:
        return {
            "fingerprint": fingerprint,
            "source_node": "",
            "target_nodes": [],
            "final_replicas": alive_replicas[:REPLICATION_FACTOR],
            "status": "already_satisfied",
        }

    # First try listed alive replicas, then fallback to probing all alive nodes.
    source_candidates = _unique_nodes(alive_replicas + [node for node in alive_nodes if node not in alive_replicas])
    payload, source_node = _read_chunk_from_sources(fingerprint, source_candidates)

    needed = max(0, REPLICATION_FACTOR - 1)
    target_nodes = _pick_targets(
        alive_nodes=alive_nodes,
        existing_replicas=[source_node],
        needed=needed,
    )
    if len(target_nodes) < needed:
        raise RuntimeError(f"not enough target nodes for {fingerprint}")

    for target_node in target_nodes:
        _upload_chunk_to_target(fingerprint, payload, target_node)

    if not is_writable_leader():
        raise RuntimeError("leader changed before metadata update")

    # Persist only replicas proven by read/write in this round.
    final_replicas = _unique_nodes([source_node] + target_nodes)[:REPLICATION_FACTOR]
    REPO.upsert_chunk_replicas(fingerprint, final_replicas)
    return {
        "fingerprint": fingerprint,
        "source_node": source_node,
        "target_nodes": target_nodes,
        "final_replicas": final_replicas,
        "status": "repaired",
    }


def _get_retry_state(fingerprint: str) -> Dict[str, Any]:
    with _REREPL_LOCK:
        state = _RETRY_STATE.get(fingerprint)
        if not isinstance(state, dict):
            return {}
        return copy.deepcopy(state)


def _clear_retry_state(fingerprint: str) -> None:
    with _REREPL_LOCK:
        _RETRY_STATE.pop(fingerprint, None)


def _mark_chunk_success(result: Dict[str, Any]) -> None:
    fingerprint = str(result.get("fingerprint", "")).strip().lower()
    if fingerprint:
        _clear_retry_state(fingerprint)

    _inc_status("succeeded_total", 1)
    _set_status(
        last_success={
            "at": now_iso(),
            "fingerprint": fingerprint,
            "status": str(result.get("status", "")),
            "source_node": str(result.get("source_node", "")),
            "target_nodes": result.get("target_nodes", []),
            "final_replicas": result.get("final_replicas", []),
        }
    )


def _mark_chunk_failure(fingerprint: str, error_message: str, now_ts: float) -> None:
    fp = str(fingerprint).strip().lower()
    if not fp:
        return

    with _REREPL_LOCK:
        existing = _RETRY_STATE.get(fp, {})
        attempts = int(existing.get("attempts", 0) or 0) + 1
        # Keep retrying with capped backoff instead of permanently blocking.
        blocked = False
        backoff_sec = min(
            REREPLICATION_RETRY_BACKOFF_MAX_SEC,
            float(REREPLICATION_RETRY_BACKOFF_SEC)
            * (2 ** max(0, min(attempts - 1, REREPLICATION_RETRY_MAX - 1))),
        )
        next_retry_at = now_ts + backoff_sec
        _RETRY_STATE[fp] = {
            "attempts": attempts,
            "next_retry_at": next_retry_at,
            "blocked": blocked,
            "last_error": str(error_message),
            "last_error_at": now_iso(),
        }

    _inc_status("failed_total", 1)
    _inc_status("retry_total", 1)
    _set_status(
        last_error={
            "at": now_iso(),
            "fingerprint": fp,
            "attempts": attempts,
            "blocked": blocked,
            "next_retry_at": next_retry_at,
            "error": str(error_message),
        }
    )


def _is_retry_ready(fingerprint: str, now_ts: float) -> bool:
    state = _get_retry_state(fingerprint)
    if not state:
        return True
    next_retry_at = float(state.get("next_retry_at", 0.0) or 0.0)
    return now_ts >= next_retry_at


def _collect_eligible_dead_nodes(membership: Dict[str, Dict[str, Any]], now_ts: float) -> List[str]:
    out: List[str] = []
    for node_id in STORAGE_NODES:
        entry = membership.get(node_id)
        if not isinstance(entry, dict):
            continue
        status = str(entry.get("status", "")).strip().lower()
        if status != "dead":
            continue
        hb_ts = float(entry.get("last_heartbeat_ts", 0.0) or 0.0)
        elapsed = (now_ts - hb_ts) if hb_ts > 0 else float("inf")
        if elapsed >= float(REREPLICATION_DEAD_GRACE_SEC):
            out.append(node_id)
    return sorted(out)


def _scan_and_repair_once() -> None:
    scan_started_ts = time.time()
    _set_status(running=True, last_scan_at=now_iso())

    processed = 0
    skipped_retry = 0
    candidates: List[str] = []
    alive_nodes: List[str] = []
    dead_nodes: List[str] = []

    try:
        state = load_state()
        if refresh_storage_membership(state):
            persist_state(state)

        membership = get_membership_snapshot(state)
        alive_nodes = get_alive_storage_nodes(state)
        dead_nodes = _collect_eligible_dead_nodes(membership, scan_started_ts)
        alive_set = set(alive_nodes)

        if len(alive_nodes) < REPLICATION_FACTOR:
            _set_status(
                last_scan_candidates=0,
                last_scan_processed=0,
                last_scan_skipped_retry=0,
                last_scan_dead_nodes=dead_nodes,
                last_scan_alive_nodes=alive_nodes,
                queue_size=0,
            )
            return

        if not dead_nodes:
            _set_status(
                last_scan_candidates=0,
                last_scan_processed=0,
                last_scan_skipped_retry=0,
                last_scan_dead_nodes=[],
                last_scan_alive_nodes=alive_nodes,
                queue_size=0,
            )
            return

        replica_sets = REPO.list_chunk_replica_sets(limit=REREPLICATION_SCAN_CHUNK_LIMIT)
        for item in replica_sets:
            fingerprint = str(item.get("fingerprint", "")).strip().lower()
            replicas_raw = item.get("replicas")
            replicas = _unique_nodes(replicas_raw if isinstance(replicas_raw, list) else [])
            if not fingerprint or not replicas:
                continue

            # Any under-replicated chunk is eligible once cluster enters degraded mode.
            alive_replicas = [node for node in replicas if node in alive_set]
            if len(alive_replicas) >= REPLICATION_FACTOR:
                continue
            if not _is_retry_ready(fingerprint, scan_started_ts):
                skipped_retry += 1
                continue
            candidates.append(fingerprint)

        if not candidates:
            _set_status(
                last_scan_candidates=0,
                last_scan_processed=0,
                last_scan_skipped_retry=skipped_retry,
                last_scan_dead_nodes=dead_nodes,
                last_scan_alive_nodes=alive_nodes,
                queue_size=0,
            )
            return

        max_to_process = min(int(REREPLICATION_BATCH_LIMIT), len(candidates))
        for fingerprint in candidates[:max_to_process]:
            if stop_event().is_set():
                break
            if not is_writable_leader():
                break
            processed += 1
            try:
                result = _repair_chunk_once(fingerprint, alive_nodes)
                _mark_chunk_success(result)
            except Exception as exc:
                _mark_chunk_failure(fingerprint, str(exc), time.time())
                record_error(f"re-replication failed for {fingerprint}: {exc}")

        queue_size = max(0, len(candidates) - processed)
        _set_status(
            last_scan_candidates=len(candidates),
            last_scan_processed=processed,
            last_scan_skipped_retry=skipped_retry,
            last_scan_dead_nodes=dead_nodes,
            last_scan_alive_nodes=alive_nodes,
            queue_size=queue_size,
        )
    finally:
        _set_status(running=False)


def _rereplication_loop() -> None:
    interval_sec = max(1.0, float(REREPLICATION_SCAN_INTERVAL_SEC))
    event = stop_event()
    next_scan_ts = 0.0

    while not event.is_set():
        if not bool(REREPLICATION_ENABLE):
            _set_status(enabled=False, running=False, queue_size=0)
            event.wait(1.0)
            continue

        if not is_writable_leader():
            _set_status(enabled=True, running=False, queue_size=0)
            event.wait(min(1.0, interval_sec))
            continue

        now_ts = time.time()
        if now_ts < next_scan_ts:
            event.wait(min(0.5, next_scan_ts - now_ts))
            continue

        try:
            _scan_and_repair_once()
        except Exception as exc:
            _inc_status("failed_total", 1)
            _set_status(
                last_error={
                    "at": now_iso(),
                    "fingerprint": "",
                    "attempts": 0,
                    "blocked": False,
                    "next_retry_at": 0.0,
                    "error": f"scan_failed: {exc}",
                }
            )
            record_error(f"re-replication scan failed: {exc}")
        finally:
            next_scan_ts = time.time() + interval_sec


def start_rereplication_runtime() -> None:
    global _REREPL_THREAD
    with _REREPL_LOCK:
        existing = _REREPL_THREAD
        if existing is not None and existing.is_alive():
            return

    _reset_runtime_status()
    thread = threading.Thread(target=_rereplication_loop, name="meta-rereplication-runtime", daemon=True)
    with _REREPL_LOCK:
        _REREPL_THREAD = thread
    thread.start()


def stop_rereplication_runtime() -> None:
    global _REREPL_THREAD
    with _REREPL_LOCK:
        thread = _REREPL_THREAD

    if thread is not None and thread.is_alive():
        thread.join(timeout=2.0)

    with _REREPL_LOCK:
        _REREPL_THREAD = None
