import copy
import json
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request as UrlRequest, urlopen

from .config import (
    META_FOLLOWER_URLS,
    META_HEARTBEAT_INTERVAL_SEC,
    META_INTERNAL_TIMEOUT_SEC,
    META_LEADER_HEARTBEAT_TIMEOUT_SEC,
    META_LEADER_URL,
    META_NODE_ID,
    META_SYNC_INTERVAL_SEC,
    ROLE,
)
from .state import (
    State,
    get_membership_snapshot,
    load_state,
    mutate_state,
    persist_state,
    refresh_storage_membership,
)

_RUNTIME_LOCK = threading.RLock()
_STOP_EVENT = threading.Event()
_LEADER_THREAD: Optional[threading.Thread] = None


_RUNTIME: Dict[str, Any] = {
    "last_heartbeat_sent_at": "",
    "last_heartbeat_success_at": "",
    "last_snapshot_sent_at": "",
    "last_snapshot_success_at": "",
    "last_error": "",
    "last_error_at": "",
    "last_leader_heartbeat_at": "",
    "last_leader_heartbeat_from": "",
    "last_leader_heartbeat_ts": 0.0,
    "last_sync_applied_at": "",
    "last_sync_source": "",
    "last_sync_reason": "",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _update_runtime(**fields: Any) -> None:
    with _RUNTIME_LOCK:
        _RUNTIME.update(fields)


def _record_error(msg: str) -> None:
    _update_runtime(last_error=str(msg), last_error_at=_now_iso())


def _post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = UrlRequest(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urlopen(req, timeout=META_INTERNAL_TIMEOUT_SEC) as resp:
        if resp.status != 200:
            raise RuntimeError(f"internal POST failed: status={resp.status}, url={url}")
        raw = resp.read()
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {}


# 0.1p04 只同步运行状态 membership (files/chunks在 PostgreSQL)
def build_state_snapshot(reason: str = "manual") -> Dict[str, Any]:
    state = load_state()
    if ROLE == "leader" and refresh_storage_membership(state):
        persist_state(state)

    return {
        "source_node_id": META_NODE_ID,
        "generated_at": _now_iso(),
        "reason": reason,
        "membership": get_membership_snapshot(state),
    }

# leader 向 followers 推送state
def push_state_to_followers(reason: str = "manual") -> Dict[str, Any]:
    if ROLE != "leader":
        return {"status": "skipped", "reason": "node is not leader", "attempted": 0, "succeeded": 0, "failed": []}

    followers = [u.rstrip("/") for u in META_FOLLOWER_URLS if u.strip()]
    if not followers:
        return {"status": "skipped", "reason": "no follower configured", "attempted": 0, "succeeded": 0, "failed": []}

    snapshot = build_state_snapshot(reason=reason)
    _update_runtime(last_snapshot_sent_at=_now_iso())

    failed: List[Dict[str, str]] = []
    success_count = 0

    for follower_base in followers:
        try:
            _post_json(f"{follower_base}/internal/replicate_state", snapshot)
            success_count += 1
            _update_runtime(last_snapshot_success_at=_now_iso())
        except (HTTPError, URLError, OSError, RuntimeError) as exc:
            failed.append({"follower": follower_base, "error": str(exc)})
            _record_error(f"replicate_state failed to {follower_base}: {exc}")

    return {
        "status": "ok",
        "reason": reason,
        "attempted": len(followers),
        "succeeded": success_count,
        "failed": failed,
    }


def record_leader_heartbeat(leader_id: str) -> str:
    observed_at = _now_iso()
    _update_runtime(
        last_leader_heartbeat_at=observed_at,
        last_leader_heartbeat_from=str(leader_id).strip(),
        last_leader_heartbeat_ts=time.time(),
    )
    return observed_at


def _sanitize_membership(raw_membership: Any) -> Dict[str, Dict[str, Any]]:
    if not isinstance(raw_membership, dict):
        raise ValueError("membership must be an object")

    out: Dict[str, Dict[str, Any]] = {}
    for node_id, entry in raw_membership.items():
        node_key = str(node_id).strip()
        if not node_key:
            continue

        if isinstance(entry, dict):
            out[node_key] = copy.deepcopy(entry)
        else:
            # 中文：兼容旧格式（value 可能是状态字符串）
            out[node_key] = {"status": str(entry)}
    return out


def apply_replicated_state(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    source_node_id = str(snapshot.get("source_node_id", "")).strip() or "unknown"
    reason = str(snapshot.get("reason", "unknown")).strip() or "unknown"
    membership = _sanitize_membership(snapshot.get("membership"))

    changed_holder = {"changed": False}

    def _mutator(state: State) -> bool:
        current = state.get("membership", {})
        if current == membership:
            return False
        # 中文：follower 直接应用 leader 快照，保持 warm standby。
        state["membership"] = copy.deepcopy(membership)
        changed_holder["changed"] = True
        return True

    mutate_state(_mutator)

    applied_at = _now_iso()
    _update_runtime(
        last_sync_applied_at=applied_at,
        last_sync_source=source_node_id,
        last_sync_reason=reason,
    )
    return {"applied_at": applied_at, "changed": changed_holder["changed"]}


def _send_heartbeat_to_followers() -> None:
    followers = [u.rstrip("/") for u in META_FOLLOWER_URLS if u.strip()]
    if not followers:
        return

    payload = {"leader_id": META_NODE_ID, "sent_at": _now_iso()}
    _update_runtime(last_heartbeat_sent_at=payload["sent_at"])

    for follower_base in followers:
        try:
            _post_json(f"{follower_base}/internal/heartbeat", payload)
            _update_runtime(last_heartbeat_success_at=_now_iso())
        except (HTTPError, URLError, OSError, RuntimeError) as exc:
            _record_error(f"leader heartbeat failed to {follower_base}: {exc}")


def _leader_replication_loop() -> None:
    hb_interval = max(0.5, float(META_HEARTBEAT_INTERVAL_SEC))
    sync_interval = max(hb_interval, float(META_SYNC_INTERVAL_SEC))
    next_sync_monotonic = 0.0

    while not _STOP_EVENT.is_set():
        _send_heartbeat_to_followers()

        now_monotonic = time.monotonic()
        if now_monotonic >= next_sync_monotonic:
            push_state_to_followers(reason="periodic_sync")
            next_sync_monotonic = now_monotonic + sync_interval

        _STOP_EVENT.wait(hb_interval)


def start_replication_runtime() -> None:
    if ROLE != "leader":
        return

    followers = [u.strip() for u in META_FOLLOWER_URLS if u.strip()]
    if not followers:
        return

    global _LEADER_THREAD
    with _RUNTIME_LOCK:
        if _LEADER_THREAD is not None and _LEADER_THREAD.is_alive():
            return
        _STOP_EVENT.clear()
        _LEADER_THREAD = threading.Thread(
            target=_leader_replication_loop,
            name="meta-leader-replication",
            daemon=True,
        )
        _LEADER_THREAD.start()


def stop_replication_runtime() -> None:
    global _LEADER_THREAD
    _STOP_EVENT.set()
    if _LEADER_THREAD is not None and _LEADER_THREAD.is_alive():
        _LEADER_THREAD.join(timeout=2.0)
    _LEADER_THREAD = None


def get_replication_status() -> Dict[str, Any]:
    with _RUNTIME_LOCK:
        data = copy.deepcopy(_RUNTIME)
        thread_alive = _LEADER_THREAD.is_alive() if _LEADER_THREAD is not None else False

    if ROLE == "leader":
        return {
            "node_id": META_NODE_ID,
            "role": ROLE,
            "followers": [u.rstrip("/") for u in META_FOLLOWER_URLS if u.strip()],
            "runtime_thread_alive": thread_alive,
            "last_heartbeat_sent_at": data["last_heartbeat_sent_at"],
            "last_heartbeat_success_at": data["last_heartbeat_success_at"],
            "last_snapshot_sent_at": data["last_snapshot_sent_at"],
            "last_snapshot_success_at": data["last_snapshot_success_at"],
            "last_error": data["last_error"],
            "last_error_at": data["last_error_at"],
        }

    last_ts = float(data.get("last_leader_heartbeat_ts", 0.0) or 0.0)
    elapsed_sec = max(0.0, time.time() - last_ts) if last_ts > 0 else None
    leader_alive = (elapsed_sec is not None) and (elapsed_sec <= META_LEADER_HEARTBEAT_TIMEOUT_SEC)

    return {
        "node_id": META_NODE_ID,
        "role": ROLE,
        "leader_url": META_LEADER_URL,
        "leader_heartbeat": {
            "source_node_id": data["last_leader_heartbeat_from"],
            "last_observed_at": data["last_leader_heartbeat_at"],
            "elapsed_sec": round(elapsed_sec, 3) if elapsed_sec is not None else None,
            "timeout_sec": META_LEADER_HEARTBEAT_TIMEOUT_SEC,
            "alive": bool(leader_alive),
        },
        "last_sync_applied_at": data["last_sync_applied_at"],
        "last_sync_source": data["last_sync_source"],
        "last_sync_reason": data["last_sync_reason"],
        "last_error": data["last_error"],
        "last_error_at": data["last_error_at"],
    }