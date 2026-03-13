import time
import threading
from typing import Any, Dict

from ..config import (
    META_HEARTBEAT_INTERVAL_SEC,
    META_LEADER_HEARTBEAT_TIMEOUT_SEC,
    META_LEADER_URL,
    META_NODE_ID,
    META_SYNC_INTERVAL_SEC,
)
from ..runtime import get_runtime_snapshot, is_writable_leader
from .heartbeat_sync import _peer_urls, _send_heartbeat_to_peers, push_state_to_followers
from .state_store import get_runtime_thread, read_runtime, reset_started_ts, set_runtime_thread, stop_event
from .takeover_scheduler import _maybe_takeover_by_timeout


# 统一后台循环：leader 负责发送 heartbeat+sync；follower 负责超时接管。
def _replication_runtime_loop() -> None:
    hb_interval = max(0.5, float(META_HEARTBEAT_INTERVAL_SEC))
    sync_interval = max(hb_interval, float(META_SYNC_INTERVAL_SEC))
    next_sync_monotonic = 0.0
    event = stop_event()

    while not event.is_set():
        if is_writable_leader():
            _send_heartbeat_to_peers()
            now_monotonic = time.monotonic()
            if now_monotonic >= next_sync_monotonic:
                push_state_to_followers(reason="periodic_sync")
                next_sync_monotonic = now_monotonic + sync_interval
        else:
            _maybe_takeover_by_timeout()

        event.wait(hb_interval)


# 启动复制/接管运行时线程。
def start_replication_runtime() -> None:
    thread = get_runtime_thread()
    if thread is not None and thread.is_alive():
        return

    event = stop_event()
    event.clear()
    reset_started_ts()
    thread = threading.Thread(target=_replication_runtime_loop, name="meta-replication-runtime", daemon=True)
    set_runtime_thread(thread)
    thread.start()


# 停止复制/接管运行时线程。
def stop_replication_runtime() -> None:
    event = stop_event()
    event.set()

    thread = get_runtime_thread()
    if thread is not None and thread.is_alive():
        thread.join(timeout=2.0)
    set_runtime_thread(None)


# 输出复制与接管状态，供 debug API 观测真实 leader/runtime 信息。
def get_replication_status() -> Dict[str, Any]:
    data = read_runtime()
    thread = get_runtime_thread()
    thread_alive = thread.is_alive() if thread is not None else False

    last_ts = float(data.get("last_leader_heartbeat_ts", 0.0) or 0.0)
    elapsed_sec = max(0.0, time.time() - last_ts) if last_ts > 0 else None
    leader_alive = (elapsed_sec is not None) and (elapsed_sec <= META_LEADER_HEARTBEAT_TIMEOUT_SEC)
    runtime_snapshot = get_runtime_snapshot()

    return {
        "node_id": META_NODE_ID,
        "runtime": runtime_snapshot,
        "runtime_thread_alive": thread_alive,
        "peer_urls": _peer_urls(),
        "legacy_leader_url": META_LEADER_URL,
        "leader_heartbeat": {
            "source_node_id": data.get("last_leader_heartbeat_from", ""),
            "leader_epoch": int(data.get("last_leader_heartbeat_epoch", 0)),
            "leader_lamport": int(data.get("last_leader_heartbeat_lamport", 0)),
            "last_observed_at": data.get("last_leader_heartbeat_at", ""),
            "elapsed_sec": round(elapsed_sec, 3) if elapsed_sec is not None else None,
            "timeout_sec": META_LEADER_HEARTBEAT_TIMEOUT_SEC,
            "alive": bool(leader_alive),
        },
        "replication": {
            "last_heartbeat_sent_at": data.get("last_heartbeat_sent_at", ""),
            "last_heartbeat_success_at": data.get("last_heartbeat_success_at", ""),
            "last_snapshot_sent_at": data.get("last_snapshot_sent_at", ""),
            "last_snapshot_success_at": data.get("last_snapshot_success_at", ""),
            "last_sync_applied_at": data.get("last_sync_applied_at", ""),
            "last_sync_source": data.get("last_sync_source", ""),
            "last_sync_reason": data.get("last_sync_reason", ""),
            "last_sync_lamport": int(data.get("last_sync_lamport", 0)),
            "last_applied_lamport": int(runtime_snapshot.get("last_applied_lamport", 0)),
        },
        "takeover": {
            "last_takeover_at": data.get("last_takeover_at", ""),
            "last_takeover_reason": data.get("last_takeover_reason", ""),
            "last_takeover_result": data.get("last_takeover_result", ""),
            "last_takeover_detail": data.get("last_takeover_detail", {}),
            "election_in_progress": bool(data.get("election_in_progress", False)),
        },
        "error": {
            "last_error": data.get("last_error", ""),
            "last_error_at": data.get("last_error_at", ""),
        },
    }
