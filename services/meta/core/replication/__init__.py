from .heartbeat_sync import (
    _get_json,
    _peer_urls,
    _post_json,
    _safe_int,
    _sanitize_membership,
    _send_heartbeat_to_peers,
    apply_replicated_state,
    build_state_snapshot,
    probe_cluster_leader_for_rejoin,
    push_state_to_followers,
    record_leader_heartbeat,
)
from .runtime_loop import (
    _replication_runtime_loop,
    get_replication_status,
    start_replication_runtime,
    stop_replication_runtime,
)
from .state_store import record_error, update_runtime
from .takeover_scheduler import (
    _maybe_takeover_by_timeout,
    _quorum_timeout_offset_sec,
    should_takeover_by_timeout,
    trigger_takeover,
    trigger_takeover_async,
)


# 中文：保留旧模块私有函数名，避免重构过程中的隐式依赖断裂。
_update_runtime = update_runtime
_record_error = record_error

__all__ = [
    "probe_cluster_leader_for_rejoin",
    "build_state_snapshot",
    "push_state_to_followers",
    "record_leader_heartbeat",
    "apply_replicated_state",
    "trigger_takeover",
    "trigger_takeover_async",
    "start_replication_runtime",
    "stop_replication_runtime",
    "get_replication_status",
]
