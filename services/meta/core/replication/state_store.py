import copy
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional


# 复制运行时锁，保护 debug 状态字典与线程句柄。
_RUNTIME_LOCK = threading.RLock()
# 主循环停止信号。
_STOP_EVENT = threading.Event()
# 复制/接管统一后台线程句柄。
_RUNTIME_THREAD: Optional[threading.Thread] = None


# 保存复制、心跳、接管过程中的观测数据，供 /debug/replication 查询。
_RUNTIME: Dict[str, Any] = {
    "started_ts": time.time(),
    "last_heartbeat_sent_at": "",
    "last_heartbeat_success_at": "",
    "last_snapshot_sent_at": "",
    "last_snapshot_success_at": "",
    "last_error": "",
    "last_error_at": "",
    "last_leader_heartbeat_at": "",
    "last_leader_heartbeat_from": "",
    "last_leader_heartbeat_epoch": 0,
    "last_leader_heartbeat_lamport": 0,
    "last_leader_heartbeat_ts": 0.0,
    "last_sync_applied_at": "",
    "last_sync_source": "",
    "last_sync_reason": "",
    "last_sync_lamport": 0,
    "last_takeover_at": "",
    "last_takeover_reason": "",
    "last_takeover_result": "",
    "last_takeover_ts": 0.0,
    "last_takeover_detail": {},
    "election_in_progress": False,
}


# 统一 UTC 时间格式，便于日志和 debug 输出对齐。
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# 更新复制运行时状态字段。
def update_runtime(**fields: Any) -> None:
    with _RUNTIME_LOCK:
        _RUNTIME.update(fields)


# 记录运行异常，避免静默失败。
def record_error(msg: str) -> None:
    update_runtime(last_error=str(msg), last_error_at=now_iso())


# 获取复制运行时快照，避免调用方直接持有共享字典引用。
def read_runtime() -> Dict[str, Any]:
    with _RUNTIME_LOCK:
        return copy.deepcopy(_RUNTIME)


def set_runtime_thread(thread: Optional[threading.Thread]) -> None:
    global _RUNTIME_THREAD
    with _RUNTIME_LOCK:
        _RUNTIME_THREAD = thread


def get_runtime_thread() -> Optional[threading.Thread]:
    with _RUNTIME_LOCK:
        return _RUNTIME_THREAD


def reset_started_ts() -> None:
    update_runtime(started_ts=time.time())


def stop_event() -> threading.Event:
    return _STOP_EVENT
