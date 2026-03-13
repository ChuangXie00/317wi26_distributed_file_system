import json
import random
import time
import os
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
from urllib.error import URLError
from urllib.request import urlopen

from .config import (
    DATA_DIR,
    META_NODE_ID,
    METADATA_FILE,
    ENABLE_STORAGE_HEALTHCHECK,
    HEARTBEAT_TIMEOUT_SEC,
    STORAGE_HEALTHCHECK_TIMEOUT_SEC,
    STORAGE_NODES,
    STORAGE_PORT,
    get_meta_peer_nodes,
)
from .membership_codec import (
    new_meta_membership_entry as _new_meta_membership_entry,
    normalize_meta_role as _normalize_meta_role,
    normalize_meta_voted_for as _normalize_meta_voted_for,
)
from .meta_probe import probe_meta_runtime as _probe_meta_runtime
from .runtime import get_runtime_snapshot, is_writable_leader

State = Dict[str, Any]
MembershipEntry = Dict[str, Any]
StateMutator = Callable[[State], bool]

# global re-entrant lock, protect concurrent R/W of metadata.json
_STATE_LOCK = threading.RLock()

# current UNIX timestamp in sec
def _now_timestamp() -> float:
    return time.time()

# convert timestamp to UTC str
def _timestamp_to_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")

# default metadata shape
def _default_state() -> State:
    return {"files": {}, "chunks": {}, "membership": {}, "version": 1}

# normalize state schema, ensure top keys and types
def _normalize_state_schema(state: State) -> bool:
    changed = False
    if not isinstance(state.get("files"), dict):
        state["files"] = {}
        changed = True
    if not isinstance(state.get("chunks"), dict):
        state["chunks"] = {}
        changed = True
    if not isinstance(state.get("membership"), dict):
        state["membership"] = {}
        changed = True
    if not isinstance(state.get("version"), int):
        state["version"] = 1
        changed = True
    return changed

# atomic write
def _write_json_atomic(state: State) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = METADATA_FILE.with_name(f"{METADATA_FILE.name}.{uuid.uuid4().hex}.tmp")
    try:
        with tmp_path.open("w", encoding="utf-8", newline="\n") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, METADATA_FILE)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass

# read metadata.json without lock, caller must already hold lock
def _read_state_unlocked() -> State:
    try:
        payload = json.loads(METADATA_FILE.read_text(encoding="utf-8"))
        state: State = payload if isinstance(payload, dict) else _default_state()
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        state = _default_state()
    _normalize_state_schema(state)
    return state

# ensure meta file is usable (auto repair)
def ensure_metadata_file() -> None:
    with _STATE_LOCK:
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        if not METADATA_FILE.exists():
            _write_json_atomic(_default_state())
            return
        
        try:
            raw = METADATA_FILE.read_text(encoding="utf-8")
            if not raw.strip():
                _write_json_atomic(_default_state())
                return
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                _write_json_atomic(_default_state())
                return
            state: State = payload
            if _normalize_state_schema(state):
                _write_json_atomic(state)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            _write_json_atomic(_default_state())


# load state from metadata file, thread safety (0.1p3.1)
def load_state() -> State:
    with _STATE_LOCK:
        ensure_metadata_file()
        state = _read_state_unlocked()
        return state
    

# write state, thread safety. atomic file replacement (0.1p3.1)
def persist_state(state: State) -> None:
    with _STATE_LOCK:
        _normalize_state_schema(state)
        _write_json_atomic(state)


# transactional state mutation entry, read-modify-write under one lock (0.1p3.1)
def mutate_state(mutator: StateMutator) -> State:
    with _STATE_LOCK:
        ensure_metadata_file()
        state = _read_state_unlocked()
        changed = bool(mutator(state))
        if changed:
            _normalize_state_schema(state)
            _write_json_atomic(state)
        return state



# 将任意值安全转换为 int，避免脏数据导致 membership 刷新中断。
def _safe_int(raw_value: Any, default: int = 0) -> int:
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return int(default)


# 规范化 membership 状态枚举，确保只落盘 alive/dead/suspected。
def _normalize_status(raw_status: str) -> str:
    normalized = str(raw_status or "").strip().lower()
    if normalized in {"alive", "dead", "suspected"}:
        return normalized
    return "dead"


# 规范化节点类型，扩展 meta 节点 membership 时统一使用 node_type 区分。
def _normalize_node_type(raw_node_type: str, node_id: str) -> str:
    normalized = str(raw_node_type or "").strip().lower()
    if normalized in {"storage", "meta"}:
        return normalized
    return "meta" if str(node_id or "").strip().lower().startswith("meta-") else "storage"


# 生成标准 membership 条目（storage 默认结构）。
def _new_membership_entry(now_ts: float, status: str = "alive", node_type: str = "storage") -> MembershipEntry:
    return {
        "node_type": "meta" if str(node_type).strip().lower() == "meta" else "storage",
        "status": _normalize_status(status),
        "last_heartbeat_ts": float(now_ts),
        "last_heartbeat_at": _timestamp_to_iso(float(now_ts)),
    }


# 兼容旧版 membership 结构，并按 node_type 统一输出规范化条目。
def _coerce_membership_entry(raw: Any, now_ts: float, node_id: str = "") -> MembershipEntry:
    normalized_node_id = str(node_id or "").strip().lower()

    # 兼容 0.1p02 的字符串格式（value 仅有状态）。
    if isinstance(raw, str):
        if _normalize_node_type("", normalized_node_id) == "meta":
            return _new_meta_membership_entry(
                node_id=normalized_node_id,
                now_ts=now_ts,
                status=raw,
                source="legacy_string",
            )
        return _new_membership_entry(now_ts, status=raw, node_type="storage")

    if isinstance(raw, dict):
        node_type = _normalize_node_type(str(raw.get("node_type", "")), normalized_node_id)
        status = _normalize_status(raw.get("status", "alive"))
        ts_raw = raw.get("last_heartbeat_ts")
        hb_ts = float(ts_raw) if isinstance(ts_raw, (int, float)) else float(now_ts)
        hb_at = raw.get("last_heartbeat_at")
        if not hb_at:
            hb_at = _timestamp_to_iso(hb_ts)

        if node_type == "meta":
            normalized_term = max(0, _safe_int(raw.get("current_term", raw.get("term", raw.get("leader_epoch", 0)))))
            return {
                "node_type": "meta",
                "status": status,
                "last_heartbeat_ts": hb_ts,
                "last_heartbeat_at": str(hb_at),
                "role": _normalize_meta_role(raw.get("role", "follower")),
                "current_leader_id": str(raw.get("current_leader_id", raw.get("leader", ""))).strip().lower(),
                "leader_epoch": max(0, _safe_int(raw.get("leader_epoch", 0))),
                "current_term": normalized_term,
                "voted_for": _normalize_meta_voted_for(raw.get("voted_for", "")),
                "lamport": max(0, _safe_int(raw.get("lamport", 0))),
                "writable_leader": bool(raw.get("writable_leader", False)),
                "source": str(raw.get("source", "meta_membership")).strip() or "meta_membership",
                "node_id": normalized_node_id,
            }

        return {
            "node_type": "storage",
            "status": status,
            "last_heartbeat_ts": hb_ts,
            "last_heartbeat_at": str(hb_at),
        }

    if _normalize_node_type("", normalized_node_id) == "meta":
        return _new_meta_membership_entry(
            node_id=normalized_node_id,
            now_ts=now_ts,
            status="suspected",
            source="default_meta",
        )
    return _new_membership_entry(now_ts, status="alive", node_type="storage")


# 确保 membership schema 完整：storage 与 meta 节点条目都存在且格式规范。
def ensure_membership_schema(state: State, now_ts: Optional[float] = None) -> bool:
    now_ts = _now_timestamp() if now_ts is None else float(now_ts)
    changed = False

    membership_raw = state.setdefault("membership", {})
    if not isinstance(membership_raw, dict):
        state["membership"] = {}
        membership_raw = state["membership"]
        changed = True

    # 规范化现有条目（兼容旧格式与缺字段格式）。
    for node_id in list(membership_raw.keys()):
        normalized = _coerce_membership_entry(membership_raw[node_id], now_ts, node_id=node_id)
        if membership_raw[node_id] != normalized:
            membership_raw[node_id] = normalized
            changed = True

    # 保证 storage 节点条目完整。
    for node_id in STORAGE_NODES:
        if node_id not in membership_raw:
            membership_raw[node_id] = _new_membership_entry(now_ts, status="alive", node_type="storage")
            changed = True

    # 保证 meta 节点条目完整（自身默认 alive，peer 默认 suspected）。
    for node_id in [META_NODE_ID] + get_meta_peer_nodes():
        if node_id not in membership_raw:
            default_status = "alive" if node_id == META_NODE_ID else "suspected"
            membership_raw[node_id] = _new_meta_membership_entry(
                node_id=node_id,
                now_ts=now_ts,
                status=default_status,
                source="schema_init",
            )
            changed = True

    return changed


# 记录 storage heartbeat（可设置最小落盘间隔，降低频繁写放大）。
def mark_storage_heartbeat(
    state: State,
    node_id: str,
    now_ts: Optional[float] = None,
    min_interval_sec: float = 0.0,
) -> bool:
    now_ts = _now_timestamp() if now_ts is None else float(now_ts)
    changed = ensure_membership_schema(state, now_ts=now_ts)
    membership = state.setdefault("membership", {})

    old_entry = _coerce_membership_entry(membership.get(node_id), now_ts, node_id=node_id)
    elapsed = now_ts - float(old_entry.get("last_heartbeat_ts", 0.0))
    if old_entry.get("status") == "alive" and elapsed < max(0.0, float(min_interval_sec)):
        return changed

    new_entry = _new_membership_entry(now_ts, status="alive", node_type="storage")
    if old_entry != new_entry:
        changed = True
    membership[node_id] = new_entry
    return changed


# storage heartbeat 超时规则：alive/suspected 转为 dead。
def apply_storage_heartbeat_timeout(state: State, now_ts: Optional[float] = None) -> bool:
    now_ts = _now_timestamp() if now_ts is None else float(now_ts)
    changed = ensure_membership_schema(state, now_ts=now_ts)
    membership = state.setdefault("membership", {})

    for node_id in STORAGE_NODES:
        entry = _coerce_membership_entry(membership.get(node_id), now_ts, node_id=node_id)
        elapsed = now_ts - float(entry.get("last_heartbeat_ts", 0.0))

        # alive/suspected 节点超时后统一标记 dead。
        if entry["status"] in {"alive", "suspected"} and elapsed > HEARTBEAT_TIMEOUT_SEC:
            entry["status"] = "dead"
            changed = True

        membership[node_id] = entry

    return changed


# 刷新 storage membership（schema 修复 + 超时淘汰）。
def refresh_storage_membership(state: State, now_ts: Optional[float] = None) -> bool:
    now_ts = _now_timestamp() if now_ts is None else float(now_ts)
    changed = ensure_membership_schema(state, now_ts=now_ts)
    if apply_storage_heartbeat_timeout(state, now_ts=now_ts):
        changed = True
    return changed


# 刷新 meta membership：写入本地 runtime，并探测 peer 节点存活与 leader 视图。
def refresh_meta_membership(state: State, now_ts: Optional[float] = None) -> bool:
    now_ts = _now_timestamp() if now_ts is None else float(now_ts)
    changed = ensure_membership_schema(state, now_ts=now_ts)
    membership = state.setdefault("membership", {})

    # 本地节点直接用 runtime 真值写入 membership。
    runtime_snapshot = get_runtime_snapshot()
    local_entry = _new_meta_membership_entry(
        node_id=META_NODE_ID,
        now_ts=now_ts,
        status="alive",
        role=str(runtime_snapshot.get("role", "follower")),
        current_leader_id=str(runtime_snapshot.get("current_leader_id", "")),
        leader_epoch=max(0, _safe_int(runtime_snapshot.get("leader_epoch", 0))),
        current_term=max(0, _safe_int(runtime_snapshot.get("current_term", runtime_snapshot.get("leader_epoch", 0)))),
        voted_for=str(runtime_snapshot.get("voted_for", "")),
        lamport=max(0, _safe_int(runtime_snapshot.get("lamport_clock", 0))),
        writable_leader=is_writable_leader(),
        source="local_runtime",
    )
    if membership.get(META_NODE_ID) != local_entry:
        membership[META_NODE_ID] = local_entry
        changed = True

    # peer 节点通过探测更新；探测失败时标记 dead 并保留上次观测的 role/epoch/term/lamport。
    for node_id in get_meta_peer_nodes():
        previous_entry = _coerce_membership_entry(membership.get(node_id), now_ts, node_id=node_id)
        peer_status = "dead"
        peer_payload: Dict[str, Any] = {}
        peer_source = "peer_probe_failed"

        try:
            peer_payload = _probe_meta_runtime(node_id)
            peer_status = "alive"
            peer_source = "peer_probe"
        except (URLError, TimeoutError, OSError, RuntimeError, UnicodeDecodeError, json.JSONDecodeError):
            pass

        if peer_status == "alive":
            peer_entry = _new_meta_membership_entry(
                node_id=node_id,
                now_ts=now_ts,
                status=peer_status,
                role=str(peer_payload.get("role", previous_entry.get("role", "follower"))),
                current_leader_id=str(
                    peer_payload.get(
                        "leader",
                        peer_payload.get("current_leader_id", previous_entry.get("current_leader_id", "")),
                    )
                ),
                leader_epoch=max(0, _safe_int(peer_payload.get("leader_epoch", previous_entry.get("leader_epoch", 0)))),
                current_term=max(
                    0,
                    _safe_int(
                        peer_payload.get(
                            "current_term",
                            peer_payload.get("term", previous_entry.get("current_term", previous_entry.get("leader_epoch", 0))),
                        )
                    ),
                ),
                voted_for=str(peer_payload.get("voted_for", previous_entry.get("voted_for", ""))),
                lamport=max(0, _safe_int(peer_payload.get("lamport", previous_entry.get("lamport", 0)))),
                writable_leader=bool(peer_payload.get("writable_leader", previous_entry.get("writable_leader", False))),
                source=peer_source,
            )
        else:
            # peer 不可达时主动降级为 unknown，避免沿用陈旧角色造成“dead 但仍是 leader/follower”的误导。
            peer_entry = _new_meta_membership_entry(
                node_id=node_id,
                now_ts=now_ts,
                status="dead",
                role="unknown",
                current_leader_id="",
                leader_epoch=0,
                current_term=max(0, _safe_int(previous_entry.get("current_term", previous_entry.get("leader_epoch", 0)))),
                voted_for="",
                lamport=max(0, _safe_int(previous_entry.get("lamport", 0))),
                writable_leader=False,
                source=peer_source,
            )
        if previous_entry != peer_entry:
            membership[node_id] = peer_entry
            changed = True

    return changed


# 刷新完整 cluster membership：storage 心跳状态 + meta 节点状态。
def refresh_cluster_membership(state: State, now_ts: Optional[float] = None) -> bool:
    now_ts = _now_timestamp() if now_ts is None else float(now_ts)
    changed = False
    if refresh_storage_membership(state, now_ts=now_ts):
        changed = True
    if refresh_meta_membership(state, now_ts=now_ts):
        changed = True
    return changed


# 返回 membership 快照（同时覆盖 storage 与 meta 条目）。
def get_membership_snapshot(state: State) -> Dict[str, MembershipEntry]:
    now_ts = _now_timestamp()
    ensure_membership_schema(state, now_ts=now_ts)
    membership = state.get("membership", {})

    out: Dict[str, MembershipEntry] = {}
    for node_id in sorted(membership.keys()):
        out[node_id] = _coerce_membership_entry(membership.get(node_id), now_ts, node_id=node_id)
    return out

# lightweight health check for storage nodes
# used in 0.1_phase02
def _storage_health_url(node_id: str) -> str:
    return f"http://{node_id}:{STORAGE_PORT}/health"

def _is_storage_alive(node_id: str) -> bool:
    try:
        with urlopen(_storage_health_url(node_id), timeout=STORAGE_HEALTHCHECK_TIMEOUT_SEC) as response:
            return response.status == 200
    except (URLError, TimeoutError, OSError):
        return False


# 基于membership计算可用storage节点
def get_alive_storage_nodes(state: State) -> List[str]:
    # 0.1p03 use membership status to determine alive nodes
    membership = get_membership_snapshot(state)
    alive_nodes = [node_id for node_id in STORAGE_NODES if membership.get(node_id, {}).get("status") == "alive"]

    # keep 0.1p02 behavior if health check is disabled
    if not ENABLE_STORAGE_HEALTHCHECK:
        return alive_nodes

    return [node_id for node_id in alive_nodes if _is_storage_alive(node_id)]


# choose n nodes as replicas from alive nodes
def choose_replicas(alive_nodes: List[str], replica_count: int) -> List[str]:
    # select n nodes from alive_nodes, if not enough, raise error
    if replica_count <= 0:
        return []
    if len(alive_nodes) < replica_count:
        raise ValueError("not enough replicas available")
    return random.sample(alive_nodes, replica_count)
