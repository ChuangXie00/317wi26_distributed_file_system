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
    METADATA_FILE,
    ENABLE_STORAGE_HEALTHCHECK,
    HEARTBEAT_TIMEOUT_SEC,
    STORAGE_HEALTHCHECK_TIMEOUT_SEC,
    STORAGE_NODES,
    STORAGE_PORT,
)

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



# normalize membership status
def _normalize_status(raw_status: str) -> str:
    normalized = str(raw_status or "").strip().lower()
    if normalized in {"alive", "dead", "suspected"}:
        return normalized
    return "dead"


# build a normalized membership entry.
def _new_membership_entry(now_ts: float, status: str = "alive") -> MembershipEntry:
    return {
        "status": _normalize_status(status),
        "last_heartbeat_ts": float(now_ts),
        "last_heartbeat_at": _timestamp_to_iso(float(now_ts)),
    }

# coerce legacy membership entry formats into current structured format
def _coerce_membership_entry(raw: Any, now_ts: float) -> MembershipEntry:
    # compatible with old format in 0.1p02(membership may be str)
    if isinstance(raw, str):
        return _new_membership_entry(now_ts, status = raw)
    
    if isinstance(raw, dict):
        status = _normalize_status(raw.get("status", "alive"))
        ts_raw = raw.get("last_heartbeat_ts")
        hb_ts = float(ts_raw) if isinstance(ts_raw, (int, float)) else float(now_ts)
        hb_at = raw.get("last_heartbeat_at")
        if not hb_at:
            hb_at = _timestamp_to_iso(hb_ts)
        return {
            "status": status,
            "last_heartbeat_ts": hb_ts,
            "last_heartbeat_at": str(hb_at),
        }
    
    return _new_membership_entry(now_ts, status="alive")


# ensure membership schema completeness and all storage node entries exist
def ensure_membership_schema(state: State, now_ts: Optional[float] = None) -> bool:
    now_ts = _now_timestamp() if now_ts is None else float(now_ts)
    changed = False

    membership_raw = state.setdefault("membership", {})
    if not isinstance(membership_raw, dict):
        state["membership"] = {}
        membership_raw = state["membership"]
        changed = True

    # normalize current str(compatible with old format)
    for node_id in list(membership_raw.keys()):
        normalized = _coerce_membership_entry(membership_raw[node_id], now_ts)
        if membership_raw[node_id] != normalized:
            membership_raw[node_id] = normalized
            changed = True

    for node_id in STORAGE_NODES:
        if node_id not in membership_raw:
            membership_raw[node_id] = _new_membership_entry(now_ts, status="alive")
            changed = True
    
    return changed


# wark storage heartbeat with optional min write interval to reduce write amplification
def mark_storage_heartbeat(
    state: State,
    node_id: str, 
    now_ts: Optional[float] = None,
    min_interval_sec: float = 0.0,
) -> bool:
    now_ts = _now_timestamp() if now_ts is None else float(now_ts)
    changed = ensure_membership_schema(state, now_ts=now_ts)
    membership = state.setdefault("membership", {})

    old_entry = _coerce_membership_entry(membership.get(node_id), now_ts)
    elapsed = now_ts - float(old_entry.get("last_heartbeat_ts", 0.0))
    if old_entry.get("status") == "alive" and elapsed < max(0.0, float(min_interval_sec)):
        return changed

    new_entry = _new_membership_entry(now_ts, status="alive")
    if old_entry != new_entry:
        changed = True
    membership[node_id] = new_entry
    return changed


# heartbeat timeout rule: transition alive/suspected nodes to dead.
def apply_storage_heartbeat_timeout(state: State, now_ts: Optional[float] = None) -> bool:
    now_ts = _now_timestamp() if now_ts is None else float(now_ts)
    changed = ensure_membership_schema(state, now_ts=now_ts)
    membership = state.setdefault("membership", {})

    for node_id in STORAGE_NODES:
        entry = _coerce_membership_entry(membership.get(node_id), now_ts)
        elapsed = now_ts - float(entry.get("last_heartbeat_ts", 0.0))

        # alive/suspected 节点超时后统一标记 dead
        if entry["status"] in {"alive", "suspected"} and elapsed > HEARTBEAT_TIMEOUT_SEC:
            entry["status"] = "dead"
            changed = True

        membership[node_id] = entry

    return changed

# 统一membership刷新流程(schema修复+超时处理)
def refresh_storage_membership(state: State, now_ts: Optional[float] = None) -> bool:
    now_ts = _now_timestamp() if now_ts is None else float(now_ts)
    changed = ensure_membership_schema(state, now_ts=now_ts)
    if apply_storage_heartbeat_timeout(state, now_ts=now_ts):
        changed = True
    return changed


def get_membership_snapshot(state: State) -> Dict[str, MembershipEntry]:
    now_ts = _now_timestamp()
    ensure_membership_schema(state, now_ts=now_ts)
    membership = state.get("membership", {})

    out: Dict[str, MembershipEntry] = {}
    for node_id in sorted(membership.keys()):
        out[node_id] = _coerce_membership_entry(membership.get(node_id), now_ts)
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