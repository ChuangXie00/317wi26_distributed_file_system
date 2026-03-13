import copy
import json
import os
import threading
import time
from typing import Any, Dict
from urllib.request import urlopen

from .config import META_INTERNAL_TIMEOUT_SEC, build_meta_base_url


# peer 探测缓存：避免 dead 节点导致每次 debug 刷新都阻塞。
_PROBE_CACHE_LOCK = threading.RLock()
_PROBE_CACHE: Dict[str, Dict[str, Any]] = {}
_PROBE_SUCCESS_CACHE_SEC = max(0.2, float(os.getenv("META_PROBE_SUCCESS_CACHE_SEC", "1.0")))
_PROBE_FAILURE_CACHE_SEC = max(0.5, float(os.getenv("META_PROBE_FAILURE_CACHE_SEC", "15.0")))


def _cache_get(node_id: str) -> Dict[str, Any] | None:
    now_ts = time.time()
    with _PROBE_CACHE_LOCK:
        entry = _PROBE_CACHE.get(node_id)
        if not isinstance(entry, dict):
            return None
        ok = bool(entry.get("ok", False))
        ttl = _PROBE_SUCCESS_CACHE_SEC if ok else _PROBE_FAILURE_CACHE_SEC
        cached_at = float(entry.get("cached_at", 0.0) or 0.0)
        if cached_at <= 0 or now_ts - cached_at > ttl:
            _PROBE_CACHE.pop(node_id, None)
            return None
        return copy.deepcopy(entry)


def _cache_set_success(node_id: str, payload: Dict[str, Any]) -> None:
    with _PROBE_CACHE_LOCK:
        _PROBE_CACHE[node_id] = {
            "ok": True,
            "cached_at": time.time(),
            "payload": copy.deepcopy(payload),
            "error": "",
        }


def _cache_set_failure(node_id: str, error: str) -> None:
    with _PROBE_CACHE_LOCK:
        _PROBE_CACHE[node_id] = {
            "ok": False,
            "cached_at": time.time(),
            "payload": {},
            "error": str(error),
        }


# 探测单个 meta peer 的运行态（用于更新 meta membership 条目）。
def probe_meta_runtime(node_id: str) -> Dict[str, Any]:
    normalized_node_id = str(node_id or "").strip().lower()
    if not normalized_node_id:
        raise RuntimeError("meta probe failed: empty node_id")

    cached = _cache_get(normalized_node_id)
    if cached is not None:
        if bool(cached.get("ok", False)):
            payload = cached.get("payload", {})
            return copy.deepcopy(payload) if isinstance(payload, dict) else {}
        raise RuntimeError(
            f"meta probe cached failure: node={normalized_node_id}, error={cached.get('error', 'unknown')}"
        )

    # 使用 internal/current_leader 轻量接口，避免调用 debug/leader 触发级联刷新。
    probe_url = f"{build_meta_base_url(normalized_node_id)}/internal/current_leader"
    # debug membership 刷新链路优先追求“快速失败”，避免单个 dead peer 拖慢整体接口耗时。
    probe_timeout_sec = max(0.1, min(float(META_INTERNAL_TIMEOUT_SEC), 0.8))
    try:
        with urlopen(probe_url, timeout=probe_timeout_sec) as response:
            if response.status != 200:
                raise RuntimeError(f"meta probe failed: status={response.status}, url={probe_url}")
            raw = response.read()
            payload = json.loads(raw.decode("utf-8")) if raw else {}
            if not isinstance(payload, dict):
                raise RuntimeError(f"meta probe payload invalid: url={probe_url}")
            _cache_set_success(normalized_node_id, payload)
            return payload
    except Exception as exc:
        _cache_set_failure(normalized_node_id, str(exc))
        raise
