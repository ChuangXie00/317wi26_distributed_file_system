import json
from typing import Any, Dict
from urllib.request import urlopen

from .config import META_INTERNAL_TIMEOUT_SEC, build_meta_base_url


# 探测单个 meta peer 的 leader 运行态（用于更新 meta membership 条目）。
def probe_meta_runtime(node_id: str) -> Dict[str, Any]:
    probe_url = f"{build_meta_base_url(node_id)}/debug/leader"
    with urlopen(probe_url, timeout=META_INTERNAL_TIMEOUT_SEC) as response:
        if response.status != 200:
            raise RuntimeError(f"meta probe failed: status={response.status}, url={probe_url}")
        raw = response.read()
        payload = json.loads(raw.decode("utf-8")) if raw else {}
        if not isinstance(payload, dict):
            raise RuntimeError(f"meta probe payload invalid: url={probe_url}")
        return payload
