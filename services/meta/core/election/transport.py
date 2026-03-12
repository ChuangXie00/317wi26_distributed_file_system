import json
from typing import Any, Dict
from urllib.request import Request as UrlRequest, urlopen

from ..config import META_INTERNAL_TIMEOUT_SEC


# 统一 HTTP JSON POST 工具，供 election/coordinator 内部通信复用。
def post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = UrlRequest(url, data=body, method="POST", headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=META_INTERNAL_TIMEOUT_SEC) as resp:
        if resp.status != 200:
            raise RuntimeError(f"internal POST failed: status={resp.status}, url={url}")
        raw = resp.read()
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))
