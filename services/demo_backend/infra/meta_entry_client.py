import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


@dataclass
class UpstreamResult:
    # 单个上游接口请求结果：成功时带 data，失败时带 error。
    source: str
    ok: bool
    data: dict[str, Any] | None = None
    error: str | None = None


class MetaEntryClient:
    def __init__(self, timeout_sec: float = 0.8) -> None:
        # 默认对接 meta-entry/nginx debug 端口，支持环境变量覆盖。
        base_url = os.getenv("DEMO_META_ENTRY_BASE_URL", "http://meta-entry:8000").rstrip("/")
        self.timeout_sec = timeout_sec
        # 四个标准 debug 源，后续由聚合器统一归并。
        self.endpoints = {
            "entry": os.getenv("DEMO_ENTRY_URL", f"{base_url}/debug/entry"),
            "membership": os.getenv("DEMO_MEMBERSHIP_URL", f"{base_url}/debug/membership"),
            "leader": os.getenv("DEMO_LEADER_URL", f"{base_url}/debug/leader"),
            "replication": os.getenv("DEMO_REPLICATION_URL", f"{base_url}/debug/replication"),
        }

    def fetch_all_parallel(self) -> dict[str, UpstreamResult]:
        # 并发拉取四个上游，保证单轮聚合时延受最慢接口而非总和限制。
        results: dict[str, UpstreamResult] = {}
        with ThreadPoolExecutor(max_workers=len(self.endpoints)) as executor:
            future_map = {
                executor.submit(self._fetch_json, source, url): source
                for source, url in self.endpoints.items()
            }
            for future in as_completed(future_map):
                source = future_map[future]
                try:
                    payload = future.result()
                    results[source] = UpstreamResult(source=source, ok=True, data=payload)
                except Exception as exc:
                    # 失败不抛到上层，保留错误信息供聚合器做降级。
                    results[source] = UpstreamResult(source=source, ok=False, error=str(exc))
        return results

    def _fetch_json(self, source: str, url: str) -> dict[str, Any]:
        # 单源请求：校验 HTTP 状态码并解析 JSON 对象。
        request = Request(url, headers={"Accept": "application/json"}, method="GET")
        try:
            with urlopen(request, timeout=self.timeout_sec) as response:
                if response.status != 200:
                    raise RuntimeError(f"{source} upstream returned status={response.status}")
                body = response.read()
        except HTTPError as exc:
            # 明确保留上游返回码，便于定位是 4xx 还是 5xx。
            raise RuntimeError(f"{source} upstream http error={exc.code}") from exc
        except URLError as exc:
            raise RuntimeError(f"{source} upstream unreachable: {exc.reason}") from exc
        except TimeoutError as exc:
            raise RuntimeError(f"{source} upstream timeout") from exc

        try:
            decoded = json.loads(body.decode("utf-8"))
            if not isinstance(decoded, dict):
                raise ValueError("payload is not object")
            return decoded
        except Exception as exc:
            # 返回非 JSON 或结构非法时统一标记为 invalid json。
            raise RuntimeError(f"{source} upstream invalid json: {exc}") from exc
