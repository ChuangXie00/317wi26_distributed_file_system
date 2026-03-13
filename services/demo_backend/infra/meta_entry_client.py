import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


@dataclass
class UpstreamResult:
    source: str
    ok: bool
    data: dict[str, Any] | None = None
    error: str | None = None


class MetaEntryClient:
    def __init__(self, timeout_sec: float = 0.8) -> None:
        base_url = os.getenv("DEMO_META_ENTRY_BASE_URL", "http://meta-entry:8000").rstrip("/")
        self.timeout_sec = timeout_sec
        self.endpoints = {
            "entry": os.getenv("DEMO_ENTRY_URL", f"{base_url}/debug/entry"),
            "membership": os.getenv("DEMO_MEMBERSHIP_URL", f"{base_url}/debug/membership"),
            "leader": os.getenv("DEMO_LEADER_URL", f"{base_url}/debug/leader"),
            "replication": os.getenv("DEMO_REPLICATION_URL", f"{base_url}/debug/replication"),
        }

    def fetch_all_parallel(self) -> dict[str, UpstreamResult]:
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
                    results[source] = UpstreamResult(source=source, ok=False, error=str(exc))
        return results

    def _fetch_json(self, source: str, url: str) -> dict[str, Any]:
        request = Request(url, headers={"Accept": "application/json"}, method="GET")
        try:
            with urlopen(request, timeout=self.timeout_sec) as response:
                if response.status != 200:
                    raise RuntimeError(f"{source} upstream returned status={response.status}")
                body = response.read()
        except HTTPError as exc:
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
            raise RuntimeError(f"{source} upstream invalid json: {exc}") from exc
