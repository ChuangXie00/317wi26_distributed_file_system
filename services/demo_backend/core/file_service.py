import base64
import hashlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


@dataclass
class DemoFileError(Exception):
    # 统一文件接口错误模型，用于 API 层稳定输出 error code 与详情。
    http_status: int
    code: str
    message: str
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


@dataclass
class UploadResult:
    # 上传结果：返回给前端用于展示文件名、总字节与分块数量。
    file_name: str
    total_bytes: int
    chunk_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "file_name": self.file_name,
            "total_bytes": self.total_bytes,
            "chunk_count": self.chunk_count,
        }


@dataclass
class DownloadResult:
    # 下载结果：内容以 base64 传回，前端再还原成 Blob。
    file_name: str
    total_bytes: int
    chunk_count: int
    content_base64: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "file_name": self.file_name,
            "total_bytes": self.total_bytes,
            "chunk_count": self.chunk_count,
            "content_base64": self.content_base64,
        }


@dataclass
class ReplicaMatrixResult:
    # 副本矩阵结果：用于 File Panel 展示 chunk 副本分布。
    file_name: str
    chunk_count: int
    rows: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "file_name": self.file_name,
            "chunk_count": self.chunk_count,
            "rows": self.rows,
        }


@dataclass
class FileListResult:
    files: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {"files": self.files}


@dataclass
class FileDeleteResult:
    file_name: str

    def to_dict(self) -> dict[str, Any]:
        return {"status": "ok", "file_name": self.file_name}


class DemoFileService:
    def __init__(self) -> None:
        # 统一走 meta-entry 入口，避免前端或 demo 后端直接绑定某个 leader 地址。
        self.meta_base_url = os.getenv("DEMO_META_ENTRY_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
        self.chunk_size = max(256 * 1024, int(os.getenv("DEMO_FILE_CHUNK_SIZE", str(1 * 1024 * 1024))))
        self.timeout_sec = float(os.getenv("DEMO_FILE_TIMEOUT_SEC", "20"))
        # 上传重试参数：缓解 demo_backend 重启后短时间连接抖动导致的 DEMO-FILE-002。
        self.upload_retry_count = max(1, int(os.getenv("DEMO_FILE_UPLOAD_RETRY", "3")))
        self.upload_retry_backoff_sec = max(0.0, float(os.getenv("DEMO_FILE_UPLOAD_RETRY_BACKOFF_SEC", "0.2")))
        self.storage_hosts = _parse_storage_hosts(
            os.getenv(
                "DEMO_STORAGE_HOSTS",
                "storage-01=http://127.0.0.1:9009,"
                "storage-02=http://127.0.0.1:9010,"
                "storage-03=http://127.0.0.1:9011",
            )
        )

    def upload_file(self, *, file_name: str, content: bytes) -> UploadResult:
        normalized_name = str(file_name or "").strip()
        if not normalized_name:
            raise DemoFileError(http_status=400, code="DEMO-FILE-001", message="file_name is required")

        chunks = list(_iter_chunks(content, self.chunk_size))
        if not chunks:
            # 空文件允许提交，保持最小可演示闭环。
            self._commit_file(file_name=normalized_name, chunk_fps=[])
            return UploadResult(file_name=normalized_name, total_bytes=0, chunk_count=0)

        chunk_fps: list[str] = []
        for chunk_bytes in chunks:
            fingerprint = _sha256_hex(chunk_bytes)
            chunk_fps.append(fingerprint)
            locations = self._resolve_chunk_locations(fingerprint)
            self._upload_chunk_to_locations(
                fingerprint=fingerprint,
                chunk_bytes=chunk_bytes,
                locations=locations,
            )

        self._commit_file(file_name=normalized_name, chunk_fps=chunk_fps)
        return UploadResult(
            file_name=normalized_name,
            total_bytes=len(content),
            chunk_count=len(chunk_fps),
        )

    def download_file(self, *, file_name: str) -> DownloadResult:
        normalized_name = str(file_name or "").strip()
        if not normalized_name:
            raise DemoFileError(http_status=400, code="DEMO-FILE-001", message="file_name is required")

        chunks = self._fetch_file_metadata(normalized_name)
        out = bytearray()
        for item in chunks:
            if not isinstance(item, dict):
                continue
            fingerprint = str(item.get("fingerprint", "")).strip()
            locations_raw = item.get("locations")
            locations = [str(node).strip() for node in locations_raw] if isinstance(locations_raw, list) else []
            if not fingerprint or not locations:
                raise DemoFileError(
                    http_status=500,
                    code="DEMO-FILE-003",
                    message=f"invalid chunk locations for {fingerprint or 'unknown'}",
                )
            out.extend(self._read_chunk_from_locations(fingerprint=fingerprint, locations=locations))

        content_bytes = bytes(out)
        return DownloadResult(
            file_name=normalized_name,
            total_bytes=len(content_bytes),
            chunk_count=len(chunks),
            content_base64=base64.b64encode(content_bytes).decode("ascii"),
        )

    def get_replica_matrix(self, *, file_name: str) -> ReplicaMatrixResult:
        normalized_name = str(file_name or "").strip()
        if not normalized_name:
            raise DemoFileError(http_status=400, code="DEMO-FILE-001", message="file_name is required")

        chunks = self._fetch_file_metadata(normalized_name)
        rows: list[dict[str, Any]] = []
        for index, item in enumerate(chunks):
            if not isinstance(item, dict):
                continue
            fingerprint = str(item.get("fingerprint", "")).strip()
            locations_raw = item.get("locations")
            locations = [str(node).strip() for node in locations_raw] if isinstance(locations_raw, list) else []
            locations = [node for node in locations if node]
            rows.append(
                {
                    "chunk_index": index,
                    "fingerprint": fingerprint,
                    "locations": locations,
                    "replica_count": len(locations),
                }
            )

        return ReplicaMatrixResult(
            file_name=normalized_name,
            chunk_count=len(rows),
            rows=rows,
        )

    def list_files(self, *, limit: int = 200) -> FileListResult:
        list_limit = max(1, min(int(limit), 1000))
        payload: dict[str, Any] = {}

        # 兼容不同版本上游：优先 /files，若 404 再尝试历史路径 /file/list。
        try:
            payload = self._request_json(
                method="GET",
                url=f"{self.meta_base_url}/files?limit={list_limit}",
                error_code="DEMO-FILE-003",
            )
        except DemoFileError as exc:
            if exc.http_status != 404:
                raise
            try:
                payload = self._request_json(
                    method="GET",
                    url=f"{self.meta_base_url}/file/list?limit={list_limit}",
                    error_code="DEMO-FILE-003",
                )
            except DemoFileError as fallback_exc:
                # 上游未实现文件列表接口时，前端按空列表降级展示，不视为错误。
                if fallback_exc.http_status == 404:
                    return FileListResult(files=[])
                raise

        items = payload.get("files")
        if not isinstance(items, list):
            raise DemoFileError(http_status=502, code="DEMO-FILE-003", message="invalid file list payload")

        files: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            files.append(
                {
                    "file_name": str(item.get("file_name", "")).strip(),
                    "chunk_count": int(item.get("chunk_count", 0) or 0),
                    "created_at": str(item.get("created_at", "")),
                    "updated_at": str(item.get("updated_at", "")),
                }
            )
        return FileListResult(files=files)

    def delete_file(self, *, file_name: str) -> FileDeleteResult:
        normalized_name = str(file_name or "").strip()
        if not normalized_name:
            raise DemoFileError(http_status=400, code="DEMO-FILE-001", message="file_name is required")

        try:
            self._request_json(
                method="DELETE",
                url=f"{self.meta_base_url}/file/{quote(normalized_name)}",
                error_code="DEMO-FILE-002",
            )
        except DemoFileError as exc:
            detail = str((exc.details or {}).get("upstream_detail", "")).lower()
            if "file not found" in detail:
                raise DemoFileError(http_status=404, code="DEMO-FILE-003", message="file not found") from exc
            raise
        return FileDeleteResult(file_name=normalized_name)

    def _resolve_chunk_locations(self, fingerprint: str) -> list[str]:
        check_payload = self._request_json(
            method="POST",
            url=f"{self.meta_base_url}/chunk/check",
            payload={"fingerprint": fingerprint},
            error_code="DEMO-FILE-002",
        )
        exists = bool(check_payload.get("exists", False))
        locations_raw = check_payload.get("locations")
        locations = [str(node).strip() for node in locations_raw] if isinstance(locations_raw, list) else []
        locations = [node for node in locations if node]

        if exists and locations:
            return locations

        register_payload = self._request_json(
            method="POST",
            url=f"{self.meta_base_url}/chunk/register",
            payload={"fingerprint": fingerprint},
            error_code="DEMO-FILE-002",
        )
        assigned = register_payload.get("assigned_nodes") or register_payload.get("assigned_node")
        assigned_nodes = [str(node).strip() for node in assigned] if isinstance(assigned, list) else []
        assigned_nodes = [node for node in assigned_nodes if node]
        if not assigned_nodes:
            raise DemoFileError(
                http_status=500,
                code="DEMO-FILE-002",
                message=f"chunk register returned empty locations: {fingerprint}",
            )
        return assigned_nodes

    def _upload_chunk_to_locations(self, *, fingerprint: str, chunk_bytes: bytes, locations: list[str]) -> None:
        errors: list[str] = []
        succeeded_nodes: list[str] = []
        for node_id in locations:
            storage_base = self.storage_hosts.get(node_id)
            if not storage_base:
                errors.append(f"{node_id}: unmapped storage host")
                continue

            upload_ok = False
            last_error = ""
            # 对每个副本节点做有限重试，降低瞬时 502/timeout 导致整文件失败的概率。
            for attempt in range(1, self.upload_retry_count + 1):
                request = Request(
                    f"{storage_base}/chunk/upload",
                    method="PUT",
                    data=chunk_bytes,
                    headers={
                        "fingerprint": fingerprint,
                        "Content-Type": "application/octet-stream",
                        "Accept": "application/json",
                    },
                )
                try:
                    with urlopen(request, timeout=self.timeout_sec) as response:
                        if response.status == 200:
                            upload_ok = True
                            succeeded_nodes.append(node_id)
                            break
                        last_error = f"status={response.status}"
                except (HTTPError, URLError, OSError) as exc:
                    last_error = str(exc)

                if attempt < self.upload_retry_count and self.upload_retry_backoff_sec > 0:
                    time.sleep(self.upload_retry_backoff_sec)

            if not upload_ok:
                errors.append(f"{node_id}: {last_error or 'upload failed'}")

        # 允许“部分副本成功”通过：避免单个节点瞬时故障导致整次上传失败。
        if not succeeded_nodes:
            raise DemoFileError(
                http_status=502,
                code="DEMO-FILE-002",
                message=f"chunk upload failed: {fingerprint}",
                details={"errors": errors, "locations": locations},
            )

    def _read_chunk_from_locations(self, *, fingerprint: str, locations: list[str]) -> bytes:
        errors: list[str] = []
        for node_id in locations:
            storage_base = self.storage_hosts.get(node_id)
            if not storage_base:
                errors.append(f"{node_id}: unmapped storage host")
                continue

            request = Request(
                f"{storage_base}/chunk/{quote(fingerprint)}",
                method="GET",
                headers={"Accept": "application/octet-stream"},
            )
            try:
                with urlopen(request, timeout=self.timeout_sec) as response:
                    if response.status != 200:
                        errors.append(f"{node_id}: status={response.status}")
                        continue
                    return response.read()
            except (HTTPError, URLError, OSError) as exc:
                errors.append(f"{node_id}: {exc}")

        raise DemoFileError(
            http_status=502,
            code="DEMO-FILE-003",
            message=f"all chunk replicas failed: {fingerprint}",
            details={"errors": errors},
        )

    def _commit_file(self, *, file_name: str, chunk_fps: list[str]) -> None:
        self._request_json(
            method="POST",
            url=f"{self.meta_base_url}/file/commit",
            payload={"file_name": file_name, "chunks": chunk_fps},
            error_code="DEMO-FILE-002",
        )

    def _fetch_file_metadata(self, file_name: str) -> list[dict[str, Any]]:
        # 统一读取 file 元数据，供下载与副本矩阵查询共用。
        meta_url = f"{self.meta_base_url}/file/{quote(file_name)}"
        metadata = self._request_json(method="GET", url=meta_url, error_code="DEMO-FILE-003")
        chunks = metadata.get("chunks")
        if not isinstance(chunks, list):
            raise DemoFileError(http_status=500, code="DEMO-FILE-003", message="invalid file metadata")
        return [item for item in chunks if isinstance(item, dict)]

    def _request_json(
        self,
        *,
        method: str,
        url: str,
        error_code: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = Request(url, method=method, data=body, headers=headers)
        try:
            with urlopen(request, timeout=self.timeout_sec) as response:
                raw = response.read()
                decoded = json.loads(raw.decode("utf-8")) if raw else {}
                if not isinstance(decoded, dict):
                    return {}
                return decoded
        except HTTPError as exc:
            if exc.code == 404 and error_code == "DEMO-FILE-003":
                raise DemoFileError(http_status=404, code="DEMO-FILE-003", message="file not found") from exc
            detail = _read_http_error_detail(exc)
            raise DemoFileError(
                http_status=502,
                code=error_code,
                message=f"upstream http error: status={exc.code}",
                details={"url": url, "upstream_detail": detail},
            ) from exc
        except (URLError, OSError, TimeoutError) as exc:
            raise DemoFileError(
                http_status=502,
                code=error_code,
                message="upstream unavailable",
                details={"url": url, "error": str(exc)},
            ) from exc
        except json.JSONDecodeError as exc:
            raise DemoFileError(
                http_status=502,
                code=error_code,
                message="upstream returned invalid json",
                details={"url": url},
            ) from exc


def _parse_storage_hosts(raw: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for item in str(raw or "").split(","):
        part = item.strip()
        if not part or "=" not in part:
            continue
        node_id, base_url = part.split("=", 1)
        node = node_id.strip()
        base = base_url.strip().rstrip("/")
        if node and base:
            mapping[node] = base
    return mapping


def _iter_chunks(data: bytes, chunk_size: int):
    if not data:
        return
    index = 0
    total = len(data)
    while index < total:
        yield data[index : index + chunk_size]
        index += chunk_size


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_http_error_detail(exc: HTTPError) -> str:
    try:
        raw = exc.read()
        if not raw:
            return ""
        return raw.decode("utf-8", errors="replace")[:600]
    except Exception:
        return ""
