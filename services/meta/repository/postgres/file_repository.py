import hashlib
from typing import List, Optional, Sequence

from ..infra.connection import PostgresConnectionFactory


class FileRepository:
    """管理 files / file_chunks 表的仓储。"""

    def __init__(self, connection_factory: PostgresConnectionFactory, default_namespace: str) -> None:
        self._connections = connection_factory
        self._default_namespace = str(default_namespace or "default").strip() or "default"

    def _normalize_fingerprint_sequence(self, fingerprints: Sequence[str]) -> List[str]:
        out: List[str] = []
        for fp in fingerprints:
            value = str(fp).strip().lower()
            if value:
                out.append(value)
        return out

    def _unique_preserve_order(self, values: Sequence[str]) -> List[str]:
        out: List[str] = []
        seen = set()
        for item in values:
            value = str(item).strip().lower()
            if value and value not in seen:
                seen.add(value)
                out.append(value)
        return out

    def _build_file_hash(self, chunk_fps: Sequence[str]) -> str:
        raw = "\n".join(chunk_fps).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def upsert_file(
        self,
        file_name: str,
        chunk_fingerprints: List[str],
        namespace: Optional[str] = None,
    ) -> None:
        ns = str(namespace or self._default_namespace).strip() or self._default_namespace
        fn = str(file_name).strip()
        if not fn:
            raise ValueError("file_name is required")

        seq = self._normalize_fingerprint_sequence(chunk_fingerprints)
        if not seq:
            raise ValueError("chunks cannot be empty")

        unique_fps = self._unique_preserve_order(seq)
        file_hash = self._build_file_hash(seq)

        with self._connections.connection() as conn:
            with conn.cursor() as cur:
                # 中文：同一个 namespace:file_name 提交强制串行，避免并发覆盖。
                lock_key = f"{ns}:{fn}"
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s));", (lock_key,))

                cur.execute(
                    "SELECT fingerprint, id FROM dfs_meta.chunks WHERE fingerprint = ANY(%s);",
                    (unique_fps,),
                )
                rows = cur.fetchall()
                chunk_id_map = {str(r["fingerprint"]).strip(): int(r["id"]) for r in rows}  # type: ignore[index]
                missing = [fp for fp in unique_fps if fp not in chunk_id_map]
                if missing:
                    raise ValueError(f"chunks not registered: {missing}")

                cur.execute(
                    """
                    INSERT INTO dfs_meta.files (namespace, file_name, file_hash, chunk_count)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (namespace, file_name)
                    DO UPDATE SET
                        file_hash = EXCLUDED.file_hash,
                        chunk_count = EXCLUDED.chunk_count,
                        updated_at = now()
                    RETURNING id;
                    """,
                    (ns, fn, file_hash, len(seq)),
                )
                file_row = cur.fetchone()
                if file_row is None:
                    raise RuntimeError("failed to upsert file metadata")
                file_id = int(file_row["id"])  # type: ignore[index]

                cur.execute("DELETE FROM dfs_meta.file_chunks WHERE file_id = %s;", (file_id,))
                for index, fp in enumerate(seq):
                    cur.execute(
                        """
                        INSERT INTO dfs_meta.file_chunks (file_id, chunk_index, chunk_id)
                        VALUES (%s, %s, %s);
                        """,
                        (file_id, index, chunk_id_map[fp]),
                    )

    def get_file_chunks(self, file_name: str, namespace: Optional[str] = None) -> Optional[List[str]]:
        ns = str(namespace or self._default_namespace).strip() or self._default_namespace
        fn = str(file_name).strip()
        if not fn:
            return None

        with self._connections.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM dfs_meta.files WHERE namespace = %s AND file_name = %s;",
                    (ns, fn),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                file_id = int(row["id"])  # type: ignore[index]

                cur.execute(
                    """
                    SELECT c.fingerprint
                    FROM dfs_meta.file_chunks fc
                    JOIN dfs_meta.chunks c ON c.id = fc.chunk_id
                    WHERE fc.file_id = %s
                    ORDER BY fc.chunk_index ASC;
                    """,
                    (file_id,),
                )
                rows = cur.fetchall()

        return [str(r["fingerprint"]).strip() for r in rows]  # type: ignore[index]
