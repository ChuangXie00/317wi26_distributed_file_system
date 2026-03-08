import hashlib
from typing import Any, Dict, List, Optional, Sequence

import psycopg
from psycopg.rows import dict_row

from .config import DEFAULT_NAMESPACE, build_pg_conn_kwargs

class PostgresMetadataRepository:
    # init Postgre with conn kwargs and default namespace
    def __init__(self, conn_kwargs: Dict[str, Any], default_namespace: str = DEFAULT_NAMESPACE) -> None:
        self._conn_kwargs = dict(conn_kwargs)
        self._default_namespace = str(default_namespace or "default").strip() or "default"

    # normalize and dedup replica node IDs while perserving order
    def _normalize_replicas(self, replicas: Sequence[str]) -> List[str]:
        out: List[str] = []
        seen = set()
        for node in replicas:
            value = str(node).strip()
            if value and value not in seen:
                seen.add(value)
                out.append(value)
        return out
    
    # normalize chunk fingerprint sequence, order but not dedup
    def _normalize_fingerprint_sequence(self, fingerprints: Sequence[str]) -> List[str]:
        out: List[str] = []
        for fp in fingerprints:
            value = str(fp).strip().lower()
            if value:
                out.append(value)
        return out
    
    # dedup while perserving order
    def _unique_preserve_order(self, values: Sequence[str]) -> List[str]:
        out: List[str] = []
        seen = set()
        for item in values:
            value = str(item).strip().lower()
            if value and value not in seen:
                seen.add(value)
                out.append(value)
        return out
    
    # build content hash from ordered chunk fingerprints(no version scenario)
    def _build_file_hash(self, chunk_fps: Sequence[str]) -> str:
        raw = "\n".join(chunk_fps).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()
    
    # open DB conn with transactional mode and dict-row mapping
    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(**self._conn_kwargs, row_factory=dict_row, autocommit=False) # type: ignore[arg-type]
    
    # init schema, tables and indexes with idempotent DDL
    def init_schema(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS dfs_meta;")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dfs_meta.files (
                        id BIGSERIAL PRIMARY KEY,
                        namespace TEXT NOT NULL DEFAULT 'default',
                        file_name TEXT NOT NULL,
                        file_hash CHAR(64) NOT NULL,
                        chunk_count INTEGER NOT NULL CHECK (chunk_count > 0),
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        UNIQUE(namespace, file_name)
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dfs_meta.chunks (
                        id BIGSERIAL PRIMARY KEY,
                        fingerprint CHAR(64) NOT NULL UNIQUE,
                        size_bytes BIGINT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        CHECK (fingerprint ~ '^[0-9a-f]{64}$')
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dfs_meta.file_chunks (
                        id BIGSERIAL PRIMARY KEY,
                        file_id BIGINT NOT NULL REFERENCES dfs_meta.files(id) ON DELETE CASCADE,
                        chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
                        chunk_id BIGINT NOT NULL REFERENCES dfs_meta.chunks(id) ON DELETE RESTRICT,
                        UNIQUE(file_id, chunk_index)
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dfs_meta.chunk_replicas (
                        id BIGSERIAL PRIMARY KEY,
                        chunk_id BIGINT NOT NULL REFERENCES dfs_meta.chunks(id) ON DELETE CASCADE,
                        node_id TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        UNIQUE(chunk_id, node_id)
                    );
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_files_namespace_hash ON dfs_meta.files(namespace, file_hash);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_file_chunks_chunk_id ON dfs_meta.file_chunks(chunk_id);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_chunk_replicas_node_id ON dfs_meta.chunk_replicas(node_id);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_chunk_replicas_chunk_id ON dfs_meta.chunk_replicas(chunk_id);")

    # ensure the chunk row exists for fingerprint and return chunk_id
    def _ensure_chunk_row(self, cur: psycopg.Cursor, fingerprint: str) -> int:
        cur.execute(
            """
            INSERT INTO dfs_meta.chunks (fingerprint)
            VALUES (%s)
            ON CONFLICT (fingerprint) DO NOTHING;
            """,
            (fingerprint,),
        )
        cur.execute("SELECT id FROM dfs_meta.chunks WHERE fingerprint = %s;", (fingerprint,))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"failed to resolve chunk_id for fingerprint={fingerprint}")
        return int(row["id"])   # type: ignore[index]
    
    # read replica node list for a chunk, sorted by node_id
    def get_chunk_replicas(self, fingerprint: str) -> List[str]:
        fp = str(fingerprint).strip().lower()
        if not fp:
            return []
        
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cr.node_id
                    FROM dfs_meta.chunk_replicas cr
                    JOIN dfs_meta.chunks c ON c.id = cr.chunk_id
                    WHERE c.fingerprint = %s
                    ORDER BY cr.node_id ASC;
                    """,
                    (fp,),
                )
                rows = cur.fetchall()

        return [str(r["node_id"]) for r in rows] # type: ignore[index]
    
    # Replace chunk replica set (ensure chunk exists, then rewrite replica relations)
    def upsert_chunk_replicas(self, fingerprint: str, replicas: List[str]) -> None:
        fp = str(fingerprint).strip().lower()
        if not fp:
            raise ValueError("fingerprint is required")

        normalized = self._normalize_replicas(replicas)

        with self._connect() as conn:
            with conn.cursor() as cur:
                chunk_id = self._ensure_chunk_row(cur, fp)
                cur.execute("DELETE FROM dfs_meta.chunk_replicas WHERE chunk_id = %s;", (chunk_id,))
                for node_id in normalized:
                    cur.execute(
                        """
                        INSERT INTO dfs_meta.chunk_replicas (chunk_id, node_id)
                        VALUES (%s, %s)
                        ON CONFLICT (chunk_id, node_id)
                        DO UPDATE SET updated_at = now();
                        """,
                        (chunk_id, node_id),
                    )
    
    # return missing fingerprints from input list, perserving input order
    def list_missing_chunks(self, fingerprints: List[str]) -> List[str]:
        normalized = self._normalize_fingerprint_sequence(fingerprints)
        unique_fps = self._unique_preserve_order(normalized)
        if not unique_fps:
            return []

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT fingerprint FROM dfs_meta.chunks WHERE fingerprint = ANY(%s);",
                    (unique_fps,),
                )
                rows = cur.fetchall()

        existing = {str(r["fingerprint"]) for r in rows} # type: ignore[index]
        return [fp for fp in unique_fps if fp not in existing]
    
    # commit file metadata by file_name (serialize same-file writes, overwrite mapping)
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

        with self._connect() as conn:
            with conn.cursor() as cur:
                # serialize same-file writes，avoid concurrent commit overwrite
                lock_key = f"{ns}:{fn}"
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s));", (lock_key,))

                cur.execute(
                    "SELECT fingerprint, id FROM dfs_meta.chunks WHERE fingerprint = ANY(%s);",
                    (unique_fps,),
                )
                rows = cur.fetchall()
                chunk_id_map = {str(r["fingerprint"]): int(r["id"]) for r in rows} # type: ignore[index]
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
                file_id = int(file_row["id"]) # type: ignore[index]

                cur.execute("DELETE FROM dfs_meta.file_chunks WHERE file_id = %s;", (file_id,))
                for index, fp in enumerate(seq):
                    cur.execute(
                        """
                        INSERT INTO dfs_meta.file_chunks (file_id, chunk_index, chunk_id)
                        VALUES (%s, %s, %s);
                        """,
                        (file_id, index, chunk_id_map[fp]),
                    )

    # read ordered chunk list by file_name(return None if file doesn't exist)
    def get_file_chunks(self, file_name: str, namespace: Optional[str] = None) -> Optional[List[str]]:
        ns = str(namespace or self._default_namespace).strip() or self._default_namespace
        fn = str(file_name).strip()
        if not fn:
            return None

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM dfs_meta.files WHERE namespace = %s AND file_name = %s;",
                    (ns, fn),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                file_id = int(row["id"]) # type: ignore[index]

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

        return [str(r["fingerprint"]) for r in rows] # type: ignore[index]
    
    # return repository health status and key table counters for debug endpoint
    def db_health(self) -> Dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(1) AS c FROM dfs_meta.files;")
                row = cur.fetchone()
                files_count = int(row["c"]) if row is not None else 0  # type: ignore[index]

                cur.execute("SELECT COUNT(1) AS c FROM dfs_meta.chunks;")
                row = cur.fetchone()
                chunks_count = int(row["c"]) if row is not None else 0  # type: ignore[index]

                cur.execute("SELECT COUNT(1) AS c FROM dfs_meta.file_chunks;")
                row = cur.fetchone()
                file_chunks_count = int(row["c"]) if row is not None else 0  # type: ignore[index]

                cur.execute("SELECT COUNT(1) AS c FROM dfs_meta.chunk_replicas;")
                row = cur.fetchone()
                replicas_count = int(row["c"]) if row is not None else 0  # type: ignore[index]

        return {
            "engine": "postgres",
            "ok": True,
            "tables": {
                "files": files_count,
                "chunks": chunks_count,
                "file_chunks": file_chunks_count,
                "chunk_replicas": replicas_count,
            },
        }
    

_REPOSITORY: Optional[PostgresMetadataRepository] = None

# get repo singleton (avoid deplicated repository instances)
def get_repository() -> PostgresMetadataRepository:
    global _REPOSITORY
    if _REPOSITORY is None:
        _REPOSITORY = PostgresMetadataRepository(build_pg_conn_kwargs(), DEFAULT_NAMESPACE)
    return _REPOSITORY

# init repo schema(called during app startup)
def init_repository_schema() -> None:
    get_repository().init_schema()