from typing import Any, Dict, List, Optional

from core.config import DEFAULT_NAMESPACE, build_pg_conn_kwargs

from .infra.connection import PostgresConnectionFactory
from .infra.schema import PostgresSchemaManager
from .postgres.chunk_repository import ChunkRepository
from .postgres.file_repository import FileRepository


class PostgresMetadataRepository:
    """对上层暴露统一仓储接口，内部组合多个子仓储。"""

    def __init__(self, conn_kwargs: Dict[str, Any], default_namespace: str = DEFAULT_NAMESPACE) -> None:
        self._connections = PostgresConnectionFactory(conn_kwargs)
        self._schema = PostgresSchemaManager(self._connections)
        self._chunk_repo = ChunkRepository(self._connections)
        self._file_repo = FileRepository(self._connections, default_namespace)

    def init_schema(self) -> None:
        self._schema.init_schema()

    def get_chunk_replicas(self, fingerprint: str) -> List[str]:
        return self._chunk_repo.get_chunk_replicas(fingerprint)

    def upsert_chunk_replicas(self, fingerprint: str, replicas: List[str]) -> None:
        self._chunk_repo.upsert_chunk_replicas(fingerprint, replicas)

    def list_missing_chunks(self, fingerprints: List[str]) -> List[str]:
        return self._chunk_repo.list_missing_chunks(fingerprints)

    def upsert_file(self, file_name: str, chunk_fingerprints: List[str], namespace: Optional[str] = None) -> None:
        self._file_repo.upsert_file(file_name, chunk_fingerprints, namespace)

    def get_file_chunks(self, file_name: str, namespace: Optional[str] = None) -> Optional[List[str]]:
        return self._file_repo.get_file_chunks(file_name, namespace)

    def db_health(self) -> Dict[str, Any]:
        with self._connections.connection() as conn:
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


def get_repository() -> PostgresMetadataRepository:
    # 中文：仓储对象单例；但每次方法调用仍会创建新连接和新事务。
    global _REPOSITORY
    if _REPOSITORY is None:
        _REPOSITORY = PostgresMetadataRepository(build_pg_conn_kwargs(), DEFAULT_NAMESPACE)
    return _REPOSITORY


def init_repository_schema() -> None:
    get_repository().init_schema()
