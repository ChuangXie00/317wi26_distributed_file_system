from .connection import PostgresConnectionFactory


class PostgresSchemaManager:
    """数据库结构初始化管理器（DDL）。"""

    def __init__(self, connection_factory: PostgresConnectionFactory) -> None:
        self._connections = connection_factory

    def init_schema(self) -> None:
        # DDL 统一放到 schema 管理器，业务仓储里不再混建表语句。
        with self._connections.connection() as conn:
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
