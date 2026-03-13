from typing import Any, Dict, List, Sequence

import psycopg

from ..infra.connection import PostgresConnectionFactory


class ChunkRepository:
    """管理 chunks / chunk_replicas 表的仓储。"""

    def __init__(self, connection_factory: PostgresConnectionFactory) -> None:
        self._connections = connection_factory

    def _normalize_replicas(self, replicas: Sequence[str]) -> List[str]:
        out: List[str] = []
        seen = set()
        for node in replicas:
            value = str(node).strip()
            if value and value not in seen:
                seen.add(value)
                out.append(value)
        return out

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

    def _ensure_chunk_row(self, cur: psycopg.Cursor[Any], fingerprint: str) -> int:
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
        return int(row["id"])  # type: ignore[index]

    def get_chunk_replicas(self, fingerprint: str) -> List[str]:
        fp = str(fingerprint).strip().lower()
        if not fp:
            return []

        with self._connections.connection() as conn:
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

        return [str(r["node_id"]).strip() for r in rows]  # type: ignore[index]

    def upsert_chunk_replicas(self, fingerprint: str, replicas: List[str]) -> None:
        fp = str(fingerprint).strip().lower()
        if not fp:
            raise ValueError("fingerprint is required")

        normalized = self._normalize_replicas(replicas)

        with self._connections.connection() as conn:
            with conn.cursor() as cur:
                chunk_id = self._ensure_chunk_row(cur, fp)
                # 直接重写副本集合，保证最终副本列表与当前策略一致。
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

    def list_missing_chunks(self, fingerprints: List[str]) -> List[str]:
        normalized = self._normalize_fingerprint_sequence(fingerprints)
        unique_fps = self._unique_preserve_order(normalized)
        if not unique_fps:
            return []

        with self._connections.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT fingerprint FROM dfs_meta.chunks WHERE fingerprint = ANY(%s);",
                    (unique_fps,),
                )
                rows = cur.fetchall()

        existing = {str(r["fingerprint"]).strip() for r in rows}  # type: ignore[index]
        return [fp for fp in unique_fps if fp not in existing]

    def get_replica_counts_by_node(self, node_ids: Sequence[str] | None = None) -> Dict[str, int]:
        normalized_nodes: List[str] = []
        if node_ids is not None:
            normalized_nodes = self._normalize_replicas(node_ids)
            if not normalized_nodes:
                return {}

        with self._connections.connection() as conn:
            with conn.cursor() as cur:
                if normalized_nodes:
                    cur.execute(
                        """
                        SELECT cr.node_id, COUNT(1) AS replica_chunks
                        FROM dfs_meta.chunk_replicas cr
                        WHERE cr.node_id = ANY(%s)
                        GROUP BY cr.node_id
                        ORDER BY cr.node_id ASC;
                        """,
                        (normalized_nodes,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT cr.node_id, COUNT(1) AS replica_chunks
                        FROM dfs_meta.chunk_replicas cr
                        GROUP BY cr.node_id
                        ORDER BY cr.node_id ASC;
                        """
                    )
                rows = cur.fetchall()

        out: Dict[str, int] = {}
        for row in rows:
            node_id = str(row["node_id"]).strip()  # type: ignore[index]
            out[node_id] = int(row["replica_chunks"])  # type: ignore[index]
        return out
