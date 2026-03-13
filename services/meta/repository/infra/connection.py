from contextlib import contextmanager
from typing import Any, Dict, Iterator

import psycopg
from psycopg.rows import dict_row


class PostgresConnectionFactory:
    """PostgreSQL 连接工厂（0.1p04）。"""

    def __init__(self, conn_kwargs: Dict[str, Any]) -> None:
        # 统一保存连接参数，避免在多个文件里散落配置拼装逻辑。
        self._conn_kwargs = dict(conn_kwargs)

    def connect(self) -> psycopg.Connection[Any]:
        # 每次调用创建新连接，新请求使用新事务，避免跨请求共享事务状态。
        return psycopg.connect(
            **self._conn_kwargs,
            row_factory=dict_row, # type:ignore
            autocommit=False,
        )  # type: ignore[arg-type]

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection[Any]]:
        # with 块退出时，psycopg 会自动提交/回滚并关闭连接。
        with self.connect() as conn:
            yield conn
