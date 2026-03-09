# 0.1p04 兼容层：
# 历史导入路径仍可用，但真实实现已迁移到 services/meta/repository。
from repository.factory import PostgresMetadataRepository, get_repository, init_repository_schema

__all__ = ["PostgresMetadataRepository", "get_repository", "init_repository_schema"]
