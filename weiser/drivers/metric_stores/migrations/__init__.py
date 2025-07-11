"""
Custom migration system for DuckDB MetricStore.

Since DuckDB doesn't have native Alembic support, we implement a lightweight
migration system that tracks schema changes and applies them automatically.
"""

from .migration_runner import DuckDBMigrationRunner
from .base_migration import BaseMigration

__all__ = ["DuckDBMigrationRunner", "BaseMigration"]