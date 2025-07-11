"""
DuckDB Migration Runner - manages and executes migrations.
"""

import os
import importlib.util
from typing import List, Optional
from datetime import datetime
from sqlalchemy import text, Engine
from sqlmodel import Session, Field, SQLModel
from rich import print

from .base_migration import BaseMigration


class MigrationRecord(SQLModel, table=True):
    """
    Tracks applied migrations in the database.
    """

    __tablename__ = "duckdb_migrations"

    id: Optional[int] = Field(
        default=None, primary_key=True, sa_column_kwargs={"autoincrement": False}
    )
    version: str = Field(index=True)
    description: str
    applied_at: datetime

    @classmethod
    def from_migration(cls, migration: BaseMigration) -> "MigrationRecord":
        """Create a MigrationRecord from a migration."""
        import hashlib

        # Generate a simple hash-based ID
        id_string = f"{migration.version}{migration.description}"
        record_id = abs(hash(id_string)) % (2**31)

        return cls(
            id=record_id,
            version=migration.version,
            description=migration.description,
            applied_at=datetime.now(),
        )


class DuckDBMigrationRunner:
    """
    Manages and executes DuckDB migrations.

    Features:
    - Automatic migration discovery
    - Migration tracking in database
    - Forward and backward migration support
    - Safe migration execution with rollback
    """

    def __init__(self, engine: Engine, migrations_dir: str = None):
        self.engine = engine
        self.migrations_dir = migrations_dir or os.path.join(
            os.path.dirname(__file__), "versions"
        )
        self._ensure_migrations_table()

    def _ensure_migrations_table(self) -> None:
        """Create the migrations tracking table if it doesn't exist."""
        try:
            # Try to create the table using SQLModel
            MigrationRecord.__table__.create(self.engine, checkfirst=True)
        except Exception:
            # Fallback to direct SQL creation
            with Session(self.engine) as session:
                session.exec(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS duckdb_migrations (
                        id INTEGER PRIMARY KEY,
                        version VARCHAR NOT NULL,
                        description VARCHAR NOT NULL,
                        applied_at TIMESTAMP NOT NULL
                    )
                """
                    )
                )
                session.commit()

    def discover_migrations(self) -> List[BaseMigration]:
        """
        Discover all migration files in the migrations directory.

        Returns:
            List of migration instances sorted by version
        """
        migrations = []

        if not os.path.exists(self.migrations_dir):
            print(f"Migrations directory not found: {self.migrations_dir}")
            return migrations

        for filename in os.listdir(self.migrations_dir):
            if filename.endswith(".py") and not filename.startswith("__"):
                migration_path = os.path.join(self.migrations_dir, filename)
                migration = self._load_migration_from_file(migration_path)
                if migration:
                    migrations.append(migration)

        # Sort by version
        migrations.sort(key=lambda m: m.version)
        return migrations

    def _load_migration_from_file(self, filepath: str) -> Optional[BaseMigration]:
        """Load a migration from a Python file."""
        try:
            spec = importlib.util.spec_from_file_location("migration", filepath)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Look for a migration class that inherits from BaseMigration
            for item_name in dir(module):
                item = getattr(module, item_name)
                if (
                    isinstance(item, type)
                    and issubclass(item, BaseMigration)
                    and item is not BaseMigration
                ):
                    return item()

            print(f"Warning: No migration class found in {filepath}")
            return None

        except Exception as e:
            print(f"Error loading migration from {filepath}: {e}")
            return None

    def get_applied_migrations(self) -> List[str]:
        """Get list of applied migration versions."""
        with Session(self.engine) as session:
            try:
                result = session.exec(
                    text("SELECT version FROM duckdb_migrations ORDER BY applied_at")
                )
                return [row[0] for row in result]
            except Exception:
                # Table might not exist yet
                return []

    def get_pending_migrations(self) -> List[BaseMigration]:
        """Get list of migrations that haven't been applied yet."""
        all_migrations = self.discover_migrations()
        applied_versions = set(self.get_applied_migrations())

        return [m for m in all_migrations if m.version not in applied_versions]

    def apply_migration(self, migration: BaseMigration) -> bool:
        """
        Apply a single migration.

        Args:
            migration: Migration to apply

        Returns:
            True if successful, False otherwise
        """
        try:
            with Session(self.engine) as session:
                print(f"Applying migration: {migration}")

                # Apply the migration
                migration.upgrade(session)

                # Record the migration
                migration_record = MigrationRecord.from_migration(migration)
                session.add(migration_record)
                session.commit()

                print(f"✓ Migration {migration.version} applied successfully")
                return True

        except Exception as e:
            print(f"✗ Error applying migration {migration.version}: {e}")
            return False

    def rollback_migration(self, migration: BaseMigration) -> bool:
        """
        Rollback a single migration.

        Args:
            migration: Migration to rollback

        Returns:
            True if successful, False otherwise
        """
        try:
            with Session(self.engine) as session:
                print(f"Rolling back migration: {migration}")

                # Rollback the migration
                migration.downgrade(session)

                # Remove the migration record
                session.exec(
                    text(
                        f"DELETE FROM duckdb_migrations WHERE version = '{migration.version}'"
                    )
                )
                session.commit()

                print(f"✓ Migration {migration.version} rolled back successfully")
                return True

        except Exception as e:
            print(f"✗ Error rolling back migration {migration.version}: {e}")
            return False

    def migrate_up(self, target_version: str = None) -> bool:
        """
        Apply all pending migrations up to target version.

        Args:
            target_version: Stop at this version (None = apply all)

        Returns:
            True if all migrations applied successfully
        """
        pending_migrations = self.get_pending_migrations()

        if not pending_migrations:
            print("No pending migrations to apply")
            return True

        success = True
        for migration in pending_migrations:
            if target_version and migration.version > target_version:
                break

            if not self.apply_migration(migration):
                success = False
                break

        return success

    def migrate_down(self, target_version: str) -> bool:
        """
        Rollback migrations down to target version.

        Args:
            target_version: Rollback to this version

        Returns:
            True if rollback successful
        """
        all_migrations = self.discover_migrations()
        applied_versions = self.get_applied_migrations()

        # Find migrations to rollback (in reverse order)
        migrations_to_rollback = []
        for migration in reversed(all_migrations):
            if (
                migration.version in applied_versions
                and migration.version > target_version
            ):
                migrations_to_rollback.append(migration)

        if not migrations_to_rollback:
            print(f"No migrations to rollback to version {target_version}")
            return True

        success = True
        for migration in migrations_to_rollback:
            if not self.rollback_migration(migration):
                success = False
                break

        return success

    def status(self) -> None:
        """Print migration status."""
        all_migrations = self.discover_migrations()
        applied_versions = set(self.get_applied_migrations())

        print("Migration Status:")
        print("================")

        if not all_migrations:
            print("No migrations found")
            return

        for migration in all_migrations:
            status = (
                "✓ Applied" if migration.version in applied_versions else "✗ Pending"
            )
            print(f"{status} - {migration.version}: {migration.description}")

        pending_count = len(
            [m for m in all_migrations if m.version not in applied_versions]
        )
        print(f"\nTotal migrations: {len(all_migrations)}")
        print(f"Applied: {len(applied_versions)}")
        print(f"Pending: {pending_count}")

    def create_migration_file(self, description: str) -> str:
        """
        Create a new migration file template.

        Args:
            description: Description of the migration

        Returns:
            Path to the created migration file
        """
        # Generate version based on timestamp
        version = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{version}_{description.lower().replace(' ', '_')}.py"
        filepath = os.path.join(self.migrations_dir, filename)

        # Ensure migrations directory exists
        os.makedirs(self.migrations_dir, exist_ok=True)

        # Create migration file from template
        template = f'''"""
Migration: {description}
Version: {version}
"""

from sqlmodel import Session
from weiser.drivers.metric_stores.migrations.base_migration import BaseMigration


class Migration_{version}(BaseMigration):
    """
    {description}
    """
    
    def __init__(self):
        super().__init__(version="{version}", description="{description}")
    
    def upgrade(self, session: Session) -> None:
        """Apply the migration."""
        # TODO: Implement your migration logic here
        # Example:
        # self.execute_sql(session, "ALTER TABLE metrics ADD COLUMN new_column VARCHAR")
        pass
    
    def downgrade(self, session: Session) -> None:
        """Revert the migration."""
        # TODO: Implement your rollback logic here
        # Example:
        # self.execute_sql(session, "ALTER TABLE metrics DROP COLUMN new_column")
        pass
'''

        with open(filepath, "w") as f:
            f.write(template)

        print(f"Created migration file: {filepath}")
        return filepath
