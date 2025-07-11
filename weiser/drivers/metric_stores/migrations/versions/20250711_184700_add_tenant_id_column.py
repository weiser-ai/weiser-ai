"""
Migration: Add tenant_id column to metrics table
Version: 20250711_184700
"""

from sqlmodel import Session
from sqlalchemy import text
from weiser.drivers.metric_stores.migrations.base_migration import BaseMigration


class Migration_20250711_184700(BaseMigration):
    """
    Add tenant_id column to metrics table with default value of 1
    """

    def __init__(self):
        super().__init__(version="20250711_184700", description="Add tenant_id column to metrics table")

    def upgrade(self, session: Session) -> None:
        """Apply the migration - add tenant_id column."""
        # Check if tenant_id column exists before adding
        try:
            result = session.exec(text("SELECT tenant_id FROM metrics LIMIT 1")).fetchone()
            print("Column tenant_id already exists, skipping column addition")
        except Exception:
            # Column doesn't exist, add it
            self.execute_sql(
                session,
                "ALTER TABLE metrics ADD COLUMN tenant_id INTEGER DEFAULT 1",
            )
            
            # Update existing rows to have tenant_id = 1
            self.execute_sql(
                session,
                "UPDATE metrics SET tenant_id = 1 WHERE tenant_id IS NULL",
            )
            
            print("Added tenant_id column with default value 1")

    def downgrade(self, session: Session) -> None:
        """Revert the migration - remove tenant_id column."""
        # Check if column exists before dropping
        try:
            result = session.exec(text("SELECT tenant_id FROM metrics LIMIT 1")).fetchone()
            # Column exists, drop it
            self.execute_sql(session, "ALTER TABLE metrics DROP COLUMN tenant_id")
            print("Dropped tenant_id column")
        except Exception:
            print("Column tenant_id does not exist, skipping column removal")