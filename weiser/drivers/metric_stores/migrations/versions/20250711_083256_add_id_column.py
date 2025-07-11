"""
Migration: Add ID column
Version: 20250711_083256
"""

from sqlmodel import Session
from sqlalchemy import text
from weiser.drivers.metric_stores.migrations.base_migration import BaseMigration


class Migration_20250711_083256(BaseMigration):
    """
    Add ID column
    """

    def __init__(self):
        super().__init__(version="20250711_083256", description="Add ID column")

    def upgrade(self, session: Session) -> None:
        """Apply the migration."""
        # Check if sequence exists before creating
        try:
            self.execute_sql(session, "CREATE SEQUENCE id_sequence START 1")
        except Exception as e:
            if "already exists" in str(e):
                print("Sequence id_sequence already exists, skipping creation")
            else:
                raise e
        
        # Check if id column exists before adding
        try:
            result = session.exec(text("SELECT id FROM metrics LIMIT 1")).fetchone()
            print("Column id already exists, skipping column addition")
        except Exception:
            # Column doesn't exist, add it
            self.execute_sql(
                session,
                "ALTER TABLE metrics ADD COLUMN id INTEGER DEFAULT nextval('id_sequence')",
            )
            
            # Add primary key constraint
            try:
                self.execute_sql(
                    session,
                    "ALTER TABLE metrics ADD PRIMARY KEY (id)",
                )
            except Exception as e:
                if "already exists" in str(e) or "constraint" in str(e).lower():
                    print("Primary key constraint already exists or cannot be added")
                else:
                    raise e

    def downgrade(self, session: Session) -> None:
        """Revert the migration."""
        # This will fail since it's not possible to drop a primary key column directly
        self.execute_sql(
            session,
            "copy (SELECT actual_value, check_id, condition, dataset, datasource, fail, name, run_id, run_time, sql, success, threshold, threshold_list, type FROM metrics) TO '/tmp/metrics_tmp.csv'",
        )
        self.execute_sql(session, "drop table metrics")
        self.execute_sql(session, "create table metrics AS FROM '/tmp/metrics_tmp.csv'")
        pass
