"""
Migration: Add ID column
Version: 20250711_083256
"""

from sqlmodel import Session
from weiser.drivers.metric_stores.migrations.base_migration import BaseMigration


class Migration_20250711_083256(BaseMigration):
    """
    Add ID column
    """

    def __init__(self):
        super().__init__(version="20250711_083256", description="Add ID column")

    def upgrade(self, session: Session) -> None:
        """Apply the migration."""
        self.execute_sql(session, "CREATE SEQUENCE id_sequence START 1")
        self.execute_sql(
            session,
            "ALTER TABLE metrics ADD COLUMN id INTEGER DEFAULT nextval('id_sequence')",
        )
        self.execute_sql(
            session,
            "ALTER TABLE metrics ADD PRIMARY KEY (id)",
        )
        pass

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
