from typing import Any, List, Tuple, Optional
from sqlmodel import SQLModel, Session, create_engine, select
from sqlalchemy.engine import URL
from sqlglot.expressions import Select
from sqlglot.dialects import Postgres
from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.operations import Operations
import os

from weiser.loader.models import MetricStore
from weiser.drivers.metric_stores.models import MetricRecord


class PostgresMetricStore:
    def __init__(self, metric_store: MetricStore, verbose: bool = False) -> None:
        self.verbose = verbose
        self.config = metric_store
        if not metric_store.uri:
            uri = URL.create(
                metric_store.db_type,
                username=metric_store.user,
                password=metric_store.password.get_secret_value(),
                host=metric_store.host,
                database=metric_store.db_name,
            )
        else:
            uri = metric_store.uri

        self.engine = create_engine(uri)
        self.db_name = metric_store.db_name
        self.dialect = Postgres

        # Initialize database schema
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize the database schema using Alembic if available, otherwise create directly."""
        try:
            # Try to use Alembic for schema management
            self._run_alembic_migrations()
        except Exception:
            # Fallback to direct table creation
            SQLModel.metadata.create_all(self.engine)

    def _run_alembic_migrations(self) -> None:
        """Run Alembic migrations to ensure schema is up to date."""
        alembic_cfg = Config("alembic.ini")

        # Set the database URL
        alembic_cfg.set_main_option("sqlalchemy.url", str(self.engine.url))

        # Check if we need to initialize Alembic
        with self.engine.connect() as connection:
            context = MigrationContext.configure(connection)
            current_rev = context.get_current_revision()

            if current_rev is None:
                # Initialize Alembic version table and stamp with current revision
                command.stamp(alembic_cfg, "head")
            else:
                # Run any pending migrations
                command.upgrade(alembic_cfg, "head")

    def execute_query(
        self,
        q: Select,
        check: Any,
        verbose: bool = False,
        validate_results: bool = True,
    ) -> List[Tuple]:
        """Execute a SQLGlot query for metadata queries like anomaly detection."""
        with Session(self.engine) as session:
            result = session.exec(q.sql(dialect=self.dialect))
            rows = list(result)

            if validate_results and (not rows or not rows[0] or rows[0][0] is None):
                raise Exception(
                    f"Unexpected result executing check: {check.model_dump()}"
                )

            if verbose:
                print(f"Query result: {rows}")

            return rows

    def insert_results(self, record: dict) -> None:
        """Insert a metric record into the database."""
        # Handle threshold list conversion
        if isinstance(record.get("threshold"), (List, Tuple)):
            threshold_list = record["threshold"]
            threshold = None
        else:
            threshold_list = record.get("threshold_list")
            threshold = record.get("threshold")

        # Create MetricRecord instance
        metric_record = MetricRecord(
            actual_value=record.get("actual_value"),
            check_id=record.get("check_id"),
            condition=record.get("condition"),
            dataset=record.get("dataset"),
            datasource=record.get("datasource"),
            fail=record.get("fail"),
            name=record.get("name"),
            run_id=record.get("run_id"),
            run_time=record.get("run_time"),
            sql=record.get("measure"),  # Note: original uses 'measure' key for SQL
            success=record.get("success"),
            threshold=threshold,
            threshold_list=threshold_list,
            type=record.get("type"),
            tenant_id=record.get("tenant_id", self.config.tenant_id),
        )

        with Session(self.engine) as session:
            session.add(metric_record)
            session.commit()

    def export_results(self, run_id: str) -> None:
        """Export results for a specific run_id (placeholder for future implementation)."""
        pass

    def get_metrics_for_check(
        self, check_id: str, limit: Optional[int] = None
    ) -> List[MetricRecord]:
        """Get metrics for a specific check_id."""
        with Session(self.engine) as session:
            statement = select(MetricRecord).where(MetricRecord.check_id == check_id)

            if limit:
                statement = statement.limit(limit)

            result = session.exec(statement)
            return list(result)

    def get_metrics_for_run(self, run_id: str) -> List[MetricRecord]:
        """Get all metrics for a specific run_id."""
        with Session(self.engine) as session:
            statement = select(MetricRecord).where(MetricRecord.run_id == run_id)
            result = session.exec(statement)
            return list(result)

    def delete_metrics_for_run(self, run_id: str) -> int:
        """Delete all metrics for a specific run_id. Returns number of deleted records."""
        with Session(self.engine) as session:
            statement = select(MetricRecord).where(MetricRecord.run_id == run_id)
            records = session.exec(statement).all()

            for record in records:
                session.delete(record)

            session.commit()
            return len(records)
