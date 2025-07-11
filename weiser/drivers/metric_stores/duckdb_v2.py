"""
DuckDB V2 MetricStore - SQLModel-based implementation

This version uses SQLModel for type safety and schema definition,
but skips Alembic migrations due to DuckDB's lack of native support.
"""

import os
import boto3
from typing import Any, List, Tuple, Optional
from sqlmodel import SQLModel, Session, create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlglot.expressions import Select
from sqlglot.dialects import DuckDB
from rich import print

from weiser.loader.models import MetricStore, S3UrlStyle
from weiser.drivers.metric_stores.models_duckdb import MetricRecordDuckDB
from weiser.drivers.metric_stores.migrations import DuckDBMigrationRunner


class DuckDBMetricStoreV2:
    """
    DuckDB V2 MetricStore with SQLModel integration.

    Features:
    - SQLModel-based schema definition
    - Type-safe database operations
    - Maintains S3 integration from V1
    - Direct DDL management (no Alembic)
    """

    def __init__(self, config: MetricStore) -> None:
        self.config = config
        self.s3_client = None
        self.dialect = DuckDB

        # Database setup
        self.db_name = config.db_name or "./metricstore.db"

        # Create SQLAlchemy engine with duckdb-engine
        self.engine = create_engine(f"duckdb:///{self.db_name}")

        # Initialize S3 client if configured
        if self._has_s3_config():
            self.s3_client = boto3.client(
                "s3",
                region_name=self.config.s3_region,
                aws_access_key_id=self.config.s3_access_key,
                aws_secret_access_key=self.config.s3_secret_access_key,
            )

        # Initialize database schema and S3 configuration
        self._init_database()

        # Initialize migration runner after database is set up
        self.migration_runner = DuckDBMigrationRunner(self.engine)

        # Run any pending migrations
        self._run_migrations()

    def _has_s3_config(self) -> bool:
        """Check if S3 configuration is properly provided."""
        return (
            self.config.s3_bucket is not None
            and self.config.s3_access_key is not None
            and self.config.s3_secret_access_key is not None
        )

    def _init_database(self) -> None:
        """Initialize database schema and S3 configuration."""
        with Session(self.engine) as session:
            # Configure S3 if available
            if self._has_s3_config():
                try:
                    session.exec(text("INSTALL httpfs"))
                    session.exec(text("LOAD httpfs"))

                    # Configure S3 settings
                    if self.config.s3_url_style == S3UrlStyle.path:
                        session.exec(
                            text(f"SET s3_url_style='{self.config.s3_url_style}'")
                        )
                    elif self.config.s3_url_style == S3UrlStyle.vhost:
                        session.exec(text(f"SET s3_region = '{self.config.s3_region}'"))

                    if self.config.s3_endpoint:
                        session.exec(
                            text(f"SET s3_endpoint = '{self.config.s3_endpoint}'")
                        )

                    session.exec(
                        text(f"SET s3_access_key_id = '{self.config.s3_access_key}'")
                    )
                    session.exec(
                        text(
                            f"SET s3_secret_access_key = '{self.config.s3_secret_access_key}'"
                        )
                    )

                except Exception as e:
                    print(f"Warning: S3 configuration failed: {e}")

        # Create tables using SQLModel
        # Note: We need to adjust the MetricRecord model for DuckDB compatibility
        self._create_tables()

        # Import existing S3 data if available
        if self._has_s3_config():
            self._import_s3_data()

    def _create_tables(self) -> None:
        """Create database tables using SQLModel."""
        try:
            # Create tables - only create the DuckDB-specific tables
            MetricRecordDuckDB.__table__.create(self.engine, checkfirst=True)
        except Exception as e:
            print(f"Warning: Table creation failed: {e}")
            # Fallback to direct SQL creation if needed
            self._create_tables_direct()

    def _create_tables_direct(self) -> None:
        """Direct SQL table creation as fallback."""
        with Session(self.engine) as session:
            session.exec(
                text(
                    """
                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY,
                    actual_value DOUBLE,
                    check_id VARCHAR,
                    condition VARCHAR,
                    dataset VARCHAR,
                    datasource VARCHAR,
                    fail BOOLEAN,
                    name VARCHAR,
                    run_id VARCHAR,
                    run_time TIMESTAMP,
                    sql VARCHAR,
                    success BOOLEAN,
                    threshold VARCHAR,
                    threshold_list VARCHAR,
                    type VARCHAR
                )
            """
                )
            )

    def _import_s3_data(self) -> None:
        """Import existing data from S3."""
        try:
            with Session(self.engine) as session:
                # Get the latest run time to avoid duplicates
                result = session.exec(text("SELECT MAX(run_time) FROM metrics")).first()
                last_run_time = "1=1" if not result else f"run_time > '{result}'"

                # Import from S3
                s3_path = f"s3://{self.config.s3_bucket}/metrics/*.parquet"
                session.exec(
                    text(
                        f"""
                    INSERT INTO metrics 
                    SELECT * FROM '{s3_path}' 
                    WHERE {last_run_time}
                """
                    )
                )
                print("S3 data import completed successfully")
        except Exception:
            print(
                "Warning: S3 data import failed. This may be expected if no data exists yet."
            )
            # Ignore errors when S3 data doesn't exist yet
            pass

    def _run_migrations(self) -> None:
        """Run any pending migrations."""
        try:
            pending_migrations = self.migration_runner.get_pending_migrations()
            if pending_migrations:
                print(f"Running {len(pending_migrations)} pending migrations...")
                self.migration_runner.migrate_up()
            else:
                print("No pending migrations to run")
        except Exception as e:
            print(f"Warning: Migration check failed: {e}")

    def migrate_up(self, target_version: str = None) -> bool:
        """
        Apply pending migrations.

        Args:
            target_version: Stop at this version (None = apply all)

        Returns:
            True if successful
        """
        return self.migration_runner.migrate_up(target_version)

    def migrate_down(self, target_version: str) -> bool:
        """
        Rollback migrations to target version.

        Args:
            target_version: Rollback to this version

        Returns:
            True if successful
        """
        return self.migration_runner.migrate_down(target_version)

    def migration_status(self) -> None:
        """Print migration status."""
        self.migration_runner.status()

    def create_migration(self, description: str) -> str:
        """
        Create a new migration file.

        Args:
            description: Description of the migration

        Returns:
            Path to the created migration file
        """
        return self.migration_runner.create_migration_file(description)

    def execute_query(
        self,
        q: Select,
        check: Any,
        verbose: bool = False,
        validate_results: bool = True,
    ) -> List[Tuple]:
        """Execute a SQLGlot query for metadata queries like anomaly detection."""
        with Session(self.engine) as session:
            sql = q.sql(dialect=self.dialect)
            result = session.exec(text(sql))
            rows = list(result)

            if validate_results and (not rows or rows[0] is None):
                if verbose:
                    print(f"Query: {sql}")
                raise Exception(
                    f"Unexpected result executing check: {check.model_dump()}"
                )

            if verbose:
                print(f"Query result: {rows}")

            return rows

    def insert_results(self, record: dict) -> None:
        """Insert a metric record into the database."""
        # Create MetricRecordDuckDB instance (handles threshold conversion internally)
        metric_record = MetricRecordDuckDB.from_dict(record)

        with Session(self.engine) as session:
            session.add(metric_record)
            session.commit()

    def export_results(self, run_id: str) -> dict:
        """Export results for a specific run_id."""
        results = {"summary": {}, "failures": []}

        with Session(self.engine) as session:
            # Get summary statistics
            summary_stmt = f"""
                SELECT 
                    COUNT(*) as total_checks,
                    SUM(CASE WHEN success THEN 1 ELSE 0 END) as passed_checks,
                    SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) as failed_checks
                FROM metrics 
                WHERE run_id = '{run_id}'
            """
            summary_result = session.exec(text(summary_stmt)).first()

            if summary_result:
                results["summary"] = {
                    "total_checks": summary_result[0],
                    "passed_checks": summary_result[1],
                    "failed_checks": summary_result[2],
                }

            # Get detailed failure information
            failures_stmt = f"""
                SELECT 
                    name, dataset, datasource, check_id, condition,
                    actual_value, threshold, type
                FROM metrics 
                WHERE run_id = '{run_id}' AND NOT success 
                LIMIT 20
            """
            failure_results = session.exec(text(failures_stmt)).all()

            columns = [
                "name",
                "dataset",
                "datasource",
                "check_id",
                "condition",
                "actual_value",
                "threshold",
                "type",
            ]

            for row in failure_results:
                failure_dict = {col: val for col, val in zip(columns, row)}
                results["failures"].append(failure_dict)

        # Handle S3 export if configured
        if self._has_s3_config():
            self._export_to_s3(run_id)
        else:
            print("No S3 bucket configured, skipping export to S3.")

        return results

    def _export_to_s3(self, run_id: str) -> None:
        """Export data to S3."""
        try:
            with Session(self.engine) as session:
                # Configure S3 for export
                session.exec(text("INSTALL httpfs"))
                session.exec(text("LOAD httpfs"))

                if self.config.s3_url_style == S3UrlStyle.path:
                    session.exec(text(f"SET s3_url_style='{self.config.s3_url_style}'"))
                elif self.config.s3_url_style == S3UrlStyle.vhost:
                    session.exec(text(f"SET s3_region = '{self.config.s3_region}'"))

                if self.config.s3_endpoint:
                    session.exec(text(f"SET s3_endpoint = '{self.config.s3_endpoint}'"))

                session.exec(
                    text(f"SET s3_access_key_id = '{self.config.s3_access_key}'")
                )
                session.exec(
                    text(
                        f"SET s3_secret_access_key = '{self.config.s3_secret_access_key}'"
                    )
                )

                # Export current run data
                s3_path = f"s3://{self.config.s3_bucket}/metrics/{run_id}.parquet"
                session.exec(
                    text(
                        f"""
                    COPY (SELECT * FROM metrics WHERE run_id='{run_id}') 
                    TO '{s3_path}' (FORMAT 'parquet')
                """
                    )
                )

                # Create merged export
                merged_path = (
                    f"s3://{self.config.s3_bucket}/tmp/merged_at_{run_id}.parquet"
                )
                session.exec(
                    text(
                        f"""
                    COPY (SELECT * FROM 's3://{self.config.s3_bucket}/metrics/*.parquet') 
                    TO '{merged_path}' (FORMAT 'parquet')
                """
                    )
                )

                # Clean up old files and move merged file
                self._cleanup_s3_files(run_id)

        except Exception as e:
            print(f"Warning: S3 export failed: {e}")

    def _cleanup_s3_files(self, run_id: str) -> None:
        """Clean up old S3 files and move merged file."""
        if not self.s3_client:
            return

        try:
            bucket_name = self.config.s3_bucket

            # Delete old parquet files
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix="metrics/"
            )

            if "Contents" in response:
                for obj in response["Contents"]:
                    if obj["Key"].endswith(".parquet"):
                        self.s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])

            # Move merged file to final location
            copy_source = {
                "Bucket": bucket_name,
                "Key": f"tmp/merged_at_{run_id}.parquet",
            }
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=bucket_name,
                Key=f"metrics/{run_id}.parquet",
            )

            # Delete temporary file
            self.s3_client.delete_object(
                Bucket=bucket_name, Key=f"tmp/merged_at_{run_id}.parquet"
            )

        except Exception as e:
            print(f"Warning: S3 cleanup failed: {e}")

    def get_metrics_for_check(
        self, check_id: str, limit: Optional[int] = None
    ) -> List[MetricRecordDuckDB]:
        """Get metrics for a specific check_id."""
        with Session(self.engine) as session:
            statement = select(MetricRecordDuckDB).where(
                MetricRecordDuckDB.check_id == check_id
            )

            if limit:
                statement = statement.limit(limit)

            return session.exec(statement).all()

    def get_metrics_for_run(
        self, run_id: str, limit: Optional[int] = None
    ) -> List[MetricRecordDuckDB]:
        """Get metrics for a specific run_id."""
        with Session(self.engine) as session:
            statement = select(MetricRecordDuckDB).where(
                MetricRecordDuckDB.run_id == run_id
            )

            if limit:
                statement = statement.limit(limit)

            return session.exec(statement).all()
