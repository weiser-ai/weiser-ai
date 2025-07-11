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


class DuckDBMetricStore:
    """
    DuckDB V2 MetricStore with SQLModel integration.

    Features:
    - SQLModel-based schema definition
    - Type-safe database operations
    - Maintains S3 integration from V1
    - Direct DDL management (no Alembic)
    """

    def __init__(self, config: MetricStore, verbose: bool = False) -> None:
        self.config = config
        self.s3_client = None
        self.dialect = DuckDB
        self.verbose = verbose

        # Database setup
        self.db_name = config.db_name or "./metricstore.db"

        # Create SQLAlchemy engine with duckdb-engine
        try:
            self.engine = create_engine(f"duckdb:///{self.db_name}")
        except Exception as e:
            if self.verbose:
                print(f"Warning: Engine creation failed: {e}")
            # In test mode, engine might be mocked
            self.engine = None

        # Initialize S3 client if configured
        if self._has_s3_config():
            try:
                self.s3_client = boto3.client(
                    "s3",
                    region_name=self.config.s3_region,
                    aws_access_key_id=self.config.s3_access_key,
                    aws_secret_access_key=self.config.s3_secret_access_key,
                )
            except Exception as e:
                if self.verbose:
                    print(f"Warning: S3 client creation failed: {e}")

        # Initialize database schema and S3 configuration (skip if engine failed)
        if self.engine:
            try:
                self._init_database()

                # Initialize migration runner after database is set up
                self.migration_runner = DuckDBMigrationRunner(self.engine)

                # Run any pending migrations
                self._run_migrations()
            except Exception as e:
                if self.verbose:
                    print(f"Warning: Database initialization failed: {e}")
                # In test environments, this might be expected

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
            if self.verbose:
                print(f"Warning: Table creation failed: {e}")
            # Fallback to direct SQL creation if needed
            try:
                self._create_tables_direct()
            except Exception as e2:
                if self.verbose:
                    print(f"Warning: Direct table creation also failed: {e2}")
                # In test environments, tables might be mocked

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
                    type VARCHAR,
                    tenant_id INTEGER DEFAULT 1
                )
            """
                )
            )

    def _import_s3_data(self) -> None:
        """Import existing data from S3."""
        try:
            with Session(self.engine) as session:
                # Check current table size to determine import strategy
                count_result = session.exec(
                    text("SELECT COUNT(*) FROM metrics")
                ).first()
                current_count = count_result[0] if count_result else 0

                # Get the latest run time to avoid duplicates
                result = session.exec(text("SELECT MAX(run_time) FROM metrics")).first()
                if not result or result[0] is None or current_count == 0:
                    # Table is empty or nearly empty - import all data
                    last_run_time = "1=1"
                    if self.verbose:
                        print(
                            f"Table has {current_count} records, importing all S3 data"
                        )
                else:
                    # Format datetime properly for DuckDB
                    max_time = result[0]
                    if hasattr(max_time, "strftime"):
                        formatted_time = max_time.strftime("%Y-%m-%d %H:%M:%S.%f")
                    else:
                        formatted_time = str(max_time)
                    last_run_time = f"run_time > '{formatted_time}'"
                    if self.verbose:
                        print(
                            f"Table has {current_count} records, importing S3 data newer than {formatted_time}"
                        )

                # Ensure sequence exists before importing
                try:
                    session.exec(
                        text("CREATE SEQUENCE IF NOT EXISTS id_sequence START 1")
                    )
                except Exception:
                    pass  # Sequence might already exist

                # Import from S3 with explicit column casting to match local schema
                s3_path = f"s3://{self.config.s3_bucket}/metrics/*.parquet"
                # Count records before import for comparison
                count_before = session.exec(
                    text("SELECT COUNT(*) FROM metrics")
                ).first()[0]

                # Migrate S3 schema to match current local schema before importing
                self._migrate_s3_schema(session, s3_path)

                session.exec(
                    text(
                        f"""
                    INSERT OR IGNORE INTO metrics 
                    SELECT 
                        COALESCE(id, nextval('id_sequence'))::INTEGER as id,
                        COALESCE(actual_value, NULL)::DOUBLE as actual_value,
                        COALESCE(check_id, NULL)::VARCHAR as check_id,
                        COALESCE(condition, NULL)::VARCHAR as condition,
                        COALESCE(dataset, NULL)::VARCHAR as dataset,
                        COALESCE(datasource, NULL)::VARCHAR as datasource,
                        COALESCE(fail, NULL)::BOOLEAN as fail,
                        COALESCE(name, NULL)::VARCHAR as name,
                        COALESCE(run_id, NULL)::VARCHAR as run_id,
                        COALESCE(run_time, NULL)::TIMESTAMP as run_time,
                        COALESCE(sql, NULL)::VARCHAR as sql,
                        COALESCE(success, NULL)::BOOLEAN as success,
                        COALESCE(threshold, NULL)::VARCHAR as threshold,
                        COALESCE(threshold_list, NULL)::VARCHAR as threshold_list,
                        COALESCE(type, NULL)::VARCHAR as type,
                        COALESCE(tenant_id, 1)::INTEGER as tenant_id
                    FROM read_parquet('{s3_path}', union_by_name=true) 
                    WHERE {last_run_time}
                """
                    )
                )
                # Commit the import transaction immediately
                session.commit()

                # Count how many records were imported
                count_result = session.exec(
                    text("SELECT COUNT(*) FROM metrics")
                ).first()
                total_records = count_result[0] if count_result else 0
                imported_count = total_records - count_before
                if self.verbose:
                    print(
                        f"S3 data import completed successfully. Filter: {last_run_time}. Imported: {imported_count} records. Total records in table: {total_records}"
                    )
        except Exception as ex:
            print(
                f"Warning: S3 data import failed. This may be expected if no data exists yet.: {ex}"
            )
            # Ignore errors when S3 data doesn't exist yet
            pass

    def _migrate_s3_schema(self, session: Session, s3_path: str) -> None:
        """
        Migrate S3 parquet files to match the current local schema.
        
        This creates a unified schema by reading existing S3 data and adding missing columns
        with default values, then writing it back to S3 with the updated schema.
        """
        try:
            if self.verbose:
                print("Checking S3 schema compatibility...")
            
            # Get current local schema from the metrics table
            local_schema = session.exec(text("DESCRIBE metrics")).fetchall()
            local_columns = {row[0]: row[1] for row in local_schema}
            
            # Try to get S3 schema - if no files exist, skip migration
            try:
                s3_schema = session.exec(
                    text(f"DESCRIBE (SELECT * FROM read_parquet('{s3_path}', union_by_name=true) LIMIT 1)")
                ).fetchall()
                s3_columns = {row[0]: row[1] for row in s3_schema}
            except Exception:
                if self.verbose:
                    print("No S3 data found, skipping schema migration")
                return
            
            # Find missing columns in S3 data
            missing_columns = set(local_columns.keys()) - set(s3_columns.keys())
            
            if not missing_columns:
                if self.verbose:
                    print("S3 schema is up to date")
                return
            
            if self.verbose:
                print(f"S3 schema migration needed. Missing columns: {missing_columns}")
            
            # Build SELECT statement with missing columns added
            select_columns = []
            for col_name, col_type in local_columns.items():
                if col_name in s3_columns:
                    # Column exists in S3, use it as-is
                    select_columns.append(f"COALESCE({col_name}, NULL) as {col_name}")
                else:
                    # Column missing in S3, add default value
                    default_value = self._get_default_value_for_column(col_name, col_type)
                    select_columns.append(f"{default_value} as {col_name}")
            
            # Create temporary migrated parquet files
            temp_s3_path = f"s3://{self.config.s3_bucket}/tmp/migrated_metrics.parquet"
            
            if self.verbose:
                print(f"Migrating S3 schema to: {temp_s3_path}")
            
            session.exec(
                text(f"""
                    COPY (
                        SELECT {', '.join(select_columns)}
                        FROM read_parquet('{s3_path}', union_by_name=true)
                    ) TO '{temp_s3_path}' (FORMAT 'parquet')
                """)
            )
            
            # Replace old files with migrated ones
            if self.s3_client:
                self._replace_s3_files_with_migrated(temp_s3_path)
            
            if self.verbose:
                print("S3 schema migration completed")
                
        except Exception as e:
            if self.verbose:
                print(f"S3 schema migration failed (will use fallback): {e}")
            # Don't raise - let the import continue with fallback logic
    
    def _get_default_value_for_column(self, col_name: str, col_type: str) -> str:
        """Get appropriate default value for a missing column based on its type and name."""
        # Define default values for known columns
        column_defaults = {
            'tenant_id': '1::INTEGER',
            'id': 'NULL::INTEGER',  # Will be handled by sequence
        }
        
        if col_name in column_defaults:
            return column_defaults[col_name]
        
        # Fallback based on type
        if 'INTEGER' in col_type.upper():
            return 'NULL::INTEGER'
        elif 'BOOLEAN' in col_type.upper():
            return 'NULL::BOOLEAN'
        elif 'DOUBLE' in col_type.upper() or 'FLOAT' in col_type.upper():
            return 'NULL::DOUBLE'
        elif 'TIMESTAMP' in col_type.upper():
            return 'NULL::TIMESTAMP'
        else:
            return 'NULL::VARCHAR'
    
    def _replace_s3_files_with_migrated(self, temp_s3_path: str) -> None:
        """Replace original S3 files with the schema-migrated version."""
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
            
            # Move migrated file to metrics location
            copy_source = {
                "Bucket": bucket_name,
                "Key": "tmp/migrated_metrics.parquet",
            }
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=bucket_name,
                Key="metrics/migrated_metrics.parquet",
            )
            
            # Delete temporary file
            self.s3_client.delete_object(
                Bucket=bucket_name, Key="tmp/migrated_metrics.parquet"
            )
            
            if self.verbose:
                print("S3 files replaced with migrated schema")
                
        except Exception as e:
            if self.verbose:
                print(f"Warning: S3 file replacement failed: {e}")

    def _run_migrations(self) -> None:
        """Run any pending migrations."""
        try:
            pending_migrations = self.migration_runner.get_pending_migrations()
            if pending_migrations:
                if self.verbose:
                    print(f"Running {len(pending_migrations)} pending migrations...")
                self.migration_runner.migrate_up()
            elif self.verbose:
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
                # if verbose:
                #     print(f"Query: {sql}")
                raise Exception(
                    f"Unexpected result executing check: {check.model_dump()}"
                )

            # if verbose:
            #     print(f"Query result: {rows}")

            return rows

    def insert_results(self, record: dict) -> None:
        """Insert a metric record into the database."""
        # Add tenant_id from config if not present in record
        if "tenant_id" not in record and hasattr(self.config, "tenant_id"):
            record["tenant_id"] = self.config.tenant_id
        
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
        elif self.verbose:
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
                    COPY (SELECT * FROM read_parquet('s3://{self.config.s3_bucket}/metrics/*.parquet', union_by_name=true)) 
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
