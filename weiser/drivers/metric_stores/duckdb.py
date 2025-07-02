import boto3
import duckdb
from rich import print
from typing import List, Tuple, Any

from sqlglot.expressions import insert, values, Select
from sqlglot.dialects import DuckDB
from weiser.loader.models import MetricStore, S3UrlStyle


class DuckDBMetricStore:
    def __init__(self, config: MetricStore) -> None:
        self.config = config
        self.s3_client = None

        # Only initialize S3 client if S3 configuration is provided
        if self._has_s3_config():
            self.s3_client = boto3.client(
                "s3",
                region_name=self.config.s3_region,
                aws_access_key_id=self.config.s3_access_key,
                aws_secret_access_key=self.config.s3_secret_access_key,
            )

        self.db_name = config.db_name
        self.dialect = DuckDB
        if not self.db_name:
            self.db_name = "./metricstore.db"

        with duckdb.connect(self.db_name) as conn:
            # Only configure S3 settings if S3 is configured
            if self._has_s3_config():
                conn.sql("INSTALL httpfs;")
                conn.sql("LOAD httpfs;")
                if self.config.s3_url_style == S3UrlStyle.path:
                    conn.sql(f"SET s3_url_style='{self.config.s3_url_style}'")
                elif self.config.s3_url_style == S3UrlStyle.vhost:
                    conn.sql(f"SET s3_region = '{self.config.s3_region}'")
                if self.config.s3_endpoint:
                    conn.sql(f"SET s3_endpoint = '{self.config.s3_endpoint}'")
                conn.sql(f"SET s3_access_key_id = '{self.config.s3_access_key}'")
                conn.sql(
                    f"SET s3_secret_access_key = '{self.config.s3_secret_access_key}'"
                )

            # Create the metrics table
            conn.sql(
                """CREATE TABLE IF NOT EXISTS metrics (
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
                     success boolean,
                     threshold VARCHAR,
                     threshold_list DOUBLE[],
                     type VARCHAR
                     )"""
            )

            # Only attempt S3 import if S3 is configured
            if self._has_s3_config():
                res = conn.sql(
                    """SELECT MAX(run_time) AS run_time FROM metrics"""
                ).fetchall()
                last_run_time = "1=1"
                if res and res[0][0]:
                    last_run_time = f"run_time > '{res[0][0]}'"
                try:
                    conn.sql(
                        f"""
                        INSERT INTO metrics SELECT * FROM 's3://{self.config.s3_bucket}/metrics/*.parquet' WHERE {last_run_time};
                        """
                    )
                except Exception:
                    # Ignore errors when S3 data doesn't exist yet
                    pass

    def _has_s3_config(self) -> bool:
        """Check if S3 configuration is properly provided."""
        return (
            self.config.s3_bucket is not None
            and self.config.s3_access_key is not None
            and self.config.s3_secret_access_key is not None
        )

    # Delete Parquet files
    def delete_parquet_files(self, prefix):
        if not self._has_s3_config() or not self.s3_client:
            return

        bucket_name = self.config.s3_bucket
        response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if "Contents" in response:
            for obj in response["Contents"]:
                if obj["Key"].endswith(".parquet"):
                    self.s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
                    # print(f"Deleted {obj['Key']}")

    # Move Parquet files
    def move_file(self, source_key, destination_key):
        if not self._has_s3_config() or not self.s3_client:
            return

        bucket_name = self.config.s3_bucket
        copy_source = {"Bucket": bucket_name, "Key": source_key}
        # Copy the file to the new location
        self.s3_client.copy_object(
            CopySource=copy_source, Bucket=bucket_name, Key=destination_key
        )
        # Delete the original file
        self.s3_client.delete_object(Bucket=bucket_name, Key=source_key)

    # Meant for metadata queries, like anomaly detection
    def execute_query(
        self,
        q: Select,
        check: Any,
        verbose: bool = False,
        validate_results: bool = True,
    ):
        with duckdb.connect(self.db_name) as conn:
            rows = conn.sql(q.sql(dialect=self.dialect)).fetchall()
            if validate_results and not len(rows) > 0:
                if verbose:
                    print(q.sql(dialect=self.dialect))
                raise Exception(
                    f"Unexpected result executing check: {check.model_dump()}"
                )
            if verbose:
                # pprint(rows)
                pass
        return rows

    def insert_results(self, record):
        with duckdb.connect(self.db_name) as conn:
            if isinstance(record["threshold"], List) or isinstance(
                record["threshold"], Tuple
            ):
                record["threshold_list"] = record["threshold"]
                record["threshold"] = None
            elif "threshold_list" not in record:
                record["threshold_list"] = None
            q = insert(
                values(
                    [
                        (
                            record["actual_value"],
                            record["check_id"],
                            record["condition"],
                            record["dataset"],
                            record["datasource"],
                            record["fail"],
                            record["name"],
                            record["run_id"],
                            record["run_time"],
                            record["measure"],
                            record["success"],
                            record["threshold"],
                            record["threshold_list"],
                            record["type"],
                        )
                    ]
                ),
                "metrics",
            )
            conn.sql(q.sql(dialect="duckdb"))

    def export_results(self, run_id):
        # First get the results
        results = {"summary": {}, "failures": []}

        with duckdb.connect(self.db_name) as conn:
            # Get summary statistics
            summary_query = f"""
                SELECT 
                    COUNT(*) as total_checks,
                    SUM(CASE WHEN success THEN 1 ELSE 0 END) as passed_checks,
                    SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) as failed_checks
                FROM metrics 
                WHERE run_id = '{run_id}'
            """
            summary_results = conn.sql(summary_query).fetchone()
            results["summary"] = {
                "total_checks": summary_results[0],
                "passed_checks": summary_results[1],
                "failed_checks": summary_results[2],
            }

            # Get detailed failure information (max 20 rows)
            failures_query = f"""
                SELECT 
                    name,
                    dataset,
                    datasource,
                    check_id,
                    condition,
                    actual_value,
                    threshold,
                    type
                FROM metrics 
                WHERE run_id = '{run_id}' 
                    AND NOT success 
                LIMIT 20
            """
            failure_results = conn.sql(failures_query).fetchall()
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

        # Now handle S3 export if configured
        if self._has_s3_config() and self.s3_client:
            with duckdb.connect(self.db_name) as conn:
                conn.sql("INSTALL httpfs;")
                conn.sql("LOAD httpfs;")
                if self.config.s3_url_style == S3UrlStyle.path:
                    conn.sql(f"SET s3_url_style='{self.config.s3_url_style}'")
                elif self.config.s3_url_style == S3UrlStyle.vhost:
                    conn.sql(f"SET s3_region = '{self.config.s3_region}'")
                if self.config.s3_endpoint:
                    conn.sql(f"SET s3_endpoint = '{self.config.s3_endpoint}'")
                conn.sql(f"SET s3_access_key_id = '{self.config.s3_access_key}'")
                conn.sql(
                    f"SET s3_secret_access_key = '{self.config.s3_secret_access_key}'"
                )
                conn.sql(
                    f"COPY (SELECT * FROM metrics WHERE run_id='{run_id}') TO 's3://{self.config.s3_bucket}/metrics/{run_id}.parquet' (FORMAT 'parquet');"
                )
                conn.sql(
                    f"""
                        COPY (SELECT * FROM 's3://{self.config.s3_bucket}/metrics/*.parquet') TO 's3://{self.config.s3_bucket}/tmp/merged_at_{run_id}.parquet' (FORMAT 'parquet');
                    """
                )
                # Delete old Parquet files
                self.delete_parquet_files("metrics/")
                # Move the merged file to the final location
                self.move_file(
                    source_key=f"tmp/merged_at_{run_id}.parquet",
                    destination_key=f"metrics/{run_id}.parquet",
                )
        else:
            print("No S3 bucket configured, skipping export")

        return results
