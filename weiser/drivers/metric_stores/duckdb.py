from pprint import pprint
from typing import List, Tuple, Any
import duckdb
from sqlglot.expressions import insert, values, Select
from sqlglot.dialects import DuckDB
from weiser.loader.models import MetricStore


class DuckDBMetricStore:
    def __init__(self, config: MetricStore) -> None:
        self.config = config
        self.db_name = config.db_name
        self.dialect = DuckDB
        if not self.db_name:
            self.db_name = "./metricstore.db"
        with duckdb.connect(self.db_name) as conn:
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

    # Meant for metadata queries, like anomaly detection
    def execute_query(self, q: Select, check: Any, verbose: bool = False):
        with duckdb.connect(self.db_name) as conn:
            rows = conn.sql(q.sql(dialect=self.dialect)).fetchall()
            if not len(rows) > 0 and not len(rows[0]) > 0 and not rows[0][0] is None:
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
        with duckdb.connect(self.db_name) as conn:
            conn.sql("INSTALL httpfs;")
            conn.sql("LOAD httpfs;")
            conn.sql(f"SET s3_url_style='path'")
            conn.sql(f"SET s3_endpoint = '{self.config.s3_endpoint}'")
            conn.sql(f"SET s3_access_key_id = '{self.config.s3_access_key}'")
            conn.sql(f"SET s3_secret_access_key = '{self.config.s3_secret_access_key}'")
            conn.sql(
                f"COPY (SELECT * FROM metrics WHERE run_id='{run_id}') TO 's3://{self.config.s3_bucket}/metrics/{run_id}.parquet' (FORMAT 'parquet');"
            )
