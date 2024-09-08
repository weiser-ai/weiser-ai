from pprint import pprint
from typing import Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlglot.expressions import insert, values, Select
from sqlglot.dialects import Postgres
from typing import List, Tuple

from weiser.loader.models import MetricStore


class PostgresMetricStore:
    def __init__(self, metric_store: MetricStore) -> None:
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

        with self.engine.connect() as conn:
            conn.execute(
                text(
                    """CREATE TABLE IF NOT EXISTS metrics (
                            actual_value double precision,
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
                            threshold_list double precision[],
                            type VARCHAR
                            )"""
                )
            )

    # Meant for metadata queries, like anomaly detection
    def execute_query(
        self,
        q: Select,
        check: Any,
        verbose: bool = False,
        validate_results: bool = True,
    ):
        engine = self.engine
        with engine.connect() as conn:
            rows = list(conn.execute(text(q.sql(dialect=self.dialect))))
            if (
                validate_results
                and not len(rows) > 0
                and not len(rows[0]) > 0
                and not rows[0][0] is None
            ):
                raise Exception(
                    f"Unexpected result executing check: {check.model_dump()}"
                )
            if verbose:
                pass
                # pprint(rows)
        return rows

    def insert_results(self, record):
        with self.engine.connect() as conn:
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
            conn.execute(text(q.sql(dialect="postgres")))

    def export_results(self, run_id):
        pass
