from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from pprint import pprint
from typing import Any, List

from sqlglot.dialects import (
    BigQuery,
    ClickHouse,
    Databricks,
    Dialect,
    Doris,
    Drill,
    DuckDB,
    Hive,
    MySQL,
    Oracle,
    Postgres,
    Presto,
    Redshift,
    Snowflake,
    Spark,
    Spark2,
    SQLite,
    StarRocks,
    Tableau,
    Teradata,
    Trino,
)
from sqlglot.expressions import Select

from weiser.loader.models import Datasource, DBType

DIALECT_TYPE_MAP = {
    "postgresql": Postgres,
    "mysql": MySQL,
    "oracle": Oracle,
    "snowflake": Snowflake,
    "databricks": Databricks,
    "bigquery": BigQuery,
    "cube": Postgres,
}


class BaseDriver:
    def __init__(self, data_source: Datasource) -> None:
        if not data_source.uri:
            data_source.uri = URL.create(
                (
                    data_source.type
                    if data_source.type != DBType.cube
                    else DBType.postgresql
                ),
                username=data_source.user,
                password=data_source.password.get_secret_value(),
                host=data_source.host,
                database=data_source.db_name,
            )

        self.data_source = data_source
        self.engine = create_engine(data_source.uri)
        self.dialect = DIALECT_TYPE_MAP.get(data_source.type, Dialect)()

    def execute_query(self, q: Select, check: Any, verbose: bool = False) -> List[Any]:
        engine = self.engine
        with engine.connect() as conn:
            rows = list(conn.execute(text(q.sql(dialect=self.dialect))))
            if not len(rows) > 0 or (len(rows) > 0 and (not len(rows[0]) > 0 or rows[0][0] is None)):
                raise Exception(
                    f"Unexpected result executing check: {check.model_dump()}"
                )
            if verbose:
                # pprint(rows)
                pass
        return rows
