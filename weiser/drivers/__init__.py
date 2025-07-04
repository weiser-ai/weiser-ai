from typing import Union

from weiser.loader.models import Datasource, DBType
from weiser.drivers.base import BaseDriver
from weiser.drivers.postgres import PostgresDriver
from weiser.drivers.mysql import MySQLDriver
from weiser.drivers.snowflake import SnowflakeDriver
from weiser.drivers.databricks import DatabricksDriver
from weiser.drivers.bigquery import BigQueryDriver


DB_DRIVER_MAP = {
    DBType.postgresql: PostgresDriver,
    DBType.mysql: MySQLDriver,
    DBType.snowflake: SnowflakeDriver,
    DBType.databricks: DatabricksDriver,
    DBType.bigquery: BigQueryDriver,
}

DBDriverType = Union[BaseDriver, PostgresDriver, MySQLDriver, SnowflakeDriver, DatabricksDriver, BigQueryDriver]


class DriverFactory:
    @staticmethod
    def create_driver(data_source: Datasource) -> DBDriverType:
        return DB_DRIVER_MAP.get(data_source.type, BaseDriver)(data_source)
