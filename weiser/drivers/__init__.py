from typing import Union

from weiser.loader.models import Datasource, DBType
from weiser.drivers.base import BaseDriver
from weiser.drivers.postgres import PostgresDriver
from weiser.drivers.snowflake import SnowflakeDriver
from weiser.drivers.databricks import DatabricksDriver


DB_DRIVER_MAP = {
    DBType.postgresql: PostgresDriver,
    DBType.snowflake: SnowflakeDriver,
    DBType.databricks: DatabricksDriver,
}

DBDriverType = Union[BaseDriver, PostgresDriver, SnowflakeDriver, DatabricksDriver]


class DriverFactory:
    @staticmethod
    def create_driver(data_source: Datasource) -> DBDriverType:
        return DB_DRIVER_MAP.get(data_source.type, BaseDriver)(data_source)
