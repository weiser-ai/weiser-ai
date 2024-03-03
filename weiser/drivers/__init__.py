from typing import Union

from weiser.loader.models import Datasource, DBType
from weiser.drivers.base import BaseDriver
from weiser.drivers.postgres import PostgresDriver


DB_DRIVER_MAP = {
    DBType.postgresql: PostgresDriver,
}

DBDriverType = Union[BaseDriver, PostgresDriver]


class DriverFactory:
    @staticmethod
    def create_driver(data_source: Datasource) -> DBDriverType:
        return DB_DRIVER_MAP.get(data_source.type, BaseDriver)(data_source)
