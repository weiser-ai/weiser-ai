from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from pypika import Query, MySQLQuery, MSSQLQuery, PostgreSQLQuery, OracleQuery, VerticaQuery
from pypika.dialects import SnowflakeQuery

from weiser.loader.models import Datasource

QUERY_TYPE_MAP = {
    'postgresql': PostgreSQLQuery,
    'mysql': MySQLQuery,
    'oracle': OracleQuery,
    'mssql': MSSQLQuery,
    'vertica': VerticaQuery,
    'snowflake': SnowflakeQuery
}

class DriverFactory():
    @staticmethod
    def create_driver(data_source: Datasource):
        # TODO: create custom drivers for different DBs, BaseDriver only work for postgres.
        return BaseDriver(data_source)

class BaseDriver():
    def __init__(self, data_source: Datasource) -> None:
        if not data_source.uri:
            data_source.uri = URL.create(
                data_source.type,
                username=data_source.user,
                password=data_source.password,
                host=data_source.host,
                database=data_source.db_name,
            )
        
        self.engine = create_engine(data_source.uri)

        self.query = QUERY_TYPE_MAP.get(data_source.type, Query)

