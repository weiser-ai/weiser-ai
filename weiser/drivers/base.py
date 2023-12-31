from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from pprint import pprint
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

    def execute_query(self, q, check, verbose):
        engine = self.engine
        with engine.connect() as conn:
            rows = list(conn.execute(str(q)))
            if not len(rows) > 0 and not len(rows[0]) > 0 and not rows[0][0] is None:
                raise Exception(f'Unexpected result executing check: {check.model_dump()}')
            if verbose:
                pprint(rows)
            value = rows[0][0]
        return value

