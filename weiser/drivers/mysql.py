from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from typing import Any, List
from sqlglot.expressions import Select

from weiser.drivers.base import BaseDriver
from weiser.loader.models import Datasource


class MySQLDriver(BaseDriver):
    """MySQL driver implementation for Weiser data quality framework."""
    
    def __init__(self, data_source: Datasource) -> None:
        # Handle MySQL-specific connection parameters
        if not data_source.uri:
            # Create MySQL connection URI with PyMySQL as the driver
            # Format: mysql+pymysql://user:password@host:port/database
            data_source.uri = URL.create(
                "mysql+pymysql",
                username=data_source.user,
                password=data_source.password.get_secret_value() if data_source.password else None,
                host=data_source.host or "localhost",
                port=data_source.port or 3306,
                database=data_source.db_name
            )
        
        # Initialize the base driver
        super().__init__(data_source)
    
    def execute_query(self, q: Select, check: Any, verbose: bool = False) -> List[Any]:
        """Execute query with MySQL-specific handling."""
        # Use the base implementation but with MySQL dialect
        return super().execute_query(q, check, verbose)