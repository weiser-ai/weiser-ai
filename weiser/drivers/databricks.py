from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from typing import Any, List
from sqlglot.expressions import Select

from weiser.drivers.base import BaseDriver
from weiser.loader.models import Datasource


class DatabricksDriver(BaseDriver):
    """Databricks driver implementation for Weiser data quality framework."""
    
    def __init__(self, data_source: Datasource) -> None:
        # Handle Databricks-specific connection parameters
        if not data_source.uri:
            # Validate required Databricks parameters
            if not data_source.host:
                raise ValueError(
                    "Databricks workspace hostname is required for connection."
                )
            if not data_source.access_token:
                raise ValueError("Databricks access token is required for connection.")
            if not data_source.http_path:
                raise ValueError("Databricks HTTP path (cluster/warehouse endpoint) is required for connection.")
            
            # Databricks connection string format:
            # databricks://token:{access_token}@{hostname}?http_path={http_path}&catalog={catalog}&schema={schema}
            uri_params = {}
            
            # Add HTTP path (required for compute endpoint)
            uri_params['http_path'] = data_source.http_path
            
            # Add catalog if specified (Unity Catalog)
            if hasattr(data_source, 'catalog') and data_source.catalog:
                uri_params['catalog'] = data_source.catalog
            
            # Add schema if specified
            if hasattr(data_source, 'schema_name') and data_source.schema_name:
                uri_params['schema'] = data_source.schema_name
            
            data_source.uri = URL.create(
                "databricks",
                username="token",
                password=data_source.access_token.get_secret_value(),
                host=data_source.host,
                query=uri_params
            )
        
        # Initialize the base driver
        super().__init__(data_source)
    
    def execute_query(self, q: Select, check: Any, verbose: bool = False) -> List[Any]:
        """Execute query with Databricks-specific handling."""
        # Use the base implementation but with Databricks dialect
        return super().execute_query(q, check, verbose)