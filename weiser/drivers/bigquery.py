from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from typing import Any, List
from sqlglot.expressions import Select
import os

from weiser.drivers.base import BaseDriver
from weiser.loader.models import Datasource


class BigQueryDriver(BaseDriver):
    """BigQuery driver implementation for Weiser data quality framework."""
    
    def __init__(self, data_source: Datasource) -> None:
        # Handle BigQuery-specific connection parameters
        if not data_source.uri:
            # Validate required BigQuery parameters
            if not data_source.project_id:
                raise ValueError(
                    "BigQuery project_id is required for connection."
                )
            
            # BigQuery connection string format:
            # bigquery://project_id/dataset_id?credentials_path=path&location=location
            uri_params = {}
            
            # Add credentials path if specified
            if hasattr(data_source, 'credentials_path') and data_source.credentials_path:
                uri_params['credentials_path'] = data_source.credentials_path
            
            # Add location if specified (for regional datasets)
            if hasattr(data_source, 'location') and data_source.location:
                uri_params['location'] = data_source.location
            
            # Create BigQuery URI
            data_source.uri = URL.create(
                "bigquery",
                host=data_source.project_id,
                database=data_source.dataset_id or data_source.db_name,
                query=uri_params if uri_params else None
            )
        
        # Set up Google Cloud credentials if credentials_path is provided
        if hasattr(data_source, 'credentials_path') and data_source.credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = data_source.credentials_path
        
        # Initialize the base driver
        super().__init__(data_source)
    
    def execute_query(self, q: Select, check: Any, verbose: bool = False) -> List[Any]:
        """Execute query with BigQuery-specific handling."""
        # Use the base implementation but with BigQuery dialect
        return super().execute_query(q, check, verbose)