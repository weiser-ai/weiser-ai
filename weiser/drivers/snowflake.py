from snowflake.sqlalchemy import URL

from weiser.drivers.base import BaseDriver
from weiser.loader.models import Datasource


class SnowflakeDriver(BaseDriver):
    """Snowflake driver implementation for Weiser data quality framework."""

    def __init__(self, data_source: Datasource) -> None:
        # Handle Snowflake-specific connection parameters
        if not data_source.uri:

            if not data_source.account:
                raise ValueError(
                    "Snowflake account identifier is required for connection."
                )
            if not data_source.warehouse:
                raise ValueError("Snowflake warehouse is required for connection.")
            if not data_source.role:
                raise ValueError("Snowflake role is required for connection.")
            if not data_source.db_name:
                raise ValueError("Snowflake database name is required for connection.")

            data_source.uri = URL(
                account=data_source.account,  # This should be the account identifier
                user=data_source.user,
                password=data_source.password.get_secret_value(),
                schema=data_source.schema_name or "public",
                warehouse=data_source.warehouse,
                role=data_source.role,
                database=data_source.db_name,
            )

        # Initialize the base driver
        super().__init__(data_source)
