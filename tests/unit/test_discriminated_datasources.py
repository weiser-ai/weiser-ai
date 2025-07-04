import pytest
import yaml
from pydantic import ValidationError

from weiser.loader.models import (
    BaseConfig,
    PostgreSQLDatasource,
    SnowflakeDatasource,
    BigQueryDatasource,
    DatabricksDatasource,
    Check,
    CheckType,
    Condition
)


class TestDiscriminatedDatasources:
    """Test discriminated union for datasources works in configurations."""

    def test_config_with_multiple_datasource_types(self):
        """Test configuration with multiple different datasource types."""
        config_data = {
            "version": 1,
            "checks": [
                {
                    "name": "test_check",
                    "dataset": "orders",
                    "type": "row_count",
                    "condition": "gt",
                    "threshold": 0
                }
            ],
            "datasources": [
                {
                    "name": "postgres_db",
                    "type": "postgresql",
                    "host": "localhost",
                    "user": "postgres",
                    "db_name": "testdb"
                },
                {
                    "name": "snowflake_db",
                    "type": "snowflake",
                    "account": "xy12345.us-east-1",
                    "user": "sf_user",
                    "password": "sf_pass"
                },
                {
                    "name": "bigquery_db",
                    "type": "bigquery",
                    "project_id": "my-gcp-project",
                    "dataset_id": "analytics"
                }
            ],
            "connections": [
                {
                    "name": "metricstore",
                    "type": "metricstore",
                    "db_type": "duckdb"
                }
            ]
        }
        
        config = BaseConfig(**config_data)
        
        assert len(config.datasources) == 3
        
        # Check that each datasource is the correct type
        postgres_ds = config.datasources[0]
        snowflake_ds = config.datasources[1]
        bigquery_ds = config.datasources[2]
        
        assert isinstance(postgres_ds, PostgreSQLDatasource)
        assert postgres_ds.type == "postgresql"
        assert postgres_ds.host == "localhost"
        
        assert isinstance(snowflake_ds, SnowflakeDatasource)
        assert snowflake_ds.type == "snowflake"
        assert snowflake_ds.account == "xy12345.us-east-1"
        
        assert isinstance(bigquery_ds, BigQueryDatasource)
        assert bigquery_ds.type == "bigquery"
        assert bigquery_ds.project_id == "my-gcp-project"

    def test_config_with_invalid_required_fields(self):
        """Test validation fails when required fields are missing."""
        config_data = {
            "version": 1,
            "checks": [
                {
                    "name": "test_check",
                    "dataset": "orders",
                    "type": "row_count",
                    "condition": "gt",
                    "threshold": 0
                }
            ],
            "datasources": [
                {
                    "name": "snowflake_db",
                    "type": "snowflake",
                    "user": "sf_user",
                    "password": "sf_pass"
                    # Missing required 'account' field
                }
            ]
        }
        
        with pytest.raises(ValidationError) as exc_info:
            BaseConfig(**config_data)
        
        assert "account" in str(exc_info.value)

    def test_config_discriminates_correctly_by_type(self):
        """Test that the discriminator correctly selects the right model based on type."""
        # Test PostgreSQL datasource
        postgres_data = {
            "version": 1,
            "checks": [{"name": "test", "dataset": "orders", "type": "row_count", "condition": "gt", "threshold": 0}],
            "datasources": [
                {
                    "name": "pg_db",
                    "type": "postgresql",
                    "host": "localhost",
                    "port": 5432,
                    "user": "user"
                }
            ]
        }
        
        config = BaseConfig(**postgres_data)
        datasource = config.datasources[0]
        
        assert isinstance(datasource, PostgreSQLDatasource)
        assert datasource.port == 5432  # PostgreSQL default
        
        # Test Databricks datasource
        databricks_data = {
            "version": 1,
            "checks": [{"name": "test", "dataset": "orders", "type": "row_count", "condition": "gt", "threshold": 0}],
            "datasources": [
                {
                    "name": "db_warehouse",
                    "type": "databricks",
                    "host": "dbc-12345678-90ab.cloud.databricks.com",
                    "http_path": "/sql/1.0/warehouses/abc123def456",
                    "access_token": "dapi123456789abcdef"
                }
            ]
        }
        
        config = BaseConfig(**databricks_data)
        datasource = config.datasources[0]
        
        assert isinstance(datasource, DatabricksDatasource)
        assert datasource.host == "dbc-12345678-90ab.cloud.databricks.com"
        assert datasource.http_path == "/sql/1.0/warehouses/abc123def456"

    def test_yaml_configuration_parsing(self):
        """Test that YAML configuration with different datasource types parses correctly."""
        yaml_config = """
version: 1
datasources:
  - name: postgres_main
    type: postgresql
    host: localhost
    port: 5432
    db_name: main_db
    user: postgres_user
    password: postgres_pass
    
  - name: snowflake_warehouse
    type: snowflake
    account: xy12345.us-east-1
    user: sf_user
    password: sf_pass
    warehouse: COMPUTE_WH
    
  - name: bigquery_analytics
    type: bigquery
    project_id: my-gcp-project
    dataset_id: analytics
    location: US

checks:
  - name: postgres_check
    dataset: orders
    type: row_count
    condition: gt
    threshold: 100
    
  - name: snowflake_check
    dataset: sales
    type: sum
    measure: amount
    condition: gt
    threshold: 10000
    
  - name: bigquery_check
    dataset: events
    type: row_count
    condition: gt
    threshold: 1000

connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
"""
        
        config_data = yaml.safe_load(yaml_config)
        config = BaseConfig(**config_data)
        
        assert len(config.datasources) == 3
        assert len(config.checks) == 3
        
        # Verify datasource types
        assert isinstance(config.datasources[0], PostgreSQLDatasource)
        assert isinstance(config.datasources[1], SnowflakeDatasource)
        assert isinstance(config.datasources[2], BigQueryDatasource)
        
        # Verify specific fields
        assert config.datasources[0].port == 5432
        assert config.datasources[1].warehouse == "COMPUTE_WH"
        assert config.datasources[2].location == "US"

    def test_config_with_unknown_datasource_type(self):
        """Test that unknown datasource types cause validation error."""
        config_data = {
            "version": 1,
            "checks": [
                {
                    "name": "test_check",
                    "dataset": "orders",
                    "type": "row_count",
                    "condition": "gt",
                    "threshold": 0
                }
            ],
            "datasources": [
                {
                    "name": "unknown_db",
                    "type": "unknown_database_type",  # This should fail
                    "host": "localhost"
                }
            ]
        }
        
        with pytest.raises(ValidationError) as exc_info:
            BaseConfig(**config_data)
        
        # Should fail because 'unknown_database_type' doesn't match any of the literal types
        assert "union_tag_invalid" in str(exc_info.value)
        assert "unknown_database_type" in str(exc_info.value)

    def test_config_serialization_preserves_types(self):
        """Test that config serialization preserves the discriminated types."""
        config_data = {
            "version": 1,
            "checks": [
                {
                    "name": "test_check",
                    "dataset": "orders",
                    "type": "row_count",
                    "condition": "gt",
                    "threshold": 0
                }
            ],
            "datasources": [
                {
                    "name": "postgres_db",
                    "type": "postgresql",
                    "host": "localhost",
                    "port": 5432,
                    "user": "postgres"
                },
                {
                    "name": "bigquery_db",
                    "type": "bigquery",
                    "project_id": "my-project",
                    "location": "EU"
                }
            ]
        }
        
        # Create config from data
        config = BaseConfig(**config_data)
        
        # Serialize back to dict
        serialized = config.model_dump()
        
        # Recreate from serialized data
        config2 = BaseConfig(**serialized)
        
        # Verify types are preserved
        assert isinstance(config2.datasources[0], PostgreSQLDatasource)
        assert isinstance(config2.datasources[1], BigQueryDatasource)
        assert config2.datasources[0].port == 5432
        assert config2.datasources[1].location == "EU"