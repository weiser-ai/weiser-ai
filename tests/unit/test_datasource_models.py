import pytest
from pydantic import ValidationError

from weiser.loader.models import (
    Datasource,
    PostgreSQLDatasource,
    MySQLDatasource,
    CubeDatasource,
    SnowflakeDatasource,
    DatabricksDatasource,
    BigQueryDatasource,
    # create_datasource,
    DBType,
)


class TestBaseDatasource:
    """Test base Datasource model."""

    def test_base_datasource_minimal(self):
        """Test base datasource with minimal required fields."""
        datasource = Datasource(name="test_db")

        assert datasource.name == "test_db"
        assert datasource.type == DBType.postgresql  # Default
        assert datasource.uri is None

    def test_base_datasource_with_uri(self):
        """Test base datasource with URI."""
        uri = "postgresql://user:pass@localhost:5432/testdb"
        datasource = Datasource(name="test_db", uri=uri, type=DBType.postgresql)

        assert datasource.name == "test_db"
        assert datasource.uri == uri
        assert datasource.type == DBType.postgresql


class TestPostgreSQLDatasource:
    """Test PostgreSQL-specific datasource model."""

    def test_postgresql_datasource_defaults(self):
        """Test PostgreSQL datasource with defaults."""
        datasource = PostgreSQLDatasource(
            name="postgres_db", host="localhost", db_name="testdb", user="postgres"
        )

        assert datasource.name == "postgres_db"
        assert datasource.type == DBType.postgresql
        assert datasource.host == "localhost"
        assert datasource.db_name == "testdb"
        assert datasource.user == "postgres"
        assert datasource.port == 5432  # Default port

    def test_postgresql_datasource_custom_port(self):
        """Test PostgreSQL datasource with custom port."""
        datasource = PostgreSQLDatasource(
            name="postgres_db",
            host="localhost",
            port=5433,
            db_name="testdb",
            user="postgres",
            password="secret",
        )

        assert datasource.port == 5433
        assert datasource.password.get_secret_value() == "secret"


class TestMySQLDatasource:
    """Test MySQL-specific datasource model."""

    def test_mysql_datasource_defaults(self):
        """Test MySQL datasource with defaults."""
        datasource = MySQLDatasource(
            name="mysql_db", host="localhost", db_name="testdb", user="mysql_user"
        )

        assert datasource.name == "mysql_db"
        assert datasource.type == DBType.mysql
        assert datasource.port == 3306  # Default MySQL port


class TestSnowflakeDatasource:
    """Test Snowflake-specific datasource model."""

    def test_snowflake_datasource_required_fields(self):
        """Test Snowflake datasource with required fields."""
        datasource = SnowflakeDatasource(
            name="snowflake_db",
            account="xy12345.us-east-1",
            user="snowflake_user",
            password="secret",
        )

        assert datasource.name == "snowflake_db"
        assert datasource.type == DBType.snowflake
        assert datasource.account == "xy12345.us-east-1"
        assert datasource.user == "snowflake_user"

    def test_snowflake_datasource_missing_account(self):
        """Test Snowflake datasource validation fails without account."""
        with pytest.raises(ValidationError):
            SnowflakeDatasource(
                name="snowflake_db",
                user="snowflake_user",
                password="secret",
                # Missing required account field
            )

    def test_snowflake_datasource_with_warehouse_and_role(self):
        """Test Snowflake datasource with optional warehouse and role."""
        datasource = SnowflakeDatasource(
            name="snowflake_db",
            account="xy12345.us-east-1",
            user="snowflake_user",
            password="secret",
            warehouse="COMPUTE_WH",
            role="ANALYST_ROLE",
            db_name="PRODUCTION_DB",
            schema_name="PUBLIC",
        )

        assert datasource.warehouse == "COMPUTE_WH"
        assert datasource.role == "ANALYST_ROLE"
        assert datasource.db_name == "PRODUCTION_DB"
        assert datasource.schema_name == "PUBLIC"


class TestDatabricksDatasource:
    """Test Databricks-specific datasource model."""

    def test_databricks_datasource_required_fields(self):
        """Test Databricks datasource with required fields."""
        datasource = DatabricksDatasource(
            name="databricks_db",
            host="dbc-12345678-90ab.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/abc123def456",
            access_token="dapi123456789abcdef",
        )

        assert datasource.name == "databricks_db"
        assert datasource.type == DBType.databricks
        assert datasource.host == "dbc-12345678-90ab.cloud.databricks.com"
        assert datasource.http_path == "/sql/1.0/warehouses/abc123def456"
        assert datasource.access_token.get_secret_value() == "dapi123456789abcdef"

    def test_databricks_datasource_missing_required_fields(self):
        """Test Databricks datasource validation fails without required fields."""
        with pytest.raises(ValidationError):
            DatabricksDatasource(
                name="databricks_db",
                host="dbc-12345678-90ab.cloud.databricks.com",
                # Missing required http_path and access_token
            )

    def test_databricks_datasource_with_catalog(self):
        """Test Databricks datasource with catalog and schema."""
        datasource = DatabricksDatasource(
            name="databricks_db",
            host="dbc-12345678-90ab.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/abc123def456",
            access_token="dapi123456789abcdef",
            catalog="main",
            schema_name="default",
        )

        assert datasource.catalog == "main"
        assert datasource.schema_name == "default"


class TestBigQueryDatasource:
    """Test BigQuery-specific datasource model."""

    def test_bigquery_datasource_required_fields(self):
        """Test BigQuery datasource with required fields."""
        datasource = BigQueryDatasource(name="bigquery_db", project_id="my-gcp-project")

        assert datasource.name == "bigquery_db"
        assert datasource.type == DBType.bigquery
        assert datasource.project_id == "my-gcp-project"
        assert datasource.location == "US"  # Default location

    def test_bigquery_datasource_missing_project_id(self):
        """Test BigQuery datasource validation fails without project_id."""
        with pytest.raises(ValidationError):
            BigQueryDatasource(
                name="bigquery_db"
                # Missing required project_id
            )

    def test_bigquery_datasource_with_optional_fields(self):
        """Test BigQuery datasource with optional fields."""
        datasource = BigQueryDatasource(
            name="bigquery_db",
            project_id="my-gcp-project",
            dataset_id="analytics",
            credentials_path="/path/to/service-account.json",
            location="EU",
        )

        assert datasource.dataset_id == "analytics"
        assert datasource.credentials_path == "/path/to/service-account.json"
        assert datasource.location == "EU"


class TestDatasourceValidationErrors:
    """Test validation errors for all datasource models."""

    def test_postgresql_datasource_invalid_port_type(self):
        """Test PostgreSQL datasource validation fails with invalid port type."""
        with pytest.raises(ValidationError) as exc_info:
            PostgreSQLDatasource(
                name="postgres_db",
                host="localhost",
                port="invalid_port",  # Should be int, not string
                user="postgres"
            )
        
        assert "Input should be a valid integer" in str(exc_info.value)

    def test_mysql_datasource_missing_name(self):
        """Test MySQL datasource validation fails when name is missing."""
        with pytest.raises(ValidationError) as exc_info:
            MySQLDatasource(
                # name is required but missing
                host="localhost",
                user="mysql_user"
            )
        
        assert "Field required" in str(exc_info.value)

    def test_cube_datasource_invalid_password_type(self):
        """Test Cube datasource validation fails with invalid password type."""
        # Test that non-string passwords are rejected
        with pytest.raises(ValidationError) as exc_info:
            CubeDatasource(
                name="cube_db",
                host="localhost",
                user="cube_user",
                password=123456  # Should be string, not int
            )
        
        assert "Input should be a valid string" in str(exc_info.value)

    def test_snowflake_datasource_missing_account_detailed(self):
        """Test Snowflake datasource validation fails when account is missing with detailed error."""
        with pytest.raises(ValidationError) as exc_info:
            SnowflakeDatasource(
                name="snowflake_db",
                user="snowflake_user",
                password="secret"
                # Missing required 'account' field
            )
        
        error_details = exc_info.value.errors()
        assert len(error_details) == 1
        assert error_details[0]['type'] == 'missing'
        assert error_details[0]['loc'] == ('account',)

    def test_databricks_datasource_all_missing_required_fields(self):
        """Test Databricks datasource validation fails when all required fields are missing."""
        with pytest.raises(ValidationError) as exc_info:
            DatabricksDatasource(
                name="databricks_db"
                # Missing all required fields: host, http_path, access_token
            )
        
        error_details = exc_info.value.errors()
        # Should have 3 missing field errors
        assert len(error_details) == 3
        
        missing_fields = {error['loc'][0] for error in error_details}
        assert missing_fields == {'host', 'http_path', 'access_token'}

    def test_databricks_datasource_empty_required_fields(self):
        """Test Databricks datasource accepts empty strings (demonstrates current behavior)."""
        # Empty strings are currently valid - this test shows current behavior
        # In a real application, you might want to add Field(min_length=1) validation
        datasource = DatabricksDatasource(
            name="databricks_db",
            host="",  # Empty string - currently allowed
            http_path="",  # Empty string - currently allowed
            access_token=""  # Empty string - currently allowed
        )
        
        # This passes because Pydantic doesn't validate min_length by default
        assert datasource.host == ""
        assert datasource.http_path == ""
        assert datasource.access_token.get_secret_value() == ""

    def test_bigquery_datasource_wrong_type_for_project_id(self):
        """Test BigQuery datasource validation fails with wrong type for project_id."""
        with pytest.raises(ValidationError) as exc_info:
            BigQueryDatasource(
                name="bigquery_db",
                project_id=12345  # Should be string, not int
            )
        
        assert "Input should be a valid string" in str(exc_info.value)

    def test_datasource_invalid_name_type(self):
        """Test base datasource validation fails with invalid name type."""
        with pytest.raises(ValidationError) as exc_info:
            PostgreSQLDatasource(
                name=None,  # Name cannot be None
                host="localhost"
            )
        
        assert "Input should be a valid string" in str(exc_info.value)

    def test_secret_str_field_validation(self):
        """Test SecretStr field validation requires string input."""
        # Test that SecretStr fields reject non-string types
        
        # PostgreSQL with integer password (should fail)
        with pytest.raises(ValidationError) as exc_info:
            PostgreSQLDatasource(
                name="postgres_db",
                password=123456  # Should be string, not int
            )
        assert "Input should be a valid string" in str(exc_info.value)
        
        # Databricks with non-string access token (should fail)
        with pytest.raises(ValidationError) as exc_info:
            DatabricksDatasource(
                name="databricks_db",
                host="host.com",
                http_path="/path",
                access_token=999888777  # Should be string, not int
            )
        assert "Input should be a valid string" in str(exc_info.value)
        
        # Test that string values work correctly
        pg_datasource = PostgreSQLDatasource(
            name="postgres_db",
            password="string_password"
        )
        assert pg_datasource.password.get_secret_value() == "string_password"

    def test_optional_fields_with_none(self):
        """Test that optional fields properly accept None values."""
        # All these should work without errors
        pg_datasource = PostgreSQLDatasource(
            name="postgres_db",
            host=None,
            user=None, 
            db_name=None,
            password=None
        )
        
        assert pg_datasource.host is None
        assert pg_datasource.user is None
        assert pg_datasource.db_name is None
        assert pg_datasource.password is None

    def test_discriminator_type_consistency(self):
        """Test that discriminator type field is consistent and cannot be overridden."""
        # Test that each model has the correct type set automatically
        pg_ds = PostgreSQLDatasource(name="test")
        mysql_ds = MySQLDatasource(name="test")
        cube_ds = CubeDatasource(name="test")
        snowflake_ds = SnowflakeDatasource(name="test", account="test-account")
        databricks_ds = DatabricksDatasource(
            name="test", 
            host="test.com", 
            http_path="/path", 
            access_token="token"
        )
        bigquery_ds = BigQueryDatasource(name="test", project_id="test-project")
        
        assert pg_ds.type == "postgresql"
        assert mysql_ds.type == "mysql"
        assert cube_ds.type == "cube"
        assert snowflake_ds.type == "snowflake"
        assert databricks_ds.type == "databricks"
        assert bigquery_ds.type == "bigquery"

    def test_port_boundary_values(self):
        """Test port validation with boundary values."""
        # Test with port 0 (might be invalid for real use)
        datasource = PostgreSQLDatasource(
            name="postgres_db",
            port=0
        )
        assert datasource.port == 0
        
        # Test with very high port
        datasource2 = MySQLDatasource(
            name="mysql_db", 
            port=65535  # Max valid port
        )
        assert datasource2.port == 65535
        
        # Test negative port - this might not fail without explicit validation
        datasource3 = CubeDatasource(
            name="cube_db",
            port=-1  # Invalid port number
        )
        assert datasource3.port == -1  # Currently passes but could be restricted
