import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy import create_engine
from pydantic import ValidationError

from weiser.drivers.base import BaseDriver, DIALECT_TYPE_MAP
from weiser.drivers import DriverFactory
from weiser.drivers.mysql import MySQLDriver
from weiser.drivers.snowflake import SnowflakeDriver
from weiser.drivers.databricks import DatabricksDriver
from weiser.drivers.bigquery import BigQueryDriver
from weiser.drivers.metric_stores import MetricStoreFactory, DuckDBMetricStore
from weiser.drivers.metric_stores.postgres import PostgresMetricStore
from weiser.loader.models import (
    PostgreSQLDatasource,
    MySQLDatasource,
    SnowflakeDatasource,
    DatabricksDatasource,
    BigQueryDatasource,
    CubeDatasource,
    MetricStore,
    MetricStoreType,
    S3UrlStyle,
)

# Import fixtures
from tests.fixtures.config_fixtures import *


class TestBaseDriver:
    """Test BaseDriver functionality."""

    def test_base_driver_initialization_with_components(self):
        """Test BaseDriver initialization with individual components."""
        datasource = PostgreSQLDatasource(
            name="test_db",
            host="localhost",
            port=5432,
            db_name="test_database",
            user="test_user",
            password="test_password",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = BaseDriver(datasource)

            assert driver.data_source == datasource
            # Check that create_engine was called with a valid URL
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert "postgresql://" in str(call_args)

    def test_base_driver_initialization_with_uri(self):
        """Test BaseDriver initialization with pre-configured URI."""
        datasource = PostgreSQLDatasource(
            name="test_db", uri="postgresql://user:pass@localhost:5432/testdb"
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = BaseDriver(datasource)

            assert driver.data_source == datasource
            # Check that create_engine was called with the provided URI
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert str(call_args) == "postgresql://user:pass@localhost:5432/testdb"

    def test_base_driver_cube_type_conversion(self):
        """Test BaseDriver converts cube type to postgresql."""
        datasource = CubeDatasource(
            name="cube_db",
            host="localhost",
            port=5432,
            db_name="cube_database",
            user="cube_user",
            password="cube_password",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = BaseDriver(datasource)

            # Should create URI with postgresql instead of cube
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert str(call_args).startswith("postgresql://")

    def test_dialect_mapping(self):
        """Test dialect mapping for different database types."""
        assert DIALECT_TYPE_MAP["postgresql"].__name__ == "Postgres"
        assert DIALECT_TYPE_MAP["mysql"].__name__ == "MySQL"
        assert DIALECT_TYPE_MAP["cube"].__name__ == "Postgres"
        assert DIALECT_TYPE_MAP["snowflake"].__name__ == "Snowflake"
        assert DIALECT_TYPE_MAP["databricks"].__name__ == "Databricks"
        assert DIALECT_TYPE_MAP["bigquery"].__name__ == "BigQuery"

    @patch("weiser.drivers.base.create_engine")
    def test_execute_query_success(
        self, mock_create_engine, sample_postgresql_datasource
    ):
        """Test successful query execution."""
        # Setup mocks
        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = [(100,), (200,)]

        mock_create_engine.return_value = mock_engine

        # Properly mock the context manager
        context_manager = MagicMock()
        context_manager.__enter__ = Mock(return_value=mock_connection)
        context_manager.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = context_manager

        mock_connection.execute.return_value = mock_result

        driver = BaseDriver(sample_postgresql_datasource)

        # Create a mock query
        mock_query = Mock()
        mock_query.sql.return_value = "SELECT COUNT(*) FROM orders"

        mock_check = Mock()
        mock_check.model_dump.return_value = {"name": "test_check"}

        result = driver.execute_query(mock_query, mock_check, verbose=False)

        assert result == mock_result
        mock_connection.execute.assert_called_once()

    @patch("weiser.drivers.base.create_engine")
    def test_execute_query_empty_result(
        self, mock_create_engine, sample_postgresql_datasource
    ):
        """Test query execution with empty results raises exception."""
        # Setup mocks
        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = []  # Empty result

        mock_create_engine.return_value = mock_engine

        # Properly mock the context manager
        context_manager = MagicMock()
        context_manager.__enter__ = Mock(return_value=mock_connection)
        context_manager.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = context_manager

        mock_connection.execute.return_value = mock_result

        driver = BaseDriver(sample_postgresql_datasource)

        mock_query = Mock()
        mock_query.sql.return_value = "SELECT COUNT(*) FROM orders"

        mock_check = Mock()
        mock_check.model_dump.return_value = {"name": "test_check"}

        with pytest.raises(Exception, match="Unexpected result executing check"):
            driver.execute_query(mock_query, mock_check, verbose=False)


class TestDriverFactory:
    """Test DriverFactory functionality."""

    def test_create_postgresql_driver(self):
        """Test factory creates PostgreSQL driver."""
        datasource = PostgreSQLDatasource(
            name="postgres_db",
            host="localhost",
            port=5432,
            db_name="test_db",
            user="user",
            password="pass",
        )

        with patch("weiser.drivers.base.create_engine"):
            driver = DriverFactory.create_driver(datasource)

            # Should return PostgresDriver (which inherits from BaseDriver)
            assert hasattr(driver, "engine")
            assert hasattr(driver, "data_source")

    def test_create_mysql_driver(self):
        """Test factory creates MySQL driver."""
        datasource = MySQLDatasource(
            name="mysql_db",
            host="localhost",
            port=3306,
            db_name="test_db",
            user="user",
            password="pass",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = DriverFactory.create_driver(datasource)

            assert isinstance(driver, MySQLDriver)
            # Should have called create_engine with mysql+pymysql URL
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert "mysql+pymysql://" in str(call_args)

    def test_create_snowflake_driver(self):
        """Test factory creates Snowflake driver."""
        datasource = SnowflakeDatasource(
            name="snowflake_db",
            account="test-account",
            db_name="test_db",
            user="user",
            password="pass",
            warehouse="COMPUTE_WH",
            role="ANALYST",
            schema_name="public",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = DriverFactory.create_driver(datasource)

            assert isinstance(driver, SnowflakeDriver)
            # Should have called create_engine with Snowflake URL
            mock_create_engine.assert_called_once()

    def test_create_databricks_driver(self):
        """Test factory creates Databricks driver."""
        datasource = DatabricksDatasource(
            name="databricks_db",
            host="workspace-123.cloud.databricks.com",
            access_token="dapi123456789abcdef",
            http_path="/sql/1.0/warehouses/abc123def456",
            catalog="main",
            schema_name="default",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = DriverFactory.create_driver(datasource)

            assert isinstance(driver, DatabricksDriver)
            # Should have called create_engine with Databricks URL
            mock_create_engine.assert_called_once()

    def test_create_bigquery_driver(self):
        """Test factory creates BigQuery driver."""
        datasource = BigQueryDatasource(
            name="bigquery_db",
            project_id="my-gcp-project",
            dataset_id="my_dataset",
            credentials_path="/path/to/credentials.json",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = DriverFactory.create_driver(datasource)

            assert isinstance(driver, BigQueryDriver)
            # Should have called create_engine with BigQuery URL
            mock_create_engine.assert_called_once()


class TestMySQLDriver:
    """Test MySQLDriver functionality."""

    def test_mysql_driver_initialization_with_components(self):
        """Test MySQLDriver initialization with individual components."""
        datasource = MySQLDatasource(
            name="mysql_db",
            host="localhost",
            port=3306,
            db_name="test_database",
            user="test_user",
            password="test_password",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = MySQLDriver(datasource)

            assert driver.data_source == datasource
            # Check that create_engine was called with mysql+pymysql URL
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert "mysql+pymysql://" in str(call_args)

    def test_mysql_driver_initialization_with_default_port(self):
        """Test MySQLDriver initialization uses default port 3306."""
        datasource = MySQLDatasource(
            name="mysql_db",
            host="localhost",
            db_name="test_database",
            user="test_user",
            password="test_password",
            # No port specified, should default to 3306
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = MySQLDriver(datasource)

            assert driver.data_source == datasource
            # Verify default port 3306 is used
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert ":3306/" in str(call_args)

    def test_mysql_driver_initialization_with_custom_port(self):
        """Test MySQLDriver initialization with custom port."""
        datasource = MySQLDatasource(
            name="mysql_db",
            host="localhost",
            port=3307,
            db_name="test_database",
            user="test_user",
            password="test_password",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = MySQLDriver(datasource)

            assert driver.data_source == datasource
            # Verify custom port is used
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert ":3307/" in str(call_args)

    def test_mysql_driver_initialization_with_uri(self):
        """Test MySQLDriver initialization with pre-configured URI."""
        datasource = MySQLDatasource(
            name="mysql_db",
            uri="mysql+pymysql://user:pass@localhost:3306/testdb",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = MySQLDriver(datasource)

            assert driver.data_source == datasource
            # Check that create_engine was called with the provided URI
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert str(call_args) == "mysql+pymysql://user:pass@localhost:3306/testdb"

    def test_mysql_driver_initialization_without_password(self):
        """Test MySQLDriver initialization without password."""
        datasource = MySQLDatasource(
            name="mysql_db",
            host="localhost",
            port=3306,
            db_name="test_database",
            user="test_user",
            # No password provided
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = MySQLDriver(datasource)

            assert driver.data_source == datasource
            # Check that create_engine was called with URL without password
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            url_str = str(call_args)
            assert "mysql+pymysql://" in url_str
            assert "test_user@" in url_str
            # Should not have password part
            assert ":@" not in url_str  # No password before @

    def test_mysql_driver_missing_required_fields_error(self):
        """Test MySQLDriver raises error when required fields are missing."""
        # This test will fail at model validation level since name is required
        with pytest.raises(ValidationError, match="Field required"):
            MySQLDatasource(
                host="localhost",
                port=3306,
                db_name="test_database",
                user="test_user",
                # Missing required name field
            )

    @patch("weiser.drivers.base.create_engine")
    def test_mysql_execute_query_success(self, mock_create_engine):
        """Test successful query execution with MySQL driver."""
        datasource = MySQLDatasource(
            name="mysql_db",
            host="localhost",
            port=3306,
            db_name="test_db",
            user="user",
            password="pass",
        )

        # Setup mocks
        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = [(100,), (200,)]

        mock_create_engine.return_value = mock_engine

        # Properly mock the context manager
        context_manager = MagicMock()
        context_manager.__enter__ = Mock(return_value=mock_connection)
        context_manager.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = context_manager

        mock_connection.execute.return_value = mock_result

        driver = MySQLDriver(datasource)

        # Create a mock query
        mock_query = Mock()
        mock_query.sql.return_value = "SELECT COUNT(*) FROM orders"

        mock_check = Mock()
        mock_check.model_dump.return_value = {"name": "test_check"}

        result = driver.execute_query(mock_query, mock_check, verbose=False)

        assert result == mock_result
        mock_connection.execute.assert_called_once()

    @patch("weiser.drivers.base.create_engine")
    def test_mysql_execute_query_with_verbose(self, mock_create_engine):
        """Test MySQL query execution with verbose logging."""
        datasource = MySQLDatasource(
            name="mysql_db",
            host="localhost",
            port=3306,
            db_name="test_db",
            user="user",
            password="pass",
        )

        # Setup mocks
        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = [(50,)]

        mock_create_engine.return_value = mock_engine

        # Properly mock the context manager
        context_manager = MagicMock()
        context_manager.__enter__ = Mock(return_value=mock_connection)
        context_manager.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = context_manager

        mock_connection.execute.return_value = mock_result

        driver = MySQLDriver(datasource)

        # Create a mock query
        mock_query = Mock()
        mock_query.sql.return_value = "SELECT COUNT(*) FROM products WHERE price > 100"

        mock_check = Mock()
        mock_check.model_dump.return_value = {"name": "test_check"}

        result = driver.execute_query(mock_query, mock_check, verbose=True)

        assert result == mock_result
        mock_connection.execute.assert_called_once()


class TestSnowflakeDriver:
    """Test SnowflakeDriver functionality."""

    def test_snowflake_driver_initialization_with_warehouse(self):
        """Test SnowflakeDriver initialization with warehouse and role."""
        datasource = SnowflakeDatasource(
            name="snowflake_db",
            account="test-account",
            db_name="test_database",
            user="test_user",
            password="test_password",
            warehouse="COMPUTE_WH",
            role="ANALYST",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = SnowflakeDriver(datasource)

            assert driver.data_source == datasource
            # Check that create_engine was called
            mock_create_engine.assert_called_once()

    def test_snowflake_driver_initialization_with_schema(self):
        """Test SnowflakeDriver initialization with schema."""
        datasource = SnowflakeDatasource(
            name="snowflake_db",
            account="test-account",
            db_name="test_database",
            user="test_user",
            password="test_password",
            warehouse="COMPUTE_WH",
            role="ANALYST",
            schema_name="public",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = SnowflakeDriver(datasource)

            # Verify engine was called
            mock_create_engine.assert_called_once()

    def test_snowflake_driver_initialization_with_uri(self):
        """Test SnowflakeDriver initialization with pre-configured URI."""
        datasource = SnowflakeDatasource(
            name="snowflake_db",
            account="dummy-account",  # Required field
            uri="snowflake://user:pass@account.snowflakecomputing.com/database?warehouse=WH&role=ROLE",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = SnowflakeDriver(datasource)

            assert driver.data_source == datasource
            # Check that create_engine was called with the provided URI
            mock_create_engine.assert_called_once()

    @patch("weiser.drivers.base.create_engine")
    def test_snowflake_execute_query_success(self, mock_create_engine):
        """Test successful query execution with Snowflake driver."""
        datasource = SnowflakeDatasource(
            name="snowflake_db",
            account="test-account",
            db_name="test_db",
            user="user",
            password="pass",
            warehouse="COMPUTE_WH",
            role="ANALYST",
        )

        # Setup mocks
        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = [(100,), (200,)]

        mock_create_engine.return_value = mock_engine

        # Properly mock the context manager
        context_manager = MagicMock()
        context_manager.__enter__ = Mock(return_value=mock_connection)
        context_manager.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = context_manager

        mock_connection.execute.return_value = mock_result

        driver = SnowflakeDriver(datasource)

        # Create a mock query
        mock_query = Mock()
        mock_query.sql.return_value = "SELECT COUNT(*) FROM orders"

        mock_check = Mock()
        mock_check.model_dump.return_value = {"name": "test_check"}

        result = driver.execute_query(mock_query, mock_check, verbose=False)

        assert result == mock_result
        mock_connection.execute.assert_called_once()


class TestDatabricksDriver:
    """Test DatabricksDriver functionality."""

    def test_databricks_driver_initialization_with_warehouse(self):
        """Test DatabricksDriver initialization with warehouse endpoint."""
        datasource = DatabricksDatasource(
            name="databricks_db",
            host="workspace-123.cloud.databricks.com",
            access_token="dapi123456789abcdef",
            http_path="/sql/1.0/warehouses/abc123def456",
            catalog="main",
            schema_name="default",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = DatabricksDriver(datasource)

            assert driver.data_source == datasource
            # Check that create_engine was called
            mock_create_engine.assert_called_once()

    def test_databricks_driver_initialization_with_cluster(self):
        """Test DatabricksDriver initialization with cluster endpoint."""
        datasource = DatabricksDatasource(
            name="databricks_db",
            host="workspace-456.cloud.databricks.com",
            access_token="dapi987654321fedcba",
            http_path="/sql/protocolv1/o/123456789/clusters/1234-567890-abc123",
            catalog="hive_metastore",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = DatabricksDriver(datasource)

            assert driver.data_source == datasource
            # Verify engine was called
            mock_create_engine.assert_called_once()

    def test_databricks_driver_initialization_with_uri(self):
        """Test DatabricksDriver initialization with pre-configured URI."""
        datasource = DatabricksDatasource(
            name="databricks_db",
            host="dummy-host",  # Required field
            http_path="dummy-path",  # Required field
            access_token="dummy-token",  # Required field
            uri="databricks://token:dapi123@workspace.databricks.com?http_path=/sql/warehouse/abc&catalog=main&schema=default",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = DatabricksDriver(datasource)

            assert driver.data_source == datasource
            # Check that create_engine was called with the provided URI
            mock_create_engine.assert_called_once()

    def test_databricks_driver_missing_host_error(self):
        """Test DatabricksDriver raises error when host is missing."""
        # This test will fail at model validation level now since host is required
        with pytest.raises(ValidationError, match="Field required"):
            DatabricksDatasource(
                name="databricks_db",
                access_token="dapi123456789abcdef",
                http_path="/sql/1.0/warehouses/abc123def456",
                # Missing required host field
            )

    def test_databricks_driver_missing_access_token_error(self):
        """Test DatabricksDriver raises error when access token is missing."""
        # This test will fail at model validation level now since access_token is required
        with pytest.raises(ValidationError, match="Field required"):
            DatabricksDatasource(
                name="databricks_db",
                host="workspace-123.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/abc123def456",
                # Missing required access_token field
            )

    def test_databricks_driver_missing_http_path_error(self):
        """Test DatabricksDriver raises error when HTTP path is missing."""
        # This test will fail at model validation level now since http_path is required
        with pytest.raises(ValidationError, match="Field required"):
            DatabricksDatasource(
                name="databricks_db",
                host="workspace-123.cloud.databricks.com",
                access_token="dapi123456789abcdef",
                # Missing required http_path field
            )

    @patch("weiser.drivers.base.create_engine")
    def test_databricks_execute_query_success(self, mock_create_engine):
        """Test successful query execution with Databricks driver."""
        datasource = DatabricksDatasource(
            name="databricks_db",
            host="workspace-123.cloud.databricks.com",
            access_token="dapi123456789abcdef",
            http_path="/sql/1.0/warehouses/abc123def456",
        )

        # Setup mocks
        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = [(100,), (200,)]

        mock_create_engine.return_value = mock_engine

        # Properly mock the context manager
        context_manager = MagicMock()
        context_manager.__enter__ = Mock(return_value=mock_connection)
        context_manager.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = context_manager

        mock_connection.execute.return_value = mock_result

        driver = DatabricksDriver(datasource)

        # Create a mock query
        mock_query = Mock()
        mock_query.sql.return_value = "SELECT COUNT(*) FROM orders"

        mock_check = Mock()
        mock_check.model_dump.return_value = {"name": "test_check"}

        result = driver.execute_query(mock_query, mock_check, verbose=False)

        assert result == mock_result
        mock_connection.execute.assert_called_once()


class TestBigQueryDriver:
    """Test BigQueryDriver functionality."""

    def test_bigquery_driver_initialization_with_project(self):
        """Test BigQueryDriver initialization with project and dataset."""
        datasource = BigQueryDatasource(
            name="bigquery_db",
            project_id="my-gcp-project",
            dataset_id="my_dataset",
            credentials_path="/path/to/credentials.json",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = BigQueryDriver(datasource)

            assert driver.data_source == datasource
            # Check that create_engine was called
            mock_create_engine.assert_called_once()

    def test_bigquery_driver_initialization_with_location(self):
        """Test BigQueryDriver initialization with location parameter."""
        datasource = BigQueryDatasource(
            name="bigquery_db",
            project_id="my-gcp-project",
            dataset_id="my_dataset",
            location="europe-west1",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = BigQueryDriver(datasource)

            assert driver.data_source == datasource
            # Verify engine was called
            mock_create_engine.assert_called_once()

    def test_bigquery_driver_initialization_with_uri(self):
        """Test BigQueryDriver initialization with pre-configured URI."""
        datasource = BigQueryDatasource(
            name="bigquery_db",
            project_id="dummy-project",  # Required field
            uri="bigquery://my-project/my-dataset?credentials_path=/path/to/creds.json",
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = BigQueryDriver(datasource)

            assert driver.data_source == datasource
            # Check that create_engine was called with the provided URI
            mock_create_engine.assert_called_once()

    def test_bigquery_driver_missing_project_error(self):
        """Test BigQueryDriver raises error when project_id is missing."""
        # This test will fail at model validation level now since project_id is required
        with pytest.raises(ValidationError):
            BigQueryDatasource(
                name="bigquery_db",
                dataset_id="my_dataset",
                # Missing required project_id field
            )

    def test_bigquery_driver_with_db_name_fallback(self):
        """Test BigQueryDriver works with minimal configuration."""
        datasource = BigQueryDatasource(
            name="bigquery_db",
            project_id="my-gcp-project",
            # No dataset_id, should work with just project_id
        )

        with patch("weiser.drivers.base.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = BigQueryDriver(datasource)

            assert driver.data_source == datasource
            mock_create_engine.assert_called_once()

    @patch("os.environ.setdefault")
    def test_bigquery_driver_sets_credentials_env_var(self, mock_setdefault):
        """Test BigQueryDriver sets GOOGLE_APPLICATION_CREDENTIALS environment variable."""
        datasource = BigQueryDatasource(
            name="bigquery_db",
            project_id="my-gcp-project",
            dataset_id="my_dataset",
            credentials_path="/path/to/credentials.json",
        )

        with (
            patch("weiser.drivers.base.create_engine") as mock_create_engine,
            patch("os.environ") as mock_environ,
        ):
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            driver = BigQueryDriver(datasource)

            # Should set the environment variable for credentials
            mock_environ.__setitem__.assert_called_with(
                "GOOGLE_APPLICATION_CREDENTIALS", "/path/to/credentials.json"
            )

    @patch("weiser.drivers.base.create_engine")
    def test_bigquery_execute_query_success(self, mock_create_engine):
        """Test successful query execution with BigQuery driver."""
        datasource = BigQueryDatasource(
            name="bigquery_db", project_id="my-gcp-project", dataset_id="my_dataset"
        )

        # Setup mocks
        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = [(100,), (200,)]

        mock_create_engine.return_value = mock_engine

        # Properly mock the context manager
        context_manager = MagicMock()
        context_manager.__enter__ = Mock(return_value=mock_connection)
        context_manager.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = context_manager

        mock_connection.execute.return_value = mock_result

        driver = BigQueryDriver(datasource)

        # Create a mock query
        mock_query = Mock()
        mock_query.sql.return_value = "SELECT COUNT(*) FROM orders"

        mock_check = Mock()
        mock_check.model_dump.return_value = {"name": "test_check"}

        result = driver.execute_query(mock_query, mock_check, verbose=False)

        assert result == mock_result
        mock_connection.execute.assert_called_once()


class TestDuckDBMetricStore:
    """Test DuckDBMetricStore functionality."""

    @patch("weiser.drivers.metric_stores.duckdb.create_engine")
    @patch("boto3.client")
    def test_duckdb_metric_store_initialization(
        self, mock_boto3_client, mock_create_engine
    ):
        """Test DuckDBMetricStore initialization."""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        config = MetricStore(
            db_type=MetricStoreType.duckdb,
            s3_access_key="test_key",
            s3_secret_access_key="test_secret",
            s3_region="us-east-1",
            s3_bucket="test-bucket",
        )

        store = DuckDBMetricStore(config)

        assert store.config == config
        assert store.db_name == "./metricstore.db"
        mock_boto3_client.assert_called_once()
        mock_create_engine.assert_called_with("duckdb:///./metricstore.db")

    @patch("weiser.drivers.metric_stores.duckdb.create_engine")
    @patch("boto3.client")
    def test_duckdb_metric_store_with_custom_db_name(
        self, mock_boto3_client, mock_create_engine
    ):
        """Test DuckDBMetricStore with custom database name."""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        config = MetricStore(
            db_type=MetricStoreType.duckdb,
            db_name="custom_metrics.db",
            s3_access_key="test_key",
            s3_secret_access_key="test_secret",
        )

        store = DuckDBMetricStore(config)

        assert store.db_name == "custom_metrics.db"
        mock_create_engine.assert_called_with("duckdb:///custom_metrics.db")

    @patch("weiser.drivers.metric_stores.duckdb.Session")
    @patch("weiser.drivers.metric_stores.duckdb.create_engine")
    @patch("boto3.client")
    def test_duckdb_metric_store_s3_path_style(
        self, mock_boto3_client, mock_create_engine, mock_session_class
    ):
        """Test DuckDBMetricStore with S3 path style configuration."""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.exec.return_value.first.return_value = (0,)  # For count queries

        config = MetricStore(
            db_type=MetricStoreType.duckdb,
            s3_url_style=S3UrlStyle.path,
            s3_endpoint="localhost:9000",
            s3_bucket="test-bucket",
            s3_access_key="test_key",
            s3_secret_access_key="test_secret",
        )
        # will call the DuckDBMetricStore constructor
        store = DuckDBMetricStore(config)  # ignore the warning about unused variable

        # Check that the path style was configured by verifying text() calls
        exec_calls = [call.args[0].text for call in mock_session.exec.call_args_list if hasattr(call.args[0], 'text')]
        assert any("SET s3_url_style='path'" in call for call in exec_calls)
        assert any("SET s3_endpoint = 'localhost:9000'" in call for call in exec_calls)

    @patch("weiser.drivers.metric_stores.duckdb.Session")
    @patch("weiser.drivers.metric_stores.duckdb.create_engine")
    @patch("boto3.client")
    def test_execute_query_success(self, mock_boto3_client, mock_create_engine, mock_session_class, sample_metric_store):
        """Test successful query execution on metric store."""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.exec.return_value.first.return_value = (0,)  # For count queries
        mock_session.exec.return_value = [(100.0, "2023-01-01")]  # For the actual query

        store = DuckDBMetricStore(sample_metric_store)

        mock_query = Mock()
        mock_query.sql.return_value = "SELECT actual_value, run_time FROM metrics"

        mock_check = Mock()
        mock_check.model_dump.return_value = {"name": "test_check"}

        result = store.execute_query(mock_query, mock_check, validate_results=True)

        assert result == [(100.0, "2023-01-01")]

    @patch("weiser.drivers.metric_stores.duckdb.Session")
    @patch("weiser.drivers.metric_stores.duckdb.create_engine")
    @patch("boto3.client")
    def test_execute_query_empty_result_with_validation(
        self, mock_boto3_client, mock_create_engine, mock_session_class, sample_metric_store
    ):
        """Test query execution with empty results and validation enabled."""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.exec.return_value.first.return_value = (0,)  # For count queries
        mock_session.exec.return_value = []  # Empty result for the actual query

        store = DuckDBMetricStore(sample_metric_store)

        mock_query = Mock()
        mock_query.sql.return_value = "SELECT * FROM empty_table"

        mock_check = Mock()
        mock_check.model_dump.return_value = {"name": "test_check"}

        with pytest.raises(Exception, match="Unexpected result executing check"):
            store.execute_query(
                mock_query, mock_check, validate_results=True
            )

    @patch("weiser.drivers.metric_stores.duckdb.Session")
    @patch("weiser.drivers.metric_stores.duckdb.create_engine")
    @patch("boto3.client")
    def test_insert_results_simple_threshold(
        self, mock_boto3_client, mock_create_engine, mock_session_class, sample_metric_store
    ):
        """Test inserting results with simple threshold."""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.exec.return_value.first.return_value = (0,)  # For count queries

        store = DuckDBMetricStore(sample_metric_store)

        record = {
            "actual_value": 100.0,
            "check_id": "test_check_id",
            "condition": "gt",
            "dataset": "orders",
            "datasource": "test_db",
            "fail": False,
            "name": "test_check",
            "run_id": "run_123",
            "run_time": "2023-01-01T00:00:00",
            "measure": "COUNT(*)",
            "success": True,
            "threshold": 50.0,
            "type": "row_count",
        }

        store.insert_results(record)

        # Verify that session.add was called (SQLModel inserts)
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

    @patch("weiser.drivers.metric_stores.duckdb.Session")
    @patch("weiser.drivers.metric_stores.duckdb.create_engine")
    @patch("boto3.client")
    def test_insert_results_list_threshold(
        self, mock_boto3_client, mock_create_engine, mock_session_class, sample_metric_store
    ):
        """Test inserting results with list threshold (between condition)."""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.exec.return_value.first.return_value = (0,)  # For count queries

        store = DuckDBMetricStore(sample_metric_store)

        record = {
            "actual_value": 1.5,
            "check_id": "test_anomaly_id",
            "condition": "between",
            "dataset": "metrics",
            "datasource": "test_db",
            "fail": False,
            "name": "test_anomaly",
            "run_id": "run_123",
            "run_time": "2023-01-01T00:00:00",
            "measure": "z_score",
            "success": True,
            "threshold": [-2.0, 2.0],
            "type": "anomaly",
        }

        store.insert_results(record)

        # Should convert list threshold to threshold_list and insert record
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

    @patch("weiser.drivers.metric_stores.duckdb.Session")
    @patch("weiser.drivers.metric_stores.duckdb.create_engine")
    @patch("boto3.client")
    def test_export_results_no_s3(self, mock_boto3_client, mock_create_engine, mock_session_class):
        """Test export results without S3 configuration."""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        
        # Create a default mock result for initialization queries
        default_result = Mock()
        default_result.first.return_value = (0,)
        default_result.fetchall.return_value = []
        default_result.fetchone.return_value = None
        
        # Mock exec to return default result for all calls
        mock_session.exec.return_value = default_result
        
        # Mock specific results for the export_results method
        summary_result = Mock()
        summary_result.first.return_value = (10, 8, 2)
        
        failures_result = Mock()  
        failures_result.all.return_value = [
            (
                "failed_check",
                "orders", 
                "test_db", 
                "check_id_123",
                "gt",
                5,
                10,
                "row_count",
            )
        ]

        config = MetricStore(
            db_type=MetricStoreType.duckdb,
            # No S3 configuration
        )

        store = DuckDBMetricStore(config)
        
        # Now override the exec mock for the export_results calls
        mock_session.exec.side_effect = [summary_result, failures_result]
        
        results = store.export_results("run_123")

        assert results["summary"]["total_checks"] == 10
        assert results["summary"]["passed_checks"] == 8
        assert results["summary"]["failed_checks"] == 2
        assert len(results["failures"]) == 1


class TestMetricStoreFactory:
    """Test MetricStoreFactory functionality."""

    def test_create_duckdb_metric_store(self):
        """Test factory creates DuckDBMetricStore."""
        config = MetricStore(
            db_type=MetricStoreType.duckdb,
            s3_access_key="test",
            s3_secret_access_key="test",
        )

        with patch("boto3.client"), patch("duckdb.connect") as mock_connect:
            mock_conn = Mock()
            mock_conn.sql.return_value.fetchall.return_value = [(None,)]
            mock_connect.return_value.__enter__.return_value = mock_conn

            store = MetricStoreFactory.create_driver(config)

            assert isinstance(store, DuckDBMetricStore)

    def test_create_postgres_metric_store(self):
        """Test factory creates PostgresMetricStore."""
        config = MetricStore(
            db_type=MetricStoreType.postgresql,
            host="localhost",
            port=5432,
            db_name="metrics",
            user="user",
            password="pass",
        )

        with patch(
            "weiser.drivers.metric_stores.postgres.create_engine"
        ) as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock the engine.connect() context manager used in PostgresMetricStore.__init__
            context_manager = MagicMock()
            mock_connection = Mock()
            context_manager.__enter__ = Mock(return_value=mock_connection)
            context_manager.__exit__ = Mock(return_value=None)
            mock_engine.connect.return_value = context_manager

            store = MetricStoreFactory.create_driver(config)

            assert isinstance(store, PostgresMetricStore)

    def test_create_default_metric_store(self):
        """Test factory creates default DuckDB store when no config provided."""
        with patch("boto3.client"), patch("duckdb.connect") as mock_connect:
            mock_conn = Mock()
            mock_conn.sql.return_value.fetchall.return_value = [(None,)]
            mock_connect.return_value.__enter__.return_value = mock_conn

            store = MetricStoreFactory.create_driver(None)

            assert isinstance(store, DuckDBMetricStore)

    def test_create_unsupported_metric_store_type(self):
        """Test factory falls back to DuckDB for unsupported types."""
        # Since Pydantic validates enum types, we need to test the factory logic differently
        # We'll test with a valid config but bypass the QUERY_TYPE_MAP lookup
        config = MetricStore(
            db_type=MetricStoreType.duckdb,  # Valid type
            s3_access_key="test",
            s3_secret_access_key="test",
        )

        with patch("boto3.client"), patch("duckdb.connect") as mock_connect:
            mock_conn = Mock()
            mock_conn.sql.return_value.fetchall.return_value = [(None,)]
            mock_connect.return_value.__enter__.return_value = mock_conn

            # Test that even with an invalid lookup, it falls back to DuckDB
            with patch.dict(
                "weiser.drivers.metric_stores.QUERY_TYPE_MAP", {}, clear=True
            ):
                store = MetricStoreFactory.create_driver(config)

                assert isinstance(store, DuckDBMetricStore)
