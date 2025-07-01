import pytest
from unittest.mock import Mock, patch, MagicMock
import boto3
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from weiser.drivers.base import BaseDriver, DIALECT_TYPE_MAP
from weiser.drivers import DriverFactory
from weiser.drivers.metric_stores import MetricStoreFactory, DuckDBMetricStore
from weiser.drivers.metric_stores.postgres import PostgresMetricStore
from weiser.loader.models import Datasource, MetricStore, DBType, MetricStoreType, S3UrlStyle

# Import fixtures
from tests.fixtures.config_fixtures import *


class TestBaseDriver:
    """Test BaseDriver functionality."""

    def test_base_driver_initialization_with_components(self):
        """Test BaseDriver initialization with individual components."""
        datasource = Datasource(
            name="test_db",
            type=DBType.postgresql,
            host="localhost",
            port=5432,
            db_name="test_database",
            user="test_user",
            password="test_password"
        )
        
        with patch('weiser.drivers.base.create_engine') as mock_create_engine:
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
        datasource = Datasource(
            name="test_db",
            type=DBType.postgresql,
            uri="postgresql://user:pass@localhost:5432/testdb"
        )
        
        with patch('weiser.drivers.base.create_engine') as mock_create_engine:
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
        datasource = Datasource(
            name="cube_db",
            type=DBType.cube,
            host="localhost",
            port=5432,
            db_name="cube_database",
            user="cube_user",
            password="cube_password"
        )
        
        with patch('weiser.drivers.base.create_engine') as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine
            
            driver = BaseDriver(datasource)
            
            # Should create URI with postgresql instead of cube
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert str(call_args).startswith('postgresql://')

    def test_dialect_mapping(self):
        """Test dialect mapping for different database types."""
        assert DIALECT_TYPE_MAP["postgresql"].__name__ == "Postgres"
        assert DIALECT_TYPE_MAP["mysql"].__name__ == "MySQL"
        assert DIALECT_TYPE_MAP["cube"].__name__ == "Postgres"

    @patch('weiser.drivers.base.create_engine')
    def test_execute_query_success(self, mock_create_engine, sample_datasource):
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
        
        driver = BaseDriver(sample_datasource)
        
        # Create a mock query
        mock_query = Mock()
        mock_query.sql.return_value = "SELECT COUNT(*) FROM orders"
        
        mock_check = Mock()
        mock_check.model_dump.return_value = {"name": "test_check"}
        
        result = driver.execute_query(mock_query, mock_check, verbose=False)
        
        assert result == mock_result
        mock_connection.execute.assert_called_once()

    @patch('weiser.drivers.base.create_engine')
    def test_execute_query_empty_result(self, mock_create_engine, sample_datasource):
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
        
        driver = BaseDriver(sample_datasource)
        
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
        datasource = Datasource(
            name="postgres_db",
            type=DBType.postgresql,
            host="localhost",
            port=5432,
            db_name="test_db",
            user="user",
            password="pass"
        )
        
        with patch('weiser.drivers.base.create_engine'):
            driver = DriverFactory.create_driver(datasource)
            
            # Should return PostgresDriver (which inherits from BaseDriver)
            assert hasattr(driver, 'engine')
            assert hasattr(driver, 'data_source')

    def test_create_driver_fallback_to_base(self):
        """Test factory falls back to BaseDriver for unsupported types."""
        datasource = Datasource(
            name="mysql_db",
            type=DBType.mysql,  # Not in DB_DRIVER_MAP, should use BaseDriver
            host="localhost",
            port=3306,
            db_name="test_db",
            user="user",
            password="pass"
        )
        
        with patch('weiser.drivers.base.create_engine') as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine
            
            driver = DriverFactory.create_driver(datasource)
            
            assert isinstance(driver, BaseDriver)
            # Should have called create_engine with mysql URL
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert "mysql://" in str(call_args)


class TestDuckDBMetricStore:
    """Test DuckDBMetricStore functionality."""

    @patch('duckdb.connect')
    @patch('boto3.client')
    def test_duckdb_metric_store_initialization(self, mock_boto3_client, mock_duckdb_connect):
        """Test DuckDBMetricStore initialization."""
        mock_conn = Mock()
        mock_duckdb_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.sql.return_value.fetchall.return_value = [(None,)]
        
        config = MetricStore(
            db_type=MetricStoreType.duckdb,
            s3_access_key="test_key",
            s3_secret_access_key="test_secret",
            s3_region="us-east-1",
            s3_bucket="test-bucket"
        )
        
        store = DuckDBMetricStore(config)
        
        assert store.config == config
        assert store.db_name == "./metricstore.db"
        mock_boto3_client.assert_called_once()
        mock_duckdb_connect.assert_called()

    @patch('duckdb.connect')
    @patch('boto3.client')
    def test_duckdb_metric_store_with_custom_db_name(self, mock_boto3_client, mock_duckdb_connect):
        """Test DuckDBMetricStore with custom database name."""
        mock_conn = Mock()
        mock_duckdb_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.sql.return_value.fetchall.return_value = [(None,)]
        
        config = MetricStore(
            db_type=MetricStoreType.duckdb,
            db_name="custom_metrics.db",
            s3_access_key="test_key",
            s3_secret_access_key="test_secret"
        )
        
        store = DuckDBMetricStore(config)
        
        assert store.db_name == "custom_metrics.db"

    @patch('duckdb.connect')
    @patch('boto3.client')
    def test_duckdb_metric_store_s3_path_style(self, mock_boto3_client, mock_duckdb_connect):
        """Test DuckDBMetricStore with S3 path style configuration."""
        mock_conn = Mock()
        mock_duckdb_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.sql.return_value.fetchall.return_value = [(None,)]
        
        config = MetricStore(
            db_type=MetricStoreType.duckdb,
            s3_url_style=S3UrlStyle.path,
            s3_endpoint="localhost:9000",
            s3_access_key="test_key",
            s3_secret_access_key="test_secret"
        )
        
        store = DuckDBMetricStore(config)
        
        # Check that the path style was configured
        mock_conn.sql.assert_any_call("SET s3_url_style='path'")
        mock_conn.sql.assert_any_call("SET s3_endpoint = 'localhost:9000'")

    @patch('duckdb.connect')
    def test_execute_query_success(self, mock_duckdb_connect, sample_metric_store):
        """Test successful query execution on metric store."""
        mock_conn = Mock()
        mock_duckdb_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.sql.return_value.fetchall.return_value = [(100.0, "2023-01-01")]
        
        with patch('boto3.client'):
            # Mock the constructor call
            mock_conn_init = Mock()
            mock_conn_init.sql.return_value.fetchall.return_value = [(None,)]
            mock_duckdb_connect.return_value.__enter__.return_value = mock_conn_init
            
            store = DuckDBMetricStore(sample_metric_store)
            
            # Reset mock for actual test
            mock_duckdb_connect.return_value.__enter__.return_value = mock_conn
            
            mock_query = Mock()
            mock_query.sql.return_value = "SELECT actual_value, run_time FROM metrics"
            
            mock_check = Mock()
            mock_check.model_dump.return_value = {"name": "test_check"}
            
            result = store.execute_query(mock_query, mock_check, verbose=False)
            
            assert result == [(100.0, "2023-01-01")]

    @patch('duckdb.connect')
    def test_execute_query_empty_result_with_validation(self, mock_duckdb_connect, sample_metric_store):
        """Test query execution with empty results and validation enabled."""
        mock_conn = Mock()
        mock_duckdb_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.sql.return_value.fetchall.return_value = []
        
        with patch('boto3.client'):
            # Mock the constructor
            mock_conn_init = Mock()
            mock_conn_init.sql.return_value.fetchall.return_value = [(None,)]
            mock_duckdb_connect.return_value.__enter__.return_value = mock_conn_init
            
            store = DuckDBMetricStore(sample_metric_store)
            
            # Reset for test
            mock_duckdb_connect.return_value.__enter__.return_value = mock_conn
            
            mock_query = Mock()
            mock_query.sql.return_value = "SELECT * FROM empty_table"
            
            mock_check = Mock()
            mock_check.model_dump.return_value = {"name": "test_check"}
            
            with pytest.raises(Exception, match="Unexpected result executing check"):
                store.execute_query(mock_query, mock_check, verbose=False, validate_results=True)

    @patch('duckdb.connect')
    def test_insert_results_simple_threshold(self, mock_duckdb_connect, sample_metric_store):
        """Test inserting results with simple threshold."""
        mock_conn = Mock()
        mock_duckdb_connect.return_value.__enter__.return_value = mock_conn
        
        with patch('boto3.client'):
            # Mock constructor
            mock_conn_init = Mock()
            mock_conn_init.sql.return_value.fetchall.return_value = [(None,)]
            mock_duckdb_connect.return_value.__enter__.return_value = mock_conn_init
            
            store = DuckDBMetricStore(sample_metric_store)
            
            # Reset for test
            mock_duckdb_connect.return_value.__enter__.return_value = mock_conn
            
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
                "type": "row_count"
            }
            
            store.insert_results(record)
            
            mock_conn.sql.assert_called()

    @patch('duckdb.connect')
    def test_insert_results_list_threshold(self, mock_duckdb_connect, sample_metric_store):
        """Test inserting results with list threshold (between condition)."""
        mock_conn = Mock()
        mock_duckdb_connect.return_value.__enter__.return_value = mock_conn
        
        with patch('boto3.client'):
            # Mock constructor
            mock_conn_init = Mock()
            mock_conn_init.sql.return_value.fetchall.return_value = [(None,)]
            mock_duckdb_connect.return_value.__enter__.return_value = mock_conn_init
            
            store = DuckDBMetricStore(sample_metric_store)
            
            # Reset for test  
            mock_duckdb_connect.return_value.__enter__.return_value = mock_conn
            
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
                "type": "anomaly"
            }
            
            store.insert_results(record)
            
            # Should convert list threshold to threshold_list
            mock_conn.sql.assert_called()

    @patch('boto3.client')
    @patch('duckdb.connect')
    def test_export_results_no_s3(self, mock_duckdb_connect, mock_boto3_client):
        """Test export results without S3 configuration."""
        mock_conn = Mock()
        mock_duckdb_connect.return_value.__enter__.return_value = mock_conn
        
        # Mock summary results - need separate mock calls for fetchone and fetchall
        summary_mock = Mock()
        summary_mock.fetchone.return_value = (10, 8, 2)  # total, passed, failed
        
        failures_mock = Mock()
        failures_mock.fetchall.return_value = [
            ("failed_check", "orders", "test_db", "check_id_123", "gt", 5, 10, "row_count")
        ]
        
        # Mock different SQL calls
        mock_conn.sql.side_effect = [summary_mock, failures_mock]
        
        config = MetricStore(
            db_type=MetricStoreType.duckdb,
            # No S3 configuration
        )
        
        # Mock constructor call
        mock_conn_init = Mock()
        mock_conn_init.sql.return_value.fetchall.return_value = [(None,)]
        mock_duckdb_connect.return_value.__enter__.return_value = mock_conn_init
        
        store = DuckDBMetricStore(config)
        
        # Reset for test
        mock_duckdb_connect.return_value.__enter__.return_value = mock_conn
        
        results = store.export_results("run_123")
        
        assert results['summary']['total_checks'] == 10
        assert results['summary']['passed_checks'] == 8
        assert results['summary']['failed_checks'] == 2
        assert len(results['failures']) == 1


class TestMetricStoreFactory:
    """Test MetricStoreFactory functionality."""

    def test_create_duckdb_metric_store(self):
        """Test factory creates DuckDBMetricStore."""
        config = MetricStore(
            db_type=MetricStoreType.duckdb,
            s3_access_key="test",
            s3_secret_access_key="test"
        )
        
        with patch('boto3.client'), patch('duckdb.connect') as mock_connect:
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
            password="pass"
        )
        
        with patch('weiser.drivers.metric_stores.postgres.create_engine') as mock_create_engine:
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
        with patch('boto3.client'), patch('duckdb.connect') as mock_connect:
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
            s3_secret_access_key="test"
        )
        
        with patch('boto3.client'), patch('duckdb.connect') as mock_connect:
            mock_conn = Mock()
            mock_conn.sql.return_value.fetchall.return_value = [(None,)]
            mock_connect.return_value.__enter__.return_value = mock_conn
            
            # Test that even with an invalid lookup, it falls back to DuckDB
            with patch.dict('weiser.drivers.metric_stores.QUERY_TYPE_MAP', {}, clear=True):
                store = MetricStoreFactory.create_driver(config)
                
                assert isinstance(store, DuckDBMetricStore)