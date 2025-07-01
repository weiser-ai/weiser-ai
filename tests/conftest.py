import pytest
import tempfile
import os
from unittest.mock import Mock, MagicMock
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from weiser.drivers.base import BaseDriver
from weiser.drivers.metric_stores.duckdb import DuckDBMetricStore
from weiser.loader.models import Datasource, MetricStore, DBType, MetricStoreType


@pytest.fixture
def temp_yaml_file():
    """Creates a temporary YAML file for testing configuration loading."""
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
    yield temp_file.name
    os.unlink(temp_file.name)


@pytest.fixture
def mock_engine():
    """Mock SQLAlchemy engine for testing."""
    engine = Mock(spec=Engine)
    connection = Mock()
    context_manager = MagicMock()
    context_manager.__enter__ = Mock(return_value=connection)
    context_manager.__exit__ = Mock(return_value=None)
    engine.connect.return_value = context_manager
    return engine


@pytest.fixture
def mock_driver(mock_engine):
    """Mock database driver for testing."""
    driver = Mock(spec=BaseDriver)
    driver.engine = mock_engine
    driver.execute_query = Mock(return_value=[(100,)])
    return driver


@pytest.fixture
def mock_metric_store():
    """Mock metric store for testing."""
    metric_store = Mock(spec=DuckDBMetricStore)
    metric_store.insert_results = Mock()
    metric_store.get_connection = Mock()
    return metric_store


@pytest.fixture
def sample_database_rows():
    """Sample database query results for testing."""
    return [
        (100,),  # Simple count result
        (1500.50,),  # Numeric result
        ('active', 50),  # Grouped result
        ('inactive', 25),  # Grouped result
    ]


@pytest.fixture
def sample_anomaly_data():
    """Sample anomaly detection data for testing."""
    return [
        (100.0, 0.5),   # value, z_score
        (150.0, 1.2),   # value, z_score  
        (200.0, 2.8),   # value, z_score (anomaly)
        (80.0, -0.3),   # value, z_score
        (90.0, 0.1),    # value, z_score
    ]


@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch):
    """Set up test environment variables."""
    test_env = {
        'DB_HOST': 'localhost',
        'DB_PORT': '5432',
        'DB_NAME': 'test_db',
        'DB_USER': 'test_user', 
        'DB_PASSWORD': 'test_pass',
        'CUBEJS_SQL_HOST': 'localhost',
        'CUBEJS_SQL_DB_NAME': 'test_cube',
        'CUBEJS_SQL_USER': 'cube_user',
        'CUBEJS_SQL_PASSWORD': 'cube_pass'
    }
    
    for key, value in test_env.items():
        monkeypatch.setenv(key, value)