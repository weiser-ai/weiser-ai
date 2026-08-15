import pytest
import tempfile
import os
from datetime import datetime
from unittest.mock import AsyncMock, Mock, MagicMock
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from weiser.drivers.base import BaseDriver
from weiser.drivers.metric_stores.duckdb import DuckDBMetricStore
from weiser.loader.models import Datasource, MetricStore, DBType, MetricStoreType
from weiser.evals.adapters.base import AgentAdapter
from weiser.evals.models import AgentTrace
from weiser.evals.semantic_layer.base import SchemaCatalog, SchemaView, SemanticLayerAdapter


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


@pytest.fixture
def mock_semantic_layer():
    """Mock SemanticLayerAdapter returning a canned single-view schema catalog."""
    adapter = Mock(spec=SemanticLayerAdapter)
    catalog = SchemaCatalog(
        views={"merchants": SchemaView(name="merchants", members={"id", "name", "country"})},
        has_semantics=False,
        fetched_at=datetime.now(),
    )
    adapter.get_schema = Mock(return_value=catalog)
    adapter.execute_query = Mock(return_value=[{"cnt": 3}])
    adapter.get_freshness = Mock(return_value=None)
    return adapter


@pytest.fixture
def mock_agent_adapter():
    """Mock AgentAdapter returning a canned, correct AgentTrace regardless of input."""
    adapter = Mock(spec=AgentAdapter)
    adapter.build = Mock(return_value=object())
    adapter.run = AsyncMock(
        return_value=AgentTrace(
            question="How many merchants are there?",
            tool_calls=[],
            predicted_sqls=["SELECT COUNT(*) AS cnt FROM merchants"],
            final_answer="There are 3 merchants.",
            query_results=[{"cnt": 3}],
            hit_limit=False,
            elapsed_s=0.1,
            cost_usd=0.01,
            prompt_tokens=100,
            completion_tokens=20,
        )
    )
    return adapter


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