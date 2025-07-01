import pytest
from datetime import datetime
from typing import Dict, Any
from weiser.loader.models import (
    BaseConfig, 
    Check, 
    Datasource, 
    MetricStore,
    CheckType,
    Condition,
    DBType,
    MetricStoreType,
    TimeDimension,
    Granularity
)


@pytest.fixture
def sample_datasource():
    """Sample PostgreSQL datasource for testing."""
    return Datasource(
        name="test_db",
        type=DBType.postgresql,
        host="localhost",
        port=5432,
        db_name="test_database",
        user="test_user",
        password="test_password"
    )


@pytest.fixture
def sample_metric_store():
    """Sample DuckDB metric store for testing."""
    return MetricStore(
        name="test_metrics",
        db_type=MetricStoreType.duckdb,
        db_name="test_metrics.db"
    )


@pytest.fixture
def sample_row_count_check():
    """Sample row count check for testing."""
    return Check(
        name="test_row_count",
        dataset="orders",
        type=CheckType.row_count,
        condition=Condition.gt,
        threshold=0
    )


@pytest.fixture
def sample_numeric_check():
    """Sample numeric check for testing."""
    return Check(
        name="test_numeric",
        dataset="orders",
        type=CheckType.numeric,
        measure="sum(amount)",
        condition=Condition.gt,
        threshold=1000.0
    )


@pytest.fixture
def sample_anomaly_check():
    """Sample anomaly detection check for testing."""
    return Check(
        name="test_anomaly",
        dataset="metrics", 
        type=CheckType.anomaly,
        check_id="test_check_id_123",
        condition=Condition.between,
        threshold=[-2.0, 2.0]
    )


@pytest.fixture
def sample_check_with_dimensions():
    """Sample check with dimensions for testing."""
    return Check(
        name="test_with_dimensions",
        dataset="orders",
        type=CheckType.row_count,
        dimensions=["status", "region"],
        condition=Condition.gt,
        threshold=0
    )


@pytest.fixture
def sample_check_with_time_dimension():
    """Sample check with time dimension for testing."""
    return Check(
        name="test_with_time",
        dataset="orders",
        type=CheckType.sum,
        measure="amount",
        time_dimension=TimeDimension(
            name="created_at",
            granularity=Granularity.day
        ),
        condition=Condition.gt,
        threshold=0
    )


@pytest.fixture
def sample_check_with_filter():
    """Sample check with filter for testing."""
    return Check(
        name="test_with_filter",
        dataset="orders",
        type=CheckType.numeric,
        measure="sum(amount)",
        filter="status = 'completed'",
        condition=Condition.gt,
        threshold=500.0
    )


@pytest.fixture
def sample_base_config(sample_datasource, sample_metric_store, sample_row_count_check, sample_numeric_check):
    """Sample base configuration for testing."""
    return BaseConfig(
        checks=[sample_row_count_check, sample_numeric_check],
        datasources=[sample_datasource],
        connections=[sample_metric_store]
    )


@pytest.fixture
def sample_yaml_config():
    """Sample YAML configuration string for testing."""
    return """
version: 1
datasources:
  - name: test_db
    type: postgresql
    host: localhost
    port: 5432
    db_name: test_database
    user: test_user
    password: test_password

checks:
  - name: test row_count
    dataset: orders
    type: row_count
    condition: gt
    threshold: 0
    
  - name: test numeric
    dataset: orders
    type: numeric
    measure: sum(amount)
    condition: gt
    threshold: 1000

connections:
  - name: test_metrics
    type: metricstore
    db_type: duckdb
    db_name: test_metrics.db
"""


@pytest.fixture
def sample_yaml_config_with_templating():
    """Sample YAML configuration with Jinja2 templating for testing."""
    return """
version: 1
datasources:
  - name: default
    type: postgresql
    host: {{DB_HOST}}
    port: {{DB_PORT}}
    db_name: {{DB_NAME}}
    user: {{DB_USER}}
    password: {{DB_PASSWORD}}

checks:
  - name: test row_count
    dataset: orders
    type: row_count
    condition: gt
    threshold: 0

connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
    db_name: metrics.db
"""


@pytest.fixture
def sample_environment_variables():
    """Sample environment variables for template testing."""
    return {
        "DB_HOST": "localhost",
        "DB_PORT": "5432", 
        "DB_NAME": "test_db",
        "DB_USER": "test_user",
        "DB_PASSWORD": "test_pass"
    }


@pytest.fixture
def sample_check_results():
    """Sample check results for testing."""
    return [
        {
            "check_id": "test_check_id_123",
            "name": "test_row_count",
            "type": "row_count",
            "datasource": "test_db",
            "dataset": "orders",
            "actual_value": 100,
            "threshold": 0,
            "condition": "gt",
            "success": True,
            "fail": False,
            "run_id": "test_run_123",
            "run_time": datetime.now().isoformat()
        }
    ]