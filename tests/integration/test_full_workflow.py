import pytest
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy import create_engine, text

from weiser.loader.config import load_config
from weiser.runner import pre_run_config, run_checks
from weiser.loader.models import BaseConfig


@pytest.mark.integration
class TestFullWorkflow:
    """Integration tests for the complete Weiser workflow."""

    def test_end_to_end_workflow_mock_database(self, temp_yaml_file):
        """Test complete workflow from config loading to check execution with mock database."""
        # Create a complete YAML configuration
        config_content = """
version: 1
datasources:
  - name: test_postgres
    type: postgresql
    host: localhost
    port: 5432
    db_name: test_database
    user: test_user
    password: test_password

checks:
  - name: orders_row_count
    dataset: orders
    type: row_count
    datasource: test_postgres
    condition: gt
    threshold: 0
    
  - name: orders_total_amount
    dataset: orders
    type: numeric
    measure: sum(amount)
    datasource: test_postgres
    condition: gt
    threshold: 1000
    
  - name: orders_by_status
    dataset: orders
    type: row_count
    dimensions:
      - status
    datasource: test_postgres
    condition: gt
    threshold: 0

connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
    db_name: test_metrics.db
"""
        
        # Write config to temp file
        with open(temp_yaml_file, 'w') as f:
            f.write(config_content)
        
        # Mock database responses
        mock_db_responses = {
            'orders_row_count': [(100,)],  # Row count result
            'orders_total_amount': [(5000.50,)],  # Sum result
            'orders_by_status': [('active', 60), ('inactive', 40)]  # Grouped result
        }
        
        # Mock SQLAlchemy engine and connection
        mock_engine = Mock()
        mock_connection = Mock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        
        def mock_execute(query):
            sql_str = str(query)
            if 'count(*)' in sql_str.lower():
                if 'group by' in sql_str.lower():
                    return mock_db_responses['orders_by_status']
                else:
                    return mock_db_responses['orders_row_count']
            elif 'sum(amount)' in sql_str.lower():
                return mock_db_responses['orders_total_amount']
            else:
                return [(1,)]  # Default response for connection test
        
        mock_connection.execute = mock_execute
        
        # Mock DuckDB metric store
        mock_metric_store = Mock()
        mock_metric_store.insert_results = Mock()
        
        with patch('sqlalchemy.create_engine', return_value=mock_engine):
            with patch('weiser.drivers.metric_stores.MetricStoreFactory.create_driver', return_value=mock_metric_store):
                with patch('boto3.client'):  # Mock boto3 for DuckDB metric store
                    with patch('duckdb.connect') as mock_duckdb:
                        mock_conn = Mock()
                        mock_conn.sql.return_value.fetchall.return_value = [(None,)]
                        mock_duckdb.return_value.__enter__.return_value = mock_conn
                        
                        # Load configuration
                        config = load_config(temp_yaml_file, verbose=False)
                        
                        # Pre-run configuration
                        context = pre_run_config(config, verbose=False)
                        
                        # Run checks
                        results = run_checks(
                            context["run_id"],
                            context["config"],
                            context["connections"],
                            context["metric_store"],
                            verbose=False
                        )
                        
                        # Verify results
                        assert len(results) == 3  # Three checks defined
                        
                        # Check that all checks succeeded
                        for result in results:
                            assert "check_instance" in result
                            assert "results" in result
                            assert len(result["results"]) > 0
                            
                            for check_result in result["results"]:
                                assert check_result["success"] == True
                                assert "actual_value" in check_result
                                assert "check_id" in check_result
                        
                        # Verify metric store was called to insert results
                        assert mock_metric_store.insert_results.call_count >= 3

    def test_workflow_with_failing_checks(self, temp_yaml_file):
        """Test workflow with checks that should fail."""
        config_content = """
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
  - name: failing_row_count
    dataset: empty_table
    type: row_count
    datasource: test_db
    condition: gt
    threshold: 100  # This will fail if table has < 100 rows
    
  - name: failing_sum
    dataset: orders
    type: numeric
    measure: sum(amount)
    datasource: test_db
    condition: gt
    threshold: 10000  # This will fail if sum < 10000

connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
"""
        
        with open(temp_yaml_file, 'w') as f:
            f.write(config_content)
        
        # Mock database to return values that will fail checks
        mock_engine = Mock()
        mock_connection = Mock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        
        def mock_execute(query):
            sql_str = str(query)
            if 'count(*)' in sql_str.lower():
                return [(50,)]  # Less than threshold of 100
            elif 'sum(amount)' in sql_str.lower():
                return [(5000.0,)]  # Less than threshold of 10000
            else:
                return [(1,)]
        
        mock_connection.execute = mock_execute
        
        mock_metric_store = Mock()
        
        with patch('sqlalchemy.create_engine', return_value=mock_engine):
            with patch('weiser.drivers.metric_stores.MetricStoreFactory.create_driver', return_value=mock_metric_store):
                with patch('boto3.client'):
                    with patch('duckdb.connect') as mock_duckdb:
                        mock_conn = Mock()
                        mock_conn.sql.return_value.fetchall.return_value = [(None,)]
                        mock_duckdb.return_value.__enter__.return_value = mock_conn
                        
                        config = load_config(temp_yaml_file, verbose=False)
                        context = pre_run_config(config, verbose=False)
                        results = run_checks(
                            context["run_id"],
                            context["config"],
                            context["connections"],
                            context["metric_store"],
                            verbose=False
                        )
                        
                        # Verify that checks failed
                        assert len(results) == 2
                        
                        for result in results:
                            for check_result in result["results"]:
                                assert check_result["success"] == False
                                assert check_result["fail"] == True

    def test_workflow_with_filters_and_dimensions(self, temp_yaml_file):
        """Test workflow with filtered and dimensioned checks."""
        config_content = """
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
  - name: active_orders_count
    dataset: orders
    type: row_count
    datasource: test_db
    filter: status = 'active'
    condition: gt
    threshold: 10
    
  - name: orders_by_region_status
    dataset: orders
    type: row_count
    dimensions:
      - region
      - status
    datasource: test_db
    condition: gt
    threshold: 0

connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
"""
        
        with open(temp_yaml_file, 'w') as f:
            f.write(config_content)
        
        mock_engine = Mock()
        mock_connection = Mock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        
        def mock_execute(query):
            sql_str = str(query)
            if 'group by' in sql_str.lower():
                # Simulated grouped results for region and status
                return [
                    ('US', 'active', 25),
                    ('US', 'inactive', 15),
                    ('EU', 'active', 30),
                    ('EU', 'inactive', 10)
                ]
            elif "status = 'active'" in sql_str:
                return [(45,)]  # Filtered count
            else:
                return [(1,)]
        
        mock_connection.execute = mock_execute
        
        mock_metric_store = Mock()
        
        with patch('sqlalchemy.create_engine', return_value=mock_engine):
            with patch('weiser.drivers.metric_stores.MetricStoreFactory.create_driver', return_value=mock_metric_store):
                with patch('boto3.client'):
                    with patch('duckdb.connect') as mock_duckdb:
                        mock_conn = Mock()
                        mock_conn.sql.return_value.fetchall.return_value = [(None,)]
                        mock_duckdb.return_value.__enter__.return_value = mock_conn
                        
                        config = load_config(temp_yaml_file, verbose=False)
                        context = pre_run_config(config, verbose=False)
                        results = run_checks(
                            context["run_id"],
                            context["config"],
                            context["connections"],
                            context["metric_store"],
                            verbose=False
                        )
                        
                        # First check should have one result (filtered)
                        active_orders_results = [r for r in results if r["check_instance"] == "active_orders_count"]
                        assert len(active_orders_results) == 1
                        assert len(active_orders_results[0]["results"]) == 1
                        
                        # Second check should have multiple results (grouped by region and status)
                        grouped_results = [r for r in results if r["check_instance"] == "orders_by_region_status"]
                        assert len(grouped_results) == 1
                        assert len(grouped_results[0]["results"]) == 4  # 2 regions × 2 statuses

    def test_workflow_with_custom_sql_dataset(self, temp_yaml_file):
        """Test workflow with custom SQL as dataset."""
        config_content = """
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
  - name: custom_sql_check
    dataset: >
      SELECT * FROM orders o 
      LEFT JOIN customers c ON o.customer_id = c.id 
      WHERE o.created_at >= '2023-01-01'
    type: numeric
    measure: sum(o.amount)
    datasource: test_db
    condition: gt
    threshold: 500

connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
"""
        
        with open(temp_yaml_file, 'w') as f:
            f.write(config_content)
        
        mock_engine = Mock()
        mock_connection = Mock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = [(2500.75,)]  # Sum result
        
        mock_metric_store = Mock()
        
        with patch('sqlalchemy.create_engine', return_value=mock_engine):
            with patch('weiser.drivers.metric_stores.MetricStoreFactory.create_driver', return_value=mock_metric_store):
                with patch('boto3.client'):
                    with patch('duckdb.connect') as mock_duckdb:
                        mock_conn = Mock()
                        mock_conn.sql.return_value.fetchall.return_value = [(None,)]
                        mock_duckdb.return_value.__enter__.return_value = mock_conn
                        
                        config = load_config(temp_yaml_file, verbose=False)
                        context = pre_run_config(config, verbose=False)
                        results = run_checks(
                            context["run_id"],
                            context["config"],
                            context["connections"],
                            context["metric_store"],
                            verbose=False
                        )
                        
                        assert len(results) == 1
                        check_result = results[0]["results"][0]
                        assert check_result["success"] == True
                        assert check_result["actual_value"] == 2500.75

    def test_workflow_multiple_datasources(self, temp_yaml_file):
        """Test workflow with checks spanning multiple datasources."""
        config_content = """
version: 1
datasources:
  - name: postgres_db
    type: postgresql
    host: localhost
    port: 5432
    db_name: postgres_db
    user: postgres_user
    password: postgres_pass
    
  - name: mysql_db  
    type: mysql
    host: localhost
    port: 3306
    db_name: mysql_db
    user: mysql_user
    password: mysql_pass

checks:
  - name: cross_db_check
    dataset: orders
    type: row_count
    datasource: [postgres_db, mysql_db]  # Same check on multiple DBs
    condition: gt
    threshold: 0

connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
"""
        
        with open(temp_yaml_file, 'w') as f:
            f.write(config_content)
        
        # Create separate mock engines for different databases
        mock_postgres_engine = Mock()
        mock_mysql_engine = Mock()
        mock_postgres_conn = Mock()
        mock_mysql_conn = Mock()
        
        mock_postgres_engine.connect.return_value.__enter__.return_value = mock_postgres_conn
        mock_mysql_engine.connect.return_value.__enter__.return_value = mock_mysql_conn
        
        mock_postgres_conn.execute.return_value = [(150,)]  # Postgres result
        mock_mysql_conn.execute.return_value = [(200,)]  # MySQL result
        
        mock_metric_store = Mock()
        
        def mock_create_engine(url_or_string, **kwargs):
            if 'postgresql' in str(url_or_string):
                return mock_postgres_engine
            elif 'mysql' in str(url_or_string):
                return mock_mysql_engine
            else:
                return Mock()
        
        with patch('sqlalchemy.create_engine', side_effect=mock_create_engine):
            with patch('weiser.drivers.metric_stores.MetricStoreFactory.create_driver', return_value=mock_metric_store):
                with patch('boto3.client'):
                    with patch('duckdb.connect') as mock_duckdb:
                        mock_conn = Mock()
                        mock_conn.sql.return_value.fetchall.return_value = [(None,)]
                        mock_duckdb.return_value.__enter__.return_value = mock_conn
                        
                        config = load_config(temp_yaml_file, verbose=False)
                        context = pre_run_config(config, verbose=False)
                        results = run_checks(
                            context["run_id"],
                            context["config"],
                            context["connections"],
                            context["metric_store"],
                            verbose=False
                        )
                        
                        # Should have 2 results (one per datasource)
                        assert len(results) == 2
                        
                        # Both should succeed with different values
                        actual_values = [r["results"][0]["actual_value"] for r in results]
                        assert 150 in actual_values
                        assert 200 in actual_values