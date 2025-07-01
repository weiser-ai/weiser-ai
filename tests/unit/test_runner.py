import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import uuid

from weiser.runner import run_checks, generate_sample_data, pre_run_config
from weiser.loader.models import BaseConfig, Check, Datasource, MetricStore, CheckType, Condition, ConnectionType

# Import fixtures
from tests.fixtures.config_fixtures import *


class TestPreRunConfig:
    """Test pre_run_config functionality."""

    def test_pre_run_config_compile_only(self, sample_base_config):
        """Test pre_run_config in compile-only mode."""
        config_dict = sample_base_config.model_dump()
        
        with patch('weiser.drivers.metric_stores.MetricStoreFactory.create_driver') as mock_metric_factory:
            mock_metric_store = Mock()
            mock_metric_factory.return_value = mock_metric_store
            
            context = pre_run_config(config_dict, compile_only=True, verbose=False)
            
            assert "config" in context
            assert "connections" in context
            assert "metric_store" in context
            assert "run_id" in context
            assert "run_ts" in context
            assert isinstance(context["config"], BaseConfig)
            assert context["connections"] == {}  # Empty in compile mode

    def test_pre_run_config_full_mode(self, sample_base_config):
        """Test pre_run_config in full execution mode."""
        config_dict = sample_base_config.model_dump()
        
        with patch('weiser.drivers.metric_stores.MetricStoreFactory.create_driver') as mock_metric_factory:
            mock_metric_store = Mock()
            mock_metric_factory.return_value = mock_metric_store
            
            with patch('weiser.drivers.DriverFactory.create_driver') as mock_driver_factory:
                mock_driver = Mock()
                mock_engine = Mock()
                mock_connection = Mock()
                
                mock_driver.engine = mock_engine
                # Properly mock the context manager
                context_manager = MagicMock()
                context_manager.__enter__ = Mock(return_value=mock_connection)
                context_manager.__exit__ = Mock(return_value=None)
                mock_engine.connect.return_value = context_manager
                mock_connection.execute.return_value = [(1,)]  # SELECT 1 result
                
                mock_driver_factory.return_value = mock_driver
                
                context = pre_run_config(config_dict, compile_only=False, verbose=False)
                
                assert "config" in context
                assert "connections" in context
                assert "metric_store" in context
                assert "run_id" in context
                assert "run_ts" in context
                assert len(context["connections"]) > 0
                
                # Should test database connections
                mock_connection.execute.assert_called()

    def test_pre_run_config_with_metric_store_connection(self):
        """Test pre_run_config finds metric store connection."""
        config_dict = {
            "checks": [{
                "name": "test_check",
                "dataset": "orders",
                "type": "row_count",
                "condition": "gt",
                "threshold": 0
            }],
            "datasources": [{
                "name": "test_db",
                "type": "postgresql",
                "host": "localhost"
            }],
            "connections": [{
                "name": "metricstore",
                "type": "metricstore",
                "db_type": "duckdb"
            }]
        }
        
        with patch('weiser.drivers.metric_stores.MetricStoreFactory.create_driver') as mock_metric_factory:
            mock_metric_store = Mock()
            mock_metric_factory.return_value = mock_metric_store
            
            context = pre_run_config(config_dict, compile_only=True, verbose=False)
            
            mock_metric_factory.assert_called_once()

    def test_pre_run_config_run_id_generation(self, sample_base_config):
        """Test that pre_run_config generates valid run IDs."""
        config_dict = sample_base_config.model_dump()
        
        with patch('weiser.drivers.metric_stores.MetricStoreFactory.create_driver'):
            context1 = pre_run_config(config_dict, compile_only=True, verbose=False)
            context2 = pre_run_config(config_dict, compile_only=True, verbose=False)
            
            # Run IDs should be different UUIDs
            assert context1["run_id"] != context2["run_id"]
            assert len(context1["run_id"]) == 36  # UUID length
            
            # Should be valid UUIDs
            uuid.UUID(context1["run_id"])
            uuid.UUID(context2["run_id"])

    def test_pre_run_config_run_timestamp(self, sample_base_config):
        """Test that pre_run_config generates timestamps."""
        config_dict = sample_base_config.model_dump()
        
        with patch('weiser.drivers.metric_stores.MetricStoreFactory.create_driver'):
            before = datetime.now()
            context = pre_run_config(config_dict, compile_only=True, verbose=False)
            after = datetime.now()
            
            assert isinstance(context["run_ts"], datetime)
            assert before <= context["run_ts"] <= after


class TestRunChecks:
    """Test run_checks functionality."""

    def test_run_checks_single_datasource(self):
        """Test running checks with single datasource."""
        # Create test config
        check = Check(
            name="test_check",
            dataset="orders",
            type=CheckType.row_count,
            datasource="test_db",
            condition=Condition.gt,
            threshold=0
        )
        
        config = BaseConfig(
            checks=[check],
            datasources=[],  # Won't be used in this test
            connections=[]
        )
        
        # Mock connections and metric store
        mock_driver = Mock()
        mock_metric_store = Mock()
        connections = {"test_db": mock_driver}
        
        # Mock check factory and check instance
        with patch('weiser.checks.CheckFactory.create_check') as mock_check_factory:
            mock_check_instance = Mock()
            mock_check_instance.check.name = "test_check"
            mock_check_instance.run.return_value = [{"check_id": "123", "success": True}]
            mock_check_factory.return_value = mock_check_instance
            
            results = run_checks("run_123", config, connections, mock_metric_store, verbose=False)
            
            assert len(results) == 1
            assert results[0]["check_instance"] == "test_check"
            assert len(results[0]["results"]) == 1
            mock_check_factory.assert_called_once()
            mock_check_instance.run.assert_called_once()

    def test_run_checks_multiple_datasources(self):
        """Test running checks with multiple datasources."""
        check = Check(
            name="test_check",
            dataset="orders",
            type=CheckType.row_count,
            datasource=["test_db1", "test_db2"],  # Multiple datasources
            condition=Condition.gt,
            threshold=0
        )
        
        config = BaseConfig(
            checks=[check],
            datasources=[],
            connections=[]
        )
        
        mock_driver1 = Mock()
        mock_driver2 = Mock()
        mock_metric_store = Mock()
        connections = {"test_db1": mock_driver1, "test_db2": mock_driver2}
        
        with patch('weiser.checks.CheckFactory.create_check') as mock_check_factory:
            mock_check_instance = Mock()
            mock_check_instance.check.name = "test_check"
            mock_check_instance.run.return_value = [{"check_id": "123", "success": True}]
            mock_check_factory.return_value = mock_check_instance
            
            results = run_checks("run_123", config, connections, mock_metric_store, verbose=False)
            
            # Should create 2 check instances (one per datasource)
            assert len(results) == 2
            assert mock_check_factory.call_count == 2

    def test_run_checks_missing_datasource(self):
        """Test run_checks raises exception for missing datasource."""
        check = Check(
            name="test_check",
            dataset="orders",
            type=CheckType.row_count,
            datasource="missing_db",
            condition=Condition.gt,
            threshold=0
        )
        
        config = BaseConfig(
            checks=[check],
            datasources=[],
            connections=[]
        )
        
        connections = {"test_db": Mock()}  # Different datasource
        mock_metric_store = Mock()
        
        with pytest.raises(Exception, match="Datasource missing_db is not configured"):
            run_checks("run_123", config, connections, mock_metric_store, verbose=False)

    def test_run_checks_multiple_checks(self):
        """Test running multiple different checks."""
        check1 = Check(
            name="check1",
            dataset="orders",
            type=CheckType.row_count,
            datasource="test_db",
            condition=Condition.gt,
            threshold=0
        )
        
        check2 = Check(
            name="check2", 
            dataset="customers",
            type=CheckType.numeric,
            measure="COUNT(*)",
            datasource="test_db",
            condition=Condition.gt,
            threshold=10
        )
        
        config = BaseConfig(
            checks=[check1, check2],
            datasources=[],
            connections=[]
        )
        
        connections = {"test_db": Mock()}
        mock_metric_store = Mock()
        
        with patch('weiser.checks.CheckFactory.create_check') as mock_check_factory:
            mock_check_instance = Mock()
            mock_check_instance.check.name = "test_check"
            mock_check_instance.run.return_value = [{"success": True}]
            mock_check_factory.return_value = mock_check_instance
            
            results = run_checks("run_123", config, connections, mock_metric_store, verbose=False)
            
            assert len(results) == 2
            assert mock_check_factory.call_count == 2


class TestGenerateSampleData:
    """Test generate_sample_data functionality."""

    def test_generate_sample_data_single_check(self):
        """Test generating sample data for a single check."""
        check = Check(
            name="test_check",
            dataset="orders",
            type=CheckType.row_count,
            datasource="test_db",
            condition=Condition.gt,
            threshold=100
        )
        
        config = BaseConfig(
            checks=[check],
            datasources=[],
            connections=[]
        )
        
        connections = {"test_db": Mock()}
        mock_metric_store = Mock()
        
        with patch('weiser.checks.CheckFactory.create_check') as mock_check_factory:
            mock_check_instance = Mock()
            mock_check_instance.check = check
            mock_check_instance.apply_condition.return_value = True
            mock_check_instance.append_result = Mock()
            mock_check_instance.run.return_value = [{"success": True}]
            mock_check_factory.return_value = mock_check_instance
            
            # Mock random to be deterministic
            with patch('random.randint', return_value=120):
                results = generate_sample_data("test_check", config, connections, mock_metric_store, verbose=False)
                
                # Should generate data for 31 days (30 days + today)
                assert len(results) >= 31
                mock_check_factory.assert_called()

    def test_generate_sample_data_between_condition(self):
        """Test generating sample data for between condition."""
        check = Check(
            name="test_anomaly",
            dataset="metrics",
            type=CheckType.anomaly,
            datasource="test_db",
            condition=Condition.between,
            threshold=[-2.0, 2.0]
        )
        
        config = BaseConfig(
            checks=[check],
            datasources=[],
            connections=[]
        )
        
        connections = {"test_db": Mock()}
        mock_metric_store = Mock()
        
        with patch('weiser.checks.CheckFactory.create_check') as mock_check_factory:
            mock_check_instance = Mock()
            mock_check_instance.check = check
            mock_check_instance.apply_condition.return_value = True
            mock_check_instance.append_result = Mock()
            mock_check_instance.run.return_value = [{"success": True}]
            mock_check_factory.return_value = mock_check_instance
            
            # Mock random to return value within range
            with patch('random.randint', return_value=1):
                results = generate_sample_data("test_anomaly", config, connections, mock_metric_store, verbose=False)
                
                assert len(results) >= 31

    def test_generate_sample_data_check_not_found(self):
        """Test generate_sample_data with non-existent check name."""
        check = Check(
            name="existing_check",
            dataset="orders",
            type=CheckType.row_count,
            datasource="test_db",
            condition=Condition.gt,
            threshold=0
        )
        
        config = BaseConfig(
            checks=[check],
            datasources=[],
            connections=[]
        )
        
        connections = {"test_db": Mock()}
        mock_metric_store = Mock()
        
        # Try to generate data for non-existent check
        results = generate_sample_data("non_existent_check", config, connections, mock_metric_store, verbose=False)
        
        # Should return empty results
        assert len(results) == 0

    def test_generate_sample_data_multiple_datasets(self):
        """Test generating sample data for check with multiple datasets."""
        check = Check(
            name="multi_dataset_check",
            dataset=["orders", "customers"],
            type=CheckType.row_count,
            datasource="test_db",
            condition=Condition.gt,
            threshold=50
        )
        
        config = BaseConfig(
            checks=[check],
            datasources=[],
            connections=[]
        )
        
        connections = {"test_db": Mock()}
        mock_metric_store = Mock()
        
        with patch('weiser.checks.CheckFactory.create_check') as mock_check_factory:
            mock_check_instance = Mock()
            mock_check_instance.check = check
            mock_check_instance.apply_condition.return_value = True
            mock_check_instance.append_result = Mock()
            mock_check_instance.run.return_value = [{"success": True}]
            mock_check_factory.return_value = mock_check_instance
            
            with patch('random.randint', return_value=60):
                results = generate_sample_data("multi_dataset_check", config, connections, mock_metric_store, verbose=False)
                
                # Should generate data for each dataset for each day
                # 31 days * 2 datasets = 62 results minimum
                assert len(results) >= 62

    def test_generate_sample_data_date_range(self):
        """Test that generate_sample_data creates data for correct date range."""
        check = Check(
            name="date_range_check",
            dataset="orders",
            type=CheckType.row_count,
            datasource="test_db",
            condition=Condition.gt,
            threshold=0
        )
        
        config = BaseConfig(
            checks=[check],
            datasources=[],
            connections=[]
        )
        
        connections = {"test_db": Mock()}
        mock_metric_store = Mock()
        
        # Capture the dates used in append_result calls
        append_result_calls = []
        
        def mock_append_result(success, value, results, dataset, dt, verbose):
            append_result_calls.append(dt)
        
        with patch('weiser.checks.CheckFactory.create_check') as mock_check_factory:
            mock_check_instance = Mock()
            mock_check_instance.check = check
            mock_check_instance.apply_condition.return_value = True
            mock_check_instance.append_result = mock_append_result
            mock_check_instance.run.return_value = [{"success": True}]
            mock_check_factory.return_value = mock_check_instance
            
            with patch('random.randint', return_value=50):
                results = generate_sample_data("date_range_check", config, connections, mock_metric_store, verbose=False)
                
                # Check that dates span 30 days back from today
                if append_result_calls:
                    earliest_date = min(append_result_calls)
                    latest_date = max(append_result_calls)
                    date_range = (latest_date - earliest_date).days
                    
                    assert date_range <= 30  # Should be within 30 days
                    assert len(append_result_calls) == 31  # 31 calls for 31 days (inclusive)