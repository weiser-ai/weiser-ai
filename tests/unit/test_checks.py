import pytest
from unittest.mock import Mock, patch
from datetime import datetime
import pandas as pd

from weiser.checks.base import BaseCheck
from weiser.checks.numeric import (
    CheckNumeric,
    CheckRowCount,
    CheckSum,
    CheckMax,
    CheckMin,
    CheckMeasure,
    CheckNotEmpty,
)
from weiser.checks.anomaly import CheckAnomaly
from weiser.checks import CheckFactory
from weiser.loader.models import Check, CheckType, Condition
from sqlglot.expressions import Select

# Import fixtures
from tests.fixtures.config_fixtures import *


class TestBaseCheck:
    """Test BaseCheck functionality."""

    def test_snake_case_conversion(
        self, sample_row_count_check, mock_driver, mock_metric_store
    ):
        """Test snake_case method converts strings correctly."""
        check = BaseCheck(
            "run_123", sample_row_count_check, mock_driver, "test_db", mock_metric_store
        )

        assert check.snake_case("CamelCase") == "camel_case"
        assert check.snake_case("snake_case") == "snake_case"
        assert check.snake_case("Mixed-Case-String") == "mixed_case_string"
        assert check.snake_case("UPPER CASE") == "upper_case"

    def test_time_dimension_alias(
        self, sample_check_with_time_dimension, mock_driver, mock_metric_store
    ):
        """Test time dimension alias generation."""
        check = BaseCheck(
            "run_123",
            sample_check_with_time_dimension,
            mock_driver,
            "test_db",
            mock_metric_store,
        )

        alias = check.time_dimension_alias()
        assert alias == "created_at_day"

    def test_apply_condition_gt(
        self, sample_row_count_check, mock_driver, mock_metric_store
    ):
        """Test apply_condition with greater than condition."""
        check = BaseCheck(
            "run_123", sample_row_count_check, mock_driver, "test_db", mock_metric_store
        )

        assert check.apply_condition(10) == True  # 10 > 0
        assert check.apply_condition(0) == False  # 0 > 0
        assert check.apply_condition(-5) == False  # -5 > 0

    def test_apply_condition_between(
        self, sample_anomaly_check, mock_driver, mock_metric_store
    ):
        """Test apply_condition with between condition."""
        check = BaseCheck(
            "run_123", sample_anomaly_check, mock_driver, "test_db", mock_metric_store
        )

        assert check.apply_condition(1.0) == True  # -2.0 <= 1.0 <= 2.0
        assert check.apply_condition(-1.5) == True  # -2.0 <= -1.5 <= 2.0
        assert check.apply_condition(3.0) == False  # 3.0 not in [-2.0, 2.0]
        assert check.apply_condition(-3.0) == False  # -3.0 not in [-2.0, 2.0]

    def test_apply_condition_with_none_value(
        self, sample_row_count_check, mock_driver, mock_metric_store
    ):
        """Test apply_condition returns False for None values."""
        check = BaseCheck(
            "run_123", sample_row_count_check, mock_driver, "test_db", mock_metric_store
        )

        assert check.apply_condition(None) == False

    def test_generate_check_id(
        self, sample_row_count_check, mock_driver, mock_metric_store
    ):
        """Test check ID generation is consistent and unique."""
        check = BaseCheck(
            "run_123", sample_row_count_check, mock_driver, "test_db", mock_metric_store
        )

        check_id_1 = check.generate_check_id("orders", "test_row_count")
        check_id_2 = check.generate_check_id("orders", "test_row_count")
        check_id_3 = check.generate_check_id("customers", "test_row_count")

        # Same inputs should generate same ID
        assert check_id_1 == check_id_2
        # Different dataset should generate different ID
        assert check_id_1 != check_id_3
        # Should be SHA256 hex string
        assert len(check_id_1) == 64
        assert all(c in "0123456789abcdef" for c in check_id_1)

    def test_parse_dataset_simple_table(
        self, sample_row_count_check, mock_driver, mock_metric_store
    ):
        """Test parsing simple table name as dataset."""
        check = BaseCheck(
            "run_123", sample_row_count_check, mock_driver, "test_db", mock_metric_store
        )

        result = check.parse_dataset("orders")
        assert result == "orders"

    def test_parse_dataset_sql_query(
        self, sample_row_count_check, mock_driver, mock_metric_store
    ):
        """Test parsing SQL query as dataset."""
        check = BaseCheck(
            "run_123", sample_row_count_check, mock_driver, "test_db", mock_metric_store
        )

        sql_query = "SELECT * FROM orders WHERE status = 'active'"
        result = check.parse_dataset(sql_query)

        # Should return a subquery
        assert hasattr(result, "alias")

    def test_append_result_simple_check(
        self, sample_row_count_check, mock_driver, mock_metric_store
    ):
        """Test appending result for simple check without dimensions."""
        check = BaseCheck(
            "run_123", sample_row_count_check, mock_driver, "test_db", mock_metric_store
        )

        results = []
        run_time = datetime.now()

        check.append_result(True, 100, results, "orders", run_time, verbose=False)

        assert len(results) == 1
        result = results[0]
        assert result["success"] == True
        assert result["actual_value"] == 100
        assert result["dataset"] == "orders"
        assert result["run_id"] == "run_123"
        mock_metric_store.insert_results.assert_called_once()

    def test_append_result_with_dimensions(
        self, sample_check_with_dimensions, mock_driver, mock_metric_store
    ):
        """Test appending result for check with dimensions."""
        check = BaseCheck(
            "run_123",
            sample_check_with_dimensions,
            mock_driver,
            "test_db",
            mock_metric_store,
        )

        results = []
        run_time = datetime.now()

        # Value with dimensions should be tuple: (dim1_value, dim2_value, actual_value)
        check.append_result(
            True, ("active", "US", 50), results, "orders", run_time, verbose=False
        )

        assert len(results) == 1
        result = results[0]
        assert result["success"] == True
        assert result["actual_value"] == 50
        # Name should include dimension values
        assert "active" in result["name"]
        assert "US" in result["name"]


class TestNumericChecks:
    """Test numeric check implementations."""

    def test_check_numeric_sql_generation(
        self, sample_numeric_check, mock_driver, mock_metric_store
    ):
        """Test CheckNumeric generates correct SQL."""
        check = CheckNumeric(
            "run_123", sample_numeric_check, mock_driver, "test_db", mock_metric_store
        )

        query = check.get_query("orders", verbose=False)
        sql = query.sql()

        assert "sum(amount)" in sql.lower()
        assert "from orders" in sql.lower()
        assert "limit 1" in sql.lower()

    def test_check_row_count_sql_generation(
        self, sample_row_count_check, mock_driver, mock_metric_store
    ):
        """Test CheckRowCount generates correct SQL."""
        check = CheckRowCount(
            "run_123", sample_row_count_check, mock_driver, "test_db", mock_metric_store
        )

        query = check.get_query("orders", verbose=False)
        sql = query.sql()

        assert "count(*)" in sql.lower()
        assert "from orders" in sql.lower()

    def test_check_sum_sql_generation(self, mock_driver, mock_metric_store):
        """Test CheckSum generates correct SQL."""
        check_config = Check(
            name="test_sum",
            dataset="orders",
            type=CheckType.sum,
            measure="amount",
            condition=Condition.gt,
            threshold=1000.0,
        )

        check = CheckSum(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        query = check.get_query("orders", verbose=False)
        sql = query.sql()

        assert "sum(amount)" in sql.lower()

    def test_check_max_sql_generation(self, mock_driver, mock_metric_store):
        """Test CheckMax generates correct SQL."""
        check_config = Check(
            name="test_max",
            dataset="orders",
            type=CheckType.max,
            measure="amount",
            condition=Condition.gt,
            threshold=1000.0,
        )

        check = CheckMax(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        query = check.get_query("orders", verbose=False)
        sql = query.sql()

        assert "max(amount)" in sql.lower()

    def test_check_min_sql_generation(self, mock_driver, mock_metric_store):
        """Test CheckMin generates correct SQL."""
        check_config = Check(
            name="test_min",
            dataset="orders",
            type=CheckType.min,
            measure="amount",
            condition=Condition.gt,
            threshold=0.0,
        )

        check = CheckMin(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        query = check.get_query("orders", verbose=False)
        sql = query.sql()

        assert "min(amount)" in sql.lower()

    def test_check_measure_sql_generation(self, mock_driver, mock_metric_store):
        """Test CheckMeasure generates correct SQL for Cube.js."""
        check_config = Check(
            name="test_measure",
            dataset="orders",
            type=CheckType.measure,
            measure="total_amount",
            condition=Condition.gt,
            threshold=1000.0,
        )

        check = CheckMeasure(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        query = check.get_query("orders", verbose=False)
        sql = query.sql()

        assert "measure(total_amount)" in sql.lower()

    def test_check_with_filter_sql_generation(
        self, sample_check_with_filter, mock_driver, mock_metric_store
    ):
        """Test check with filter generates correct WHERE clause."""
        check = CheckNumeric(
            "run_123",
            sample_check_with_filter,
            mock_driver,
            "test_db",
            mock_metric_store,
        )

        query = check.get_query("orders", verbose=False)
        sql = query.sql()

        assert "where" in sql.lower()
        assert "status = 'completed'" in sql.lower()

    def test_check_with_dimensions_sql_generation(
        self, sample_check_with_dimensions, mock_driver, mock_metric_store
    ):
        """Test check with dimensions generates correct GROUP BY clause."""
        check = CheckRowCount(
            "run_123",
            sample_check_with_dimensions,
            mock_driver,
            "test_db",
            mock_metric_store,
        )

        query = check.get_query("orders", verbose=False)
        sql = query.sql()

        assert "group by" in sql.lower()
        assert "status" in sql.lower()
        assert "region" in sql.lower()

    def test_check_with_time_dimension_sql_generation(
        self, sample_check_with_time_dimension, mock_driver, mock_metric_store
    ):
        """Test check with time dimension generates correct DATE_TRUNC."""
        check = CheckSum(
            "run_123",
            sample_check_with_time_dimension,
            mock_driver,
            "test_db",
            mock_metric_store,
        )

        query = check.get_query("orders", verbose=False)
        sql = query.sql()

        assert "date_trunc" in sql.lower()
        assert "'day'" in sql.lower()
        assert "created_at" in sql.lower()


class TestAnomalyCheck:
    """Test anomaly detection check."""

    @patch("duckdb.connect")
    def test_anomaly_check_insufficient_data(
        self, mock_duckdb, sample_anomaly_check, mock_driver, mock_metric_store
    ):
        """Test anomaly check with insufficient data points."""
        # Mock metric store to return only 3 data points (less than 5)
        mock_metric_store.execute_query.return_value = [
            (100.0, "2023-01-01"),
            (105.0, "2023-01-02"),
            (110.0, "2023-01-03"),
        ]

        check = CheckAnomaly(
            "run_123", sample_anomaly_check, mock_driver, "test_db", mock_metric_store
        )

        results = check.run(verbose=False)

        assert len(results) == 1
        # Should fail due to insufficient data
        assert results[0]["success"] == False

    @patch("duckdb.connect")
    def test_anomaly_check_sufficient_data(
        self, mock_duckdb, sample_anomaly_check, mock_driver, mock_metric_store
    ):
        """Test anomaly check with sufficient data points."""
        # Mock DuckDB connection and results
        mock_conn = Mock()
        mock_duckdb.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = [
            (5.0, 100.0, 102.0)
        ]  # mad, median, last_value

        # Mock metric store to return sufficient data points
        mock_metric_store.execute_query.return_value = [
            (100.0, "2023-01-01"),
            (105.0, "2023-01-02"),
            (110.0, "2023-01-03"),
            (95.0, "2023-01-04"),
            (102.0, "2023-01-05"),
        ]

        check = CheckAnomaly(
            "run_123", sample_anomaly_check, mock_driver, "test_db", mock_metric_store
        )

        results = check.run(verbose=False)

        assert len(results) == 1
        # MAD calculation should determine if anomaly
        mock_conn.execute.assert_called()

    def test_anomaly_check_sql_generation(
        self, sample_anomaly_check, mock_driver, mock_metric_store
    ):
        """Test anomaly check generates correct SQL for metrics table."""
        check = CheckAnomaly(
            "run_123", sample_anomaly_check, mock_driver, "test_db", mock_metric_store
        )

        query = check.get_query("metrics", verbose=False)
        sql = query.sql()

        assert "actual_value" in sql.lower()
        assert "run_time" in sql.lower()
        assert "check_id like" in sql.lower()
        assert "order by run_time asc" in sql.lower()


class TestCheckFactory:
    """Test CheckFactory functionality."""

    def test_create_numeric_check(
        self, sample_numeric_check, mock_driver, mock_metric_store
    ):
        """Test factory creates CheckNumeric for numeric type."""
        check = CheckFactory.create_check(
            "run_123", sample_numeric_check, mock_driver, "test_db", mock_metric_store
        )

        assert isinstance(check, CheckNumeric)

    def test_create_row_count_check(
        self, sample_row_count_check, mock_driver, mock_metric_store
    ):
        """Test factory creates CheckRowCount for row_count type."""
        check = CheckFactory.create_check(
            "run_123", sample_row_count_check, mock_driver, "test_db", mock_metric_store
        )

        assert isinstance(check, CheckRowCount)

    def test_create_anomaly_check(
        self, sample_anomaly_check, mock_driver, mock_metric_store
    ):
        """Test factory creates CheckAnomaly for anomaly type."""
        check = CheckFactory.create_check(
            "run_123", sample_anomaly_check, mock_driver, "test_db", mock_metric_store
        )

        assert isinstance(check, CheckAnomaly)

    def test_create_sum_check(self, mock_driver, mock_metric_store):
        """Test factory creates CheckSum for sum type."""
        check_config = Check(
            name="test_sum",
            dataset="orders",
            type=CheckType.sum,
            measure="amount",
            condition=Condition.gt,
            threshold=0,
        )

        check = CheckFactory.create_check(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        assert isinstance(check, CheckSum)

    def test_create_max_check(self, mock_driver, mock_metric_store):
        """Test factory creates CheckMax for max type."""
        check_config = Check(
            name="test_max",
            dataset="orders",
            type=CheckType.max,
            measure="amount",
            condition=Condition.gt,
            threshold=0,
        )

        check = CheckFactory.create_check(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        assert isinstance(check, CheckMax)

    def test_create_min_check(self, mock_driver, mock_metric_store):
        """Test factory creates CheckMin for min type."""
        check_config = Check(
            name="test_min",
            dataset="orders",
            type=CheckType.min,
            measure="amount",
            condition=Condition.gt,
            threshold=0,
        )

        check = CheckFactory.create_check(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        assert isinstance(check, CheckMin)

    def test_create_measure_check(self, mock_driver, mock_metric_store):
        """Test factory creates CheckMeasure for measure type."""
        check_config = Check(
            name="test_measure",
            dataset="orders",
            type=CheckType.measure,
            measure="total_amount",
            condition=Condition.gt,
            threshold=0,
        )

        check = CheckFactory.create_check(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        assert isinstance(check, CheckMeasure)

    def test_create_not_empty_check(self, mock_driver, mock_metric_store):
        """Test factory creates CheckNotEmpty for not_empty type."""
        check_config = Check(
            name="test_not_empty",
            dataset="orders",
            type=CheckType.not_empty,
            dimensions=["customer_id", "product_id"],
            condition=Condition.le,
            threshold=5
        )

        check = CheckFactory.create_check(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        assert isinstance(check, CheckNotEmpty)

    def test_create_check_unsupported_type(self, mock_driver, mock_metric_store):
        """Test factory raises exception for unsupported check type."""
        # Create check with invalid type by bypassing validation
        check_config = Mock()
        check_config.type = "unsupported_type"

        with pytest.raises(Exception, match="Check Type .* not implemented yet"):
            CheckFactory.create_check(
                "run_123", check_config, mock_driver, "test_db", mock_metric_store
            )


class TestThresholdValidation:
    """Test that each check type properly validates data against thresholds."""

    def test_row_count_check_passes_threshold(self, mock_driver, mock_metric_store):
        """Test CheckRowCount passes when actual value exceeds threshold."""
        check_config = Check(
            name="test_row_count_pass",
            dataset="orders",
            type=CheckType.row_count,
            condition=Condition.gt,
            threshold=100
        )

        check = CheckRowCount(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: 150 rows (should pass > 100)
        mock_driver.execute_query.return_value = [(150,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == True
        assert results[0]["actual_value"] == 150
        assert results[0]["threshold"] == 100

    def test_row_count_check_fails_threshold(self, mock_driver, mock_metric_store):
        """Test CheckRowCount fails when actual value is below threshold."""
        check_config = Check(
            name="test_row_count_fail",
            dataset="orders",
            type=CheckType.row_count,
            condition=Condition.gt,
            threshold=100
        )

        check = CheckRowCount(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: 50 rows (should fail <= 100)
        mock_driver.execute_query.return_value = [(50,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == False
        assert results[0]["actual_value"] == 50
        assert results[0]["threshold"] == 100

    def test_numeric_check_passes_threshold(self, mock_driver, mock_metric_store):
        """Test CheckNumeric passes when actual value exceeds threshold."""
        check_config = Check(
            name="test_numeric_pass",
            dataset="orders",
            type=CheckType.numeric,
            measure="avg(amount)",
            condition=Condition.gt,
            threshold=500.0
        )

        check = CheckNumeric(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: 750.5 (should pass > 500.0)
        mock_driver.execute_query.return_value = [(750.5,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == True
        assert results[0]["actual_value"] == 750.5
        assert results[0]["threshold"] == 500.0

    def test_numeric_check_fails_threshold(self, mock_driver, mock_metric_store):
        """Test CheckNumeric fails when actual value is below threshold."""
        check_config = Check(
            name="test_numeric_fail",
            dataset="orders",
            type=CheckType.numeric,
            measure="avg(amount)",
            condition=Condition.gt,
            threshold=500.0
        )

        check = CheckNumeric(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: 300.2 (should fail <= 500.0)
        mock_driver.execute_query.return_value = [(300.2,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == False
        assert results[0]["actual_value"] == 300.2
        assert results[0]["threshold"] == 500.0

    def test_sum_check_passes_threshold(self, mock_driver, mock_metric_store):
        """Test CheckSum passes when actual value exceeds threshold."""
        check_config = Check(
            name="test_sum_pass",
            dataset="orders",
            type=CheckType.sum,
            measure="amount",
            condition=Condition.ge,
            threshold=10000.0
        )

        check = CheckSum(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: 15000.0 (should pass >= 10000.0)
        mock_driver.execute_query.return_value = [(15000.0,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == True
        assert results[0]["actual_value"] == 15000.0
        assert results[0]["threshold"] == 10000.0

    def test_sum_check_fails_threshold(self, mock_driver, mock_metric_store):
        """Test CheckSum fails when actual value is below threshold."""
        check_config = Check(
            name="test_sum_fail",
            dataset="orders",
            type=CheckType.sum,
            measure="amount",
            condition=Condition.ge,
            threshold=10000.0
        )

        check = CheckSum(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: 8500.0 (should fail < 10000.0)
        mock_driver.execute_query.return_value = [(8500.0,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == False
        assert results[0]["actual_value"] == 8500.0
        assert results[0]["threshold"] == 10000.0

    def test_max_check_passes_threshold(self, mock_driver, mock_metric_store):
        """Test CheckMax passes when actual value is within threshold."""
        check_config = Check(
            name="test_max_pass",
            dataset="orders",
            type=CheckType.max,
            measure="amount",
            condition=Condition.le,
            threshold=1000.0
        )

        check = CheckMax(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: 950.0 (should pass <= 1000.0)
        mock_driver.execute_query.return_value = [(950.0,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == True
        assert results[0]["actual_value"] == 950.0
        assert results[0]["threshold"] == 1000.0

    def test_max_check_fails_threshold(self, mock_driver, mock_metric_store):
        """Test CheckMax fails when actual value exceeds threshold."""
        check_config = Check(
            name="test_max_fail",
            dataset="orders",
            type=CheckType.max,
            measure="amount",
            condition=Condition.le,
            threshold=1000.0
        )

        check = CheckMax(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: 1500.0 (should fail > 1000.0)
        mock_driver.execute_query.return_value = [(1500.0,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == False
        assert results[0]["actual_value"] == 1500.0
        assert results[0]["threshold"] == 1000.0

    def test_min_check_passes_threshold(self, mock_driver, mock_metric_store):
        """Test CheckMin passes when actual value exceeds threshold."""
        check_config = Check(
            name="test_min_pass",
            dataset="orders",
            type=CheckType.min,
            measure="amount",
            condition=Condition.ge,
            threshold=10.0
        )

        check = CheckMin(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: 25.0 (should pass >= 10.0)
        mock_driver.execute_query.return_value = [(25.0,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == True
        assert results[0]["actual_value"] == 25.0
        assert results[0]["threshold"] == 10.0

    def test_min_check_fails_threshold(self, mock_driver, mock_metric_store):
        """Test CheckMin fails when actual value is below threshold."""
        check_config = Check(
            name="test_min_fail",
            dataset="orders",
            type=CheckType.min,
            measure="amount",
            condition=Condition.ge,
            threshold=10.0
        )

        check = CheckMin(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: 5.0 (should fail < 10.0)
        mock_driver.execute_query.return_value = [(5.0,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == False
        assert results[0]["actual_value"] == 5.0
        assert results[0]["threshold"] == 10.0

    def test_measure_check_passes_threshold(self, mock_driver, mock_metric_store):
        """Test CheckMeasure passes when actual value exceeds threshold."""
        check_config = Check(
            name="test_measure_pass",
            dataset="orders",
            type=CheckType.measure,
            measure="total_revenue",
            condition=Condition.gt,
            threshold=50000.0
        )

        check = CheckMeasure(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: 75000.0 (should pass > 50000.0)
        mock_driver.execute_query.return_value = [(75000.0,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == True
        assert results[0]["actual_value"] == 75000.0
        assert results[0]["threshold"] == 50000.0

    def test_measure_check_fails_threshold(self, mock_driver, mock_metric_store):
        """Test CheckMeasure fails when actual value is below threshold."""
        check_config = Check(
            name="test_measure_fail",
            dataset="orders",
            type=CheckType.measure,
            measure="total_revenue",
            condition=Condition.gt,
            threshold=50000.0
        )

        check = CheckMeasure(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: 35000.0 (should fail <= 50000.0)
        mock_driver.execute_query.return_value = [(35000.0,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == False
        assert results[0]["actual_value"] == 35000.0
        assert results[0]["threshold"] == 50000.0

    def test_between_condition_passes_threshold(self, mock_driver, mock_metric_store):
        """Test between condition passes when value is within range."""
        check_config = Check(
            name="test_between_pass",
            dataset="orders",
            type=CheckType.numeric,
            measure="z_score",
            condition=Condition.between,
            threshold=[-2.0, 2.0]
        )

        check = CheckNumeric(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: 1.5 (should pass within [-2.0, 2.0])
        mock_driver.execute_query.return_value = [(1.5,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == True
        assert results[0]["actual_value"] == 1.5
        assert results[0]["threshold"] == [-2.0, 2.0]

    def test_between_condition_fails_threshold_high(self, mock_driver, mock_metric_store):
        """Test between condition fails when value is above upper bound."""
        check_config = Check(
            name="test_between_fail_high",
            dataset="orders",
            type=CheckType.numeric,
            measure="z_score",
            condition=Condition.between,
            threshold=[-2.0, 2.0]
        )

        check = CheckNumeric(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: 3.5 (should fail > 2.0)
        mock_driver.execute_query.return_value = [(3.5,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == False
        assert results[0]["actual_value"] == 3.5
        assert results[0]["threshold"] == [-2.0, 2.0]

    def test_between_condition_fails_threshold_low(self, mock_driver, mock_metric_store):
        """Test between condition fails when value is below lower bound."""
        check_config = Check(
            name="test_between_fail_low",
            dataset="orders",
            type=CheckType.numeric,
            measure="z_score",
            condition=Condition.between,
            threshold=[-2.0, 2.0]
        )

        check = CheckNumeric(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: -3.0 (should fail < -2.0)
        mock_driver.execute_query.return_value = [(-3.0,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == False
        assert results[0]["actual_value"] == -3.0
        assert results[0]["threshold"] == [-2.0, 2.0]

    def test_anomaly_check_passes_threshold(self, mock_driver, mock_metric_store):
        """Test CheckAnomaly passes when z-score is within bounds."""
        check_config = Check(
            name="test_anomaly_pass",
            dataset="metrics",
            type=CheckType.anomaly,
            check_id="anomaly_123",
            condition=Condition.between,
            threshold=[-2.0, 2.0]
        )

        # Mock sufficient historical data
        mock_metric_store.execute_query.return_value = [
            (100.0, "2023-01-01"), (105.0, "2023-01-02"), (98.0, "2023-01-03"),
            (102.0, "2023-01-04"), (101.0, "2023-01-05"), (99.0, "2023-01-06")
        ]

        with patch('duckdb.connect') as mock_duckdb:
            mock_conn = Mock()
            mock_duckdb.return_value.__enter__.return_value = mock_conn
            # Mock MAD calculation: median=100, mad=2, last_value=101 -> z_score=0.5
            mock_conn.execute.return_value.fetchall.return_value = [(2.0, 100.0, 101.0)]

            check = CheckAnomaly(
                "run_123", check_config, mock_driver, "test_db", mock_metric_store
            )

            results = check.run(verbose=False)

            assert len(results) == 1
            assert results[0]["success"] == True
            # Z-score should be 0.5 which is within [-2.0, 2.0]

    def test_anomaly_check_fails_threshold(self, mock_driver, mock_metric_store):
        """Test CheckAnomaly fails when z-score is outside bounds."""
        check_config = Check(
            name="test_anomaly_fail",
            dataset="metrics",
            type=CheckType.anomaly,
            check_id="anomaly_456",
            condition=Condition.between,
            threshold=[-2.0, 2.0]
        )

        # Mock sufficient historical data
        mock_metric_store.execute_query.return_value = [
            (100.0, "2023-01-01"), (105.0, "2023-01-02"), (98.0, "2023-01-03"),
            (102.0, "2023-01-04"), (101.0, "2023-01-05"), (99.0, "2023-01-06")
        ]

        with patch('duckdb.connect') as mock_duckdb:
            mock_conn = Mock()
            mock_duckdb.return_value.__enter__.return_value = mock_conn
            # Mock MAD calculation: median=100, mad=2, last_value=110 -> z_score=5.0
            mock_conn.execute.return_value.fetchall.return_value = [(2.0, 100.0, 110.0)]

            check = CheckAnomaly(
                "run_123", check_config, mock_driver, "test_db", mock_metric_store
            )

            results = check.run(verbose=False)

            assert len(results) == 1
            assert results[0]["success"] == False
            # Z-score should be 5.0 which is outside [-2.0, 2.0]

    def test_not_empty_check_passes_threshold(self, mock_driver, mock_metric_store):
        """Test CheckNotEmpty passes when NULL count is within threshold."""
        check_config = Check(
            name="test_not_empty_pass",
            dataset="orders",
            type=CheckType.not_empty,
            dimensions=["customer_id", "product_id"],
            condition=Condition.le,
            threshold=5
        )

        check = CheckNotEmpty(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database responses: 3 NULLs in customer_id, 2 NULLs in product_id (both <= 5)
        mock_driver.execute_query.side_effect = [[(3,)], [(2,)]]

        results = check.run(verbose=False)

        assert len(results) == 2  # One result per dimension
        assert results[0]["success"] == True  # customer_id: 3 <= 5
        assert results[0]["actual_value"] == 3
        assert results[0]["threshold"] == 5
        assert "customer_id_not_empty" in results[0]["name"]
        
        assert results[1]["success"] == True  # product_id: 2 <= 5
        assert results[1]["actual_value"] == 2
        assert results[1]["threshold"] == 5
        assert "product_id_not_empty" in results[1]["name"]

    def test_not_empty_check_fails_threshold(self, mock_driver, mock_metric_store):
        """Test CheckNotEmpty fails when NULL count exceeds threshold."""
        check_config = Check(
            name="test_not_empty_fail",
            dataset="orders",
            type=CheckType.not_empty,
            dimensions=["customer_id", "product_id"],
            condition=Condition.le,
            threshold=5
        )

        check = CheckNotEmpty(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database responses: 8 NULLs in customer_id, 3 NULLs in product_id
        mock_driver.execute_query.side_effect = [[(8,)], [(3,)]]

        results = check.run(verbose=False)

        assert len(results) == 2  # One result per dimension
        assert results[0]["success"] == False  # customer_id: 8 > 5
        assert results[0]["actual_value"] == 8
        assert results[0]["threshold"] == 5
        assert "customer_id_not_empty" in results[0]["name"]
        
        assert results[1]["success"] == True  # product_id: 3 <= 5
        assert results[1]["actual_value"] == 3
        assert results[1]["threshold"] == 5
        assert "product_id_not_empty" in results[1]["name"]

    def test_not_empty_check_default_threshold(self, mock_driver, mock_metric_store):
        """Test CheckNotEmpty uses default threshold of 0 when not specified."""
        check_config = Check(
            name="test_not_empty_default",
            dataset="orders",
            type=CheckType.not_empty,
            dimensions=["customer_id"],
            condition=Condition.le,
            # No threshold specified
        )

        check = CheckNotEmpty(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: 0 NULLs in customer_id
        mock_driver.execute_query.return_value = [(0,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == True  # 0 <= 0 (default threshold)
        assert results[0]["actual_value"] == 0
        assert results[0]["threshold"] == 0  # Default threshold

    def test_not_empty_check_no_dimensions_error(self, mock_driver, mock_metric_store):
        """Test CheckNotEmpty raises error when no dimensions specified."""
        check_config = Check(
            name="test_not_empty_no_dims",
            dataset="orders",
            type=CheckType.not_empty,
            dimensions=[],  # Empty dimensions
            condition=Condition.le,
            threshold=0
        )

        check = CheckNotEmpty(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        with pytest.raises(ValueError, match="NotEmpty check requires at least one dimension"):
            check.run(verbose=False)

    def test_not_empty_check_sql_generation(self, mock_driver, mock_metric_store):
        """Test CheckNotEmpty generates correct SQL for each dimension."""
        check_config = Check(
            name="test_not_empty_sql",
            dataset="orders",
            type=CheckType.not_empty,
            dimensions=["customer_id"],
            condition=Condition.le,
            threshold=0,
            filter="status = 'active'"
        )

        check = CheckNotEmpty(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response
        mock_driver.execute_query.return_value = [(0,)]

        results = check.run(verbose=True)

        # Verify the query was called
        mock_driver.execute_query.assert_called()
        # The query should contain the NULL check for customer_id
        # and should include the filter condition
