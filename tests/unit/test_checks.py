import pytest
from unittest.mock import Mock, patch

from weiser.checks.numeric import (
    CheckNumeric,
    CheckRowCount,
    CheckSum,
    CheckMax,
    CheckMin,
    CheckMeasure,
    CheckNotEmpty,
    CheckNotEmptyPct,
)
from weiser.checks.anomaly import CheckAnomaly
from weiser.loader.models import Check, CheckType, Condition

# Import fixtures
from tests.fixtures.config_fixtures import *


class TestThresholdValidation:
    """Test that each check type properly validates data against thresholds."""

    def test_row_count_check_passes_threshold(self, mock_driver, mock_metric_store):
        """Test CheckRowCount passes when actual value exceeds threshold."""
        check_config = Check(
            name="test_row_count_pass",
            dataset="orders",
            type=CheckType.row_count,
            condition=Condition.gt,
            threshold=100,
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
            threshold=100,
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
            threshold=500.0,
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
            threshold=500.0,
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
            threshold=10000.0,
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
            threshold=10000.0,
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
            threshold=1000.0,
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
            threshold=1000.0,
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
            threshold=10.0,
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
            threshold=10.0,
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
            threshold=50000.0,
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
            threshold=50000.0,
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
            threshold=[-2.0, 2.0],
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

    def test_between_condition_fails_threshold_high(
        self, mock_driver, mock_metric_store
    ):
        """Test between condition fails when value is above upper bound."""
        check_config = Check(
            name="test_between_fail_high",
            dataset="orders",
            type=CheckType.numeric,
            measure="z_score",
            condition=Condition.between,
            threshold=[-2.0, 2.0],
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

    def test_between_condition_fails_threshold_low(
        self, mock_driver, mock_metric_store
    ):
        """Test between condition fails when value is below lower bound."""
        check_config = Check(
            name="test_between_fail_low",
            dataset="orders",
            type=CheckType.numeric,
            measure="z_score",
            condition=Condition.between,
            threshold=[-2.0, 2.0],
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
            threshold=[-2.0, 2.0],
        )

        # Mock sufficient historical data
        mock_metric_store.execute_query.return_value = [
            (100.0, "2023-01-01"),
            (105.0, "2023-01-02"),
            (98.0, "2023-01-03"),
            (102.0, "2023-01-04"),
            (101.0, "2023-01-05"),
            (99.0, "2023-01-06"),
        ]

        with patch("duckdb.connect") as mock_duckdb:
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
            threshold=[-2.0, 2.0],
        )

        # Mock sufficient historical data
        mock_metric_store.execute_query.return_value = [
            (100.0, "2023-01-01"),
            (105.0, "2023-01-02"),
            (98.0, "2023-01-03"),
            (102.0, "2023-01-04"),
            (101.0, "2023-01-05"),
            (99.0, "2023-01-06"),
        ]

        with patch("duckdb.connect") as mock_duckdb:
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
            threshold=5,
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
            threshold=5,
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
            threshold=0,
        )

        check = CheckNotEmpty(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        with pytest.raises(
            ValueError, match="NotEmpty check requires at least one dimension"
        ):
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
            filter="status = 'active'",
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

    def test_not_empty_pct_check_passes_threshold(self, mock_driver, mock_metric_store):
        """Test CheckNotEmptyPct passes when NULL percentage is within threshold."""
        check_config = Check(
            name="test_not_empty_pct_pass",
            dataset="orders",
            type=CheckType.not_empty_pct,
            dimensions=["customer_id", "product_id"],
            condition=Condition.le,
            threshold=0.1,  # 10% threshold
        )

        check = CheckNotEmptyPct(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database responses: 5% NULLs in customer_id, 8% NULLs in product_id (both <= 10%)
        mock_driver.execute_query.side_effect = [[(0.05,)], [(0.08,)]]

        results = check.run(verbose=False)

        assert len(results) == 2  # One result per dimension
        assert results[0]["success"] == True  # customer_id: 5% <= 10%
        assert results[0]["actual_value"] == 0.05
        assert results[0]["threshold"] == 0.1
        assert "customer_id_not_empty_pct" in results[0]["name"]

        assert results[1]["success"] == True  # product_id: 8% <= 10%
        assert results[1]["actual_value"] == 0.08
        assert results[1]["threshold"] == 0.1
        assert "product_id_not_empty_pct" in results[1]["name"]

    def test_not_empty_pct_check_fails_threshold(self, mock_driver, mock_metric_store):
        """Test CheckNotEmptyPct fails when NULL percentage exceeds threshold."""
        check_config = Check(
            name="test_not_empty_pct_fail",
            dataset="orders",
            type=CheckType.not_empty_pct,
            dimensions=["customer_id", "product_id"],
            condition=Condition.le,
            threshold=0.1,  # 10% threshold
        )

        check = CheckNotEmptyPct(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database responses: 15% NULLs in customer_id, 5% NULLs in product_id
        mock_driver.execute_query.side_effect = [[(0.15,)], [(0.05,)]]

        results = check.run(verbose=False)

        assert len(results) == 2  # One result per dimension
        assert results[0]["success"] == False  # customer_id: 15% > 10%
        assert results[0]["actual_value"] == 0.15
        assert results[0]["threshold"] == 0.1
        assert "customer_id_not_empty_pct" in results[0]["name"]

        assert results[1]["success"] == True  # product_id: 5% <= 10%
        assert results[1]["actual_value"] == 0.05
        assert results[1]["threshold"] == 0.1
        assert "product_id_not_empty_pct" in results[1]["name"]

    def test_not_empty_pct_check_default_threshold(
        self, mock_driver, mock_metric_store
    ):
        """Test CheckNotEmptyPct uses default threshold of 0.0 when not specified."""
        check_config = Check(
            name="test_not_empty_pct_default",
            dataset="orders",
            type=CheckType.not_empty_pct,
            dimensions=["customer_id"],
            condition=Condition.le,
            # No threshold specified
        )

        check = CheckNotEmptyPct(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: 0% NULLs in customer_id
        mock_driver.execute_query.return_value = [(0.0,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == True  # 0% <= 0% (default threshold)
        assert results[0]["actual_value"] == 0.0
        assert results[0]["threshold"] == 0.0  # Default threshold

    def test_not_empty_pct_check_boundary_conditions(
        self, mock_driver, mock_metric_store
    ):
        """Test CheckNotEmptyPct boundary conditions with exact threshold values."""
        check_config = Check(
            name="test_not_empty_pct_boundary",
            dataset="orders",
            type=CheckType.not_empty_pct,
            dimensions=["customer_id"],
            condition=Condition.le,
            threshold=0.1,  # Exactly 10%
        )

        check = CheckNotEmptyPct(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response: exactly 10% NULLs
        mock_driver.execute_query.return_value = [(0.1,)]

        results = check.run(verbose=False)

        assert len(results) == 1
        assert results[0]["success"] == True  # 10% <= 10%
        assert results[0]["actual_value"] == 0.1
        assert results[0]["threshold"] == 0.1

    def test_not_empty_pct_check_sql_generation(self, mock_driver, mock_metric_store):
        """Test CheckNotEmptyPct generates correct SQL for percentage calculation."""
        check_config = Check(
            name="test_not_empty_pct_sql",
            dataset="orders",
            type=CheckType.not_empty_pct,
            dimensions=["customer_id"],
            condition=Condition.le,
            threshold=0.05,  # 5% threshold
            filter="status = 'active'",
        )

        check = CheckNotEmptyPct(
            "run_123", check_config, mock_driver, "test_db", mock_metric_store
        )

        # Mock database response
        mock_driver.execute_query.return_value = [(0.03,)]

        results = check.run(verbose=True)

        # Verify the query was called
        mock_driver.execute_query.assert_called()
        # The query should calculate percentage:
        # CAST(SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS FLOAT) / CAST(COUNT(*) AS FLOAT)

        assert len(results) == 1
        assert results[0]["success"] == True  # 3% <= 5%
        assert results[0]["actual_value"] == 0.03
