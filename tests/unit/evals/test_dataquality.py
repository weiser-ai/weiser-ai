from datetime import datetime
from unittest.mock import Mock

from weiser.evals.dataquality import attribute_failure, build_dq_context
from weiser.evals.models import CriterionScore, DQContext
from weiser.loader.models import Check, CheckType, Condition


def _check(name="merchants_not_empty", dataset="merchants", threshold=100):
    return Check(
        name=name,
        dataset=dataset,
        datasource="local_db",
        type=CheckType.row_count,
        condition=Condition.gt,
        threshold=threshold,
    )


class TestBuildDQContext:
    def test_latest_mode_finds_failing_check(self, mock_driver, mock_metric_store):
        check = _check()
        fake_record = Mock(success=False, actual_value=3, run_time=datetime.now())
        mock_metric_store.get_metrics_for_check = Mock(return_value=[fake_record])

        ctx = build_dq_context(
            predicted_sqls=["SELECT COUNT(*) FROM merchants"],
            checks=[check],
            connections={"local_db": mock_driver},
            metric_store=mock_metric_store,
            run_id="run1",
            mode="latest",
        )
        assert "merchants" in ctx.touched_datasets
        assert ctx.has_failing_dq is True
        assert ctx.dq_results[0]["success"] is False

    def test_latest_mode_no_history_is_clean(self, mock_driver, mock_metric_store):
        check = _check(threshold=0)
        mock_metric_store.get_metrics_for_check = Mock(return_value=[])

        ctx = build_dq_context(
            ["SELECT COUNT(*) FROM merchants"],
            [check],
            {"local_db": mock_driver},
            mock_metric_store,
            "run1",
            mode="latest",
        )
        assert ctx.has_failing_dq is False
        assert ctx.dq_results == []

    def test_untouched_dataset_is_not_confounding_evidence(self, mock_driver, mock_metric_store):
        check = _check(name="other_check", dataset="orders")
        mock_metric_store.get_metrics_for_check = Mock(
            return_value=[Mock(success=False, actual_value=0, run_time=datetime.now())]
        )

        ctx = build_dq_context(
            ["SELECT COUNT(*) FROM merchants"],
            [check],
            {"local_db": mock_driver},
            mock_metric_store,
            "run1",
            mode="latest",
        )
        assert ctx.has_failing_dq is False
        assert ctx.dq_results == []

    def test_latest_mode_picks_most_recent_result(self, mock_driver, mock_metric_store):
        check = _check()
        older = Mock(success=False, actual_value=3, run_time=datetime(2026, 1, 1))
        newer = Mock(success=True, actual_value=200, run_time=datetime(2026, 6, 1))
        mock_metric_store.get_metrics_for_check = Mock(return_value=[older, newer])

        ctx = build_dq_context(
            ["SELECT COUNT(*) FROM merchants"],
            [check],
            {"local_db": mock_driver},
            mock_metric_store,
            "run1",
            mode="latest",
        )
        assert ctx.has_failing_dq is False
        assert ctx.dq_results[0]["success"] is True

    def test_live_mode_runs_check_now(self, mock_driver, mock_metric_store):
        check = _check()
        mock_driver.execute_query = Mock(return_value=[(3,)])

        ctx = build_dq_context(
            ["SELECT COUNT(*) FROM merchants"],
            [check],
            {"local_db": mock_driver},
            mock_metric_store,
            "run1",
            mode="live",
        )
        assert ctx.has_failing_dq is True
        mock_metric_store.insert_results.assert_called()


class TestAttributeFailure:
    def test_clean_when_score_clears_threshold(self):
        assert attribute_failure([], DQContext(), overall_score=0.9, threshold=0.5) == "clean"

    def test_data_quality_when_deterministic_clean_and_dq_failing(self):
        criteria = [
            CriterionScore(criterion="schema_membership", score=1.0),
            CriterionScore(criterion="expected_view_recall", applicable=False),
            CriterionScore(criterion="step_efficiency", score=1.0),
            CriterionScore(criterion="reference_value_match", score=0.0),
        ]
        dq_context = DQContext(
            touched_datasets=["merchants"],
            dq_results=[{"check_name": "x", "success": False}],
            has_failing_dq=True,
        )
        assert (
            attribute_failure(criteria, dq_context, overall_score=0.25, threshold=0.5)
            == "data_quality"
        )

    def test_agent_when_deterministic_clean_and_no_dq_evidence(self):
        criteria = [
            CriterionScore(criterion="schema_membership", score=1.0),
            CriterionScore(criterion="reference_value_match", score=0.0),
        ]
        assert (
            attribute_failure(criteria, DQContext(), overall_score=0.0, threshold=0.5)
            == "agent"
        )

    def test_judge_uncertain_when_deterministic_checks_also_failed(self):
        criteria = [CriterionScore(criterion="schema_membership", score=0.0)]
        dq_context = DQContext(has_failing_dq=True)
        assert (
            attribute_failure(criteria, dq_context, overall_score=0.0, threshold=0.5)
            == "judge_uncertain"
        )
