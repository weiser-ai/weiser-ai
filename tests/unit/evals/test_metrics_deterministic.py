from datetime import datetime

from weiser.evals.metrics import MetricFactory
from weiser.evals.models import AgentTrace, EvalTestCase, ToolCall
from weiser.evals.semantic_layer.base import SchemaCatalog, SchemaView
from weiser.loader.models import EvalGolden, MetricConfig, ReferenceValue

SCHEMA = SchemaCatalog(
    views={"merchants": SchemaView(name="merchants", members={"id", "name", "country"})},
    has_semantics=False,
    fetched_at=datetime.now(),
)


def _test_case(golden, trace, schema=SCHEMA, arm="baseline"):
    return EvalTestCase(golden=golden, arm=arm, trace=trace, schema_catalog=schema)


class TestSchemaMembershipMetric:
    def _metric(self):
        return MetricFactory.create(MetricConfig(type="schema_membership", threshold=1.0))

    def test_clean_sql_scores_one(self):
        golden = EvalGolden(id="q1", input="?")
        trace = AgentTrace(question="?", predicted_sqls=["SELECT id FROM merchants"])
        metric = self._metric()
        cs = metric.measure(_test_case(golden, trace))
        assert cs.applicable is True
        assert cs.score == 1.0

    def test_unknown_table_scores_zero(self):
        golden = EvalGolden(id="q1", input="?")
        trace = AgentTrace(question="?", predicted_sqls=["SELECT id FROM ghost_table"])
        metric = self._metric()
        cs = metric.measure(_test_case(golden, trace))
        assert cs.score == 0.0
        assert "ghost_table" in cs.rationale

    def test_not_applicable_without_predicted_sqls(self):
        golden = EvalGolden(id="q1", input="?")
        trace = AgentTrace(question="?", predicted_sqls=[])
        metric = self._metric()
        cs = metric.measure(_test_case(golden, trace))
        assert cs.applicable is False


class TestExpectedViewRecallMetric:
    def _metric(self):
        return MetricFactory.create(MetricConfig(type="expected_view_recall"))

    def test_not_applicable_without_expected_views(self):
        golden = EvalGolden(id="q1", input="?")
        trace = AgentTrace(question="?", predicted_sqls=["SELECT id FROM merchants"])
        cs = self._metric().measure(_test_case(golden, trace))
        assert cs.applicable is False

    def test_full_recall(self):
        golden = EvalGolden(id="q1", input="?", expected_views=["merchants"])
        trace = AgentTrace(question="?", predicted_sqls=["SELECT id FROM merchants"])
        cs = self._metric().measure(_test_case(golden, trace))
        assert cs.score == 1.0

    def test_missing_expected_view(self):
        golden = EvalGolden(id="q1", input="?", expected_views=["merchants", "payments"])
        trace = AgentTrace(question="?", predicted_sqls=["SELECT id FROM merchants"])
        cs = self._metric().measure(_test_case(golden, trace))
        assert cs.score == 0.5
        assert "payments" in cs.rationale


class TestReferenceValueMatchMetric:
    def _metric(self):
        return MetricFactory.create(MetricConfig(type="reference_value_match"))

    def test_not_applicable_without_reference_values(self):
        golden = EvalGolden(id="q1", input="?")
        trace = AgentTrace(question="?", query_results=[{"cnt": 3}])
        cs = self._metric().measure(_test_case(golden, trace))
        assert cs.applicable is False

    def test_matching_value_within_tolerance(self):
        golden = EvalGolden(
            id="q1", input="?",
            reference_values=[ReferenceValue(metric="cnt", expected=3, tolerance_pct=0.0)],
        )
        trace = AgentTrace(question="?", final_answer="3", query_results=[{"cnt": 3}])
        cs = self._metric().measure(_test_case(golden, trace))
        assert cs.score == 1.0

    def test_confidently_wrong_answer_is_caught(self):
        """The exact gap this metric closes: the agent queried the adjacent view/wrong
        aggregate, got a plausible-looking number back, and would pass a
        text-plausibility judge -- but the pinned reference value catches it."""
        golden = EvalGolden(
            id="q1", input="?",
            reference_values=[ReferenceValue(metric="cnt", expected=3, tolerance_pct=0.0)],
        )
        trace = AgentTrace(question="?", final_answer="42", query_results=[{"cnt": 42}])
        cs = self._metric().measure(_test_case(golden, trace))
        assert cs.score == 0.0
        assert "cnt" in cs.rationale

    def test_no_output_scores_zero_not_inapplicable(self):
        golden = EvalGolden(
            id="q1", input="?",
            reference_values=[ReferenceValue(metric="cnt", expected=3, tolerance_pct=0.0)],
        )
        trace = AgentTrace(question="?", final_answer=None, query_results=[])
        cs = self._metric().measure(_test_case(golden, trace))
        assert cs.applicable is True
        assert cs.score == 0.0
        assert cs.rationale == "no output to compare"


class TestStepEfficiencyMetric:
    def _metric(self):
        return MetricFactory.create(MetricConfig(type="step_efficiency"))

    def test_no_duplicates_scores_one(self):
        golden = EvalGolden(id="q1", input="?")
        trace = AgentTrace(
            question="?",
            tool_calls=[
                ToolCall(tool_name="query", args={"sql": "SELECT 1"}, turn=1),
                ToolCall(tool_name="submit_answer", args={"sql": "SELECT 1", "answer": "1"}, turn=2),
            ],
        )
        cs = self._metric().measure(_test_case(golden, trace))
        assert cs.score == 1.0

    def test_duplicate_call_is_flagged(self):
        golden = EvalGolden(id="q1", input="?")
        trace = AgentTrace(
            question="?",
            tool_calls=[
                ToolCall(tool_name="query", args={"sql": "SELECT 1"}, turn=1),
                ToolCall(tool_name="query", args={"sql": "SELECT 1"}, turn=2),
            ],
        )
        cs = self._metric().measure(_test_case(golden, trace))
        assert cs.score == 0.5
        assert "duplicate" in cs.rationale.lower() or "1/2" in cs.rationale


class TestHitLimitMetric:
    def _metric(self):
        return MetricFactory.create(MetricConfig(type="hit_limit"))

    def test_hit_limit_scores_zero(self):
        golden = EvalGolden(id="q1", input="?")
        trace = AgentTrace(question="?", hit_limit=True)
        cs = self._metric().measure(_test_case(golden, trace))
        assert cs.score == 0.0

    def test_finished_scores_one(self):
        golden = EvalGolden(id="q1", input="?")
        trace = AgentTrace(question="?", hit_limit=False)
        cs = self._metric().measure(_test_case(golden, trace))
        assert cs.score == 1.0
