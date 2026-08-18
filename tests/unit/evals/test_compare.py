from weiser.evals.compare import lint_arms, summarize_arm

from tests.fixtures.config_fixtures import *  # noqa: F401,F403


class TestLintArms:
    def test_single_variable_diff_is_quiet(
        self, sample_eval_suite_two_arms, sample_agent_variant, sample_agent_variant_tool_ablation
    ):
        variants = {
            "baseline": sample_agent_variant,
            "with_lookup_tool": sample_agent_variant_tool_ablation,
        }
        assert lint_arms(sample_eval_suite_two_arms, variants) == []

    def test_multi_variable_diff_warns(
        self, sample_eval_suite_two_arms, sample_agent_variant, sample_agent_variant_multi_change
    ):
        variants = {
            "baseline": sample_agent_variant,
            "with_lookup_tool": sample_agent_variant_multi_change,
        }
        warnings = lint_arms(sample_eval_suite_two_arms, variants)
        assert len(warnings) == 1
        assert "model" in warnings[0]
        assert "tools" in warnings[0]

    def test_single_arm_suite_has_no_warnings(self, sample_eval_suite, sample_agent_variant):
        assert lint_arms(sample_eval_suite, {"baseline": sample_agent_variant}) == []

    def test_unresolved_variant_is_tolerated(self, sample_eval_suite_two_arms):
        assert lint_arms(sample_eval_suite_two_arms, {}) == []


class TestSummarizeArm:
    def test_summarize_arm_basic(self):
        rows = [
            {
                "result": {"arm": "baseline", "level": "easy", "failure_attribution": "clean"},
                "trace": {"cost_usd": 0.01, "tool_calls": [{}, {}], "hit_limit": False},
            },
            {
                "result": {"arm": "baseline", "level": "hard", "failure_attribution": "agent"},
                "trace": {"cost_usd": 0.02, "tool_calls": [{}], "hit_limit": True},
            },
        ]
        summary = summarize_arm(rows, "baseline")
        assert summary["n"] == 2
        assert summary["accuracy_overall"] == 0.5
        assert summary["accuracy_easy"] == 1.0
        assert summary["accuracy_hard"] == 0.0
        assert summary["hit_limit_rate"] == 0.5
        assert summary["attribution"] == {"clean": 1, "agent": 1}

    def test_summarize_arm_with_no_matching_rows(self):
        summary = summarize_arm([], "baseline")
        assert summary == {"arm": "baseline", "n": 0}
