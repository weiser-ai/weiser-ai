from weiser.evals.compare import lint_arms, summarize_arm
from weiser.loader.models import AgentFramework, AgentVariant, EvalArm, EvalSuite

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

    def test_custom_adapter_class_diff_counts_as_a_variable(self):
        """Two `framework: custom` arms that also differ in model must be flagged --
        adapter_class identity is itself a comparison dimension, not something the lint
        can ignore just because every AgentVariant field PydanticAIAdapter cares about
        (tools/system_prompt/model_settings) happens to match."""
        suite = EvalSuite(
            name="custom_ablation",
            arms=[
                EvalArm(name="router_a", agent_variant="router_a", semantic_layer="sl"),
                EvalArm(name="router_b", agent_variant="router_b", semantic_layer="sl"),
            ],
            metrics=[],
        )
        variants = {
            "router_a": AgentVariant(
                name="router_a",
                framework=AgentFramework.custom,
                entrypoint="myapp.eval_agents.build_router_config",
                adapter_class="myapp.eval_adapter.AdapterOne",
                model="model-a",
            ),
            "router_b": AgentVariant(
                name="router_b",
                framework=AgentFramework.custom,
                entrypoint="myapp.eval_agents.build_router_config",
                adapter_class="myapp.eval_adapter.AdapterTwo",
                model="model-b",
            ),
        }
        warnings = lint_arms(suite, variants)
        assert len(warnings) == 1
        assert "adapter_class" in warnings[0]
        assert "model" in warnings[0]

    def test_custom_adapter_class_diff_alone_is_quiet(self):
        """A single-variable adapter_class swap (same model/tools/etc.) should not
        warn -- mirrors test_single_variable_diff_is_quiet for the new field."""
        suite = EvalSuite(
            name="custom_ablation",
            arms=[
                EvalArm(name="router_a", agent_variant="router_a", semantic_layer="sl"),
                EvalArm(name="router_b", agent_variant="router_b", semantic_layer="sl"),
            ],
            metrics=[],
        )
        variants = {
            "router_a": AgentVariant(
                name="router_a",
                framework=AgentFramework.custom,
                entrypoint="myapp.eval_agents.build_router_config",
                adapter_class="myapp.eval_adapter.AdapterOne",
                model="model-a",
            ),
            "router_b": AgentVariant(
                name="router_b",
                framework=AgentFramework.custom,
                entrypoint="myapp.eval_agents.build_router_config",
                adapter_class="myapp.eval_adapter.AdapterTwo",
                model="model-a",
            ),
        }
        assert lint_arms(suite, variants) == []


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
