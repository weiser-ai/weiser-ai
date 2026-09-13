import asyncio

import pytest
from pydantic_ai.messages import ModelResponse, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel

from weiser.evals.metrics.llm_judge import LLMJudgeMetric
from weiser.evals.metrics.presets import (
    answer_correctness_metric_config,
    chart_type_appropriateness_metric_config,
    dashboard_composition_metric_config,
    groundedness_metric_config,
    sql_soundness_metric_config,
)
from weiser.evals.models import AgentTrace, EvalTestCase, WidgetSummary
from weiser.loader.models import EvalGolden, MetricConfig


def _scripted_judge(call_log, score=1.0, reason="ok"):
    def fn(messages, info: AgentInfo) -> ModelResponse:
        call_log.append(messages[-1].parts[-1].content)
        tool_name = info.output_tools[0].name
        return ModelResponse(
            parts=[ToolCallPart(tool_name=tool_name, args={"score": score, "reason": reason})]
        )

    return FunctionModel(fn)


def _test_case(
    input_="How many merchants?",
    final_answer="3",
    reference_answer_text=None,
    widgets=None,
    is_dashboard_turn=False,
):
    golden = EvalGolden(id="q1", input=input_, reference_answer_text=reference_answer_text)
    trace = AgentTrace(
        question=input_,
        final_answer=final_answer,
        predicted_sqls=["SELECT 1"],
        widgets=widgets or [],
        is_dashboard_turn=is_dashboard_turn,
    )
    return EvalTestCase(golden=golden, arm="baseline", trace=trace)


class TestLLMJudgeMetric:
    def test_requires_criteria_or_steps(self):
        with pytest.raises(ValueError):
            LLMJudgeMetric(MetricConfig(type="llm_judge", name="x", params={}))

    def test_rejects_both_criteria_and_steps(self):
        with pytest.raises(ValueError):
            LLMJudgeMetric(
                MetricConfig(
                    type="llm_judge",
                    name="x",
                    params={"criteria": "a", "evaluation_steps": ["b"]},
                )
            )

    def test_rejects_unknown_evaluation_param(self):
        with pytest.raises(ValueError):
            LLMJudgeMetric(
                MetricConfig(
                    type="llm_judge",
                    name="x",
                    params={"criteria": "a", "evaluation_params": ["not_a_real_field"]},
                )
            )

    def test_scores_via_structured_output(self):
        call_log = []
        metric = LLMJudgeMetric(
            MetricConfig(
                type="llm_judge",
                name="sql_soundness",
                threshold=0.5,
                params={
                    "criteria": "Is the SQL sound?",
                    "evaluation_params": ["input", "final_answer"],
                    "judge_model": _scripted_judge(call_log, score=0.9, reason="looks right"),
                },
            )
        )
        test_case = _test_case()
        cs = asyncio.run(metric.a_measure(test_case))

        assert cs.applicable is True
        assert cs.score == 0.9
        assert cs.rationale == "looks right"
        assert cs.judge_prompt_version == metric.prompt_version
        assert len(call_log) == 1
        assert "How many merchants?" in call_log[0]
        assert "3" in call_log[0]

    def test_score_is_clamped_to_unit_interval(self):
        call_log = []
        metric = LLMJudgeMetric(
            MetricConfig(
                type="llm_judge",
                name="x",
                params={
                    "criteria": "c",
                    "judge_model": _scripted_judge(call_log, score=1.7),
                },
            )
        )
        cs = asyncio.run(metric.a_measure(_test_case()))
        assert cs.score == 1.0

    def test_applicable_when_skips_llm_call(self):
        call_log = []
        metric = LLMJudgeMetric(
            MetricConfig(
                type="llm_judge",
                name="answer_correctness",
                params={
                    "criteria": "c",
                    "applicable_when": "reference_answer_text",
                    "judge_model": _scripted_judge(call_log),
                },
            )
        )
        cs = asyncio.run(metric.a_measure(_test_case(reference_answer_text=None)))
        assert cs.applicable is False
        assert cs.score is None
        assert call_log == []

    def test_applicable_when_present_calls_llm(self):
        call_log = []
        metric = LLMJudgeMetric(
            MetricConfig(
                type="llm_judge",
                name="answer_correctness",
                params={
                    "criteria": "c",
                    "applicable_when": "reference_answer_text",
                    "evaluation_params": ["final_answer", "reference_answer_text"],
                    "judge_model": _scripted_judge(call_log, score=1.0),
                },
            )
        )
        cs = asyncio.run(
            metric.a_measure(_test_case(reference_answer_text="There are 3 merchants."))
        )
        assert cs.applicable is True
        assert len(call_log) == 1

    def test_prompt_version_changes_with_criteria(self):
        call_log = []
        m1 = LLMJudgeMetric(
            MetricConfig(type="llm_judge", name="x", params={"criteria": "a", "judge_model": _scripted_judge(call_log)})
        )
        m2 = LLMJudgeMetric(
            MetricConfig(type="llm_judge", name="x", params={"criteria": "b", "judge_model": _scripted_judge(call_log)})
        )
        assert m1.prompt_version != m2.prompt_version

    def test_sync_measure_wraps_async(self):
        call_log = []
        metric = LLMJudgeMetric(
            MetricConfig(
                type="llm_judge",
                name="x",
                params={"criteria": "c", "judge_model": _scripted_judge(call_log, score=0.42)},
            )
        )
        cs = metric.measure(_test_case())
        assert cs.score == 0.42


class TestChartAndDashboardEvaluationParams:
    def test_widgets_param_renders_and_applicable_when_gates_on_it(self):
        call_log = []
        metric = LLMJudgeMetric(
            MetricConfig(
                type="llm_judge",
                name="chart_type_appropriateness",
                params={
                    "criteria": "c",
                    "applicable_when": "widgets",
                    "evaluation_params": ["input", "widgets"],
                    "judge_model": _scripted_judge(call_log, score=1.0),
                },
            )
        )
        # No widgets -> not applicable, no LLM call.
        cs = asyncio.run(metric.a_measure(_test_case(widgets=[])))
        assert cs.applicable is False
        assert call_log == []

        # A widget present -> applicable, and its summary reaches the judge prompt.
        widget = WidgetSummary(chart_type="bar", columns=["region", "cnt"], row_count=5)
        cs = asyncio.run(metric.a_measure(_test_case(widgets=[widget])))
        assert cs.applicable is True
        assert len(call_log) == 1
        assert "bar" in call_log[0]

    def test_is_dashboard_turn_param_gates_dashboard_composition(self):
        call_log = []
        metric = LLMJudgeMetric(
            MetricConfig(
                type="llm_judge",
                name="dashboard_composition",
                params={
                    "criteria": "c",
                    "applicable_when": "is_dashboard_turn",
                    "evaluation_params": ["input", "tool_calls"],
                    "judge_model": _scripted_judge(call_log, score=1.0),
                },
            )
        )
        cs = asyncio.run(metric.a_measure(_test_case(is_dashboard_turn=False)))
        assert cs.applicable is False
        assert call_log == []

        cs = asyncio.run(metric.a_measure(_test_case(is_dashboard_turn=True)))
        assert cs.applicable is True
        assert len(call_log) == 1


class TestPresets:
    def test_answer_correctness_preset_shape(self):
        config = answer_correctness_metric_config()
        assert config.type == "llm_judge"
        assert config.name == "answer_correctness"
        assert config.params["applicable_when"] == "reference_answer_text"

    def test_sql_soundness_preset_shape(self):
        config = sql_soundness_metric_config()
        assert config.name == "sql_soundness"
        assert "schema_catalog" in config.params["evaluation_params"]

    def test_groundedness_preset_shape(self):
        config = groundedness_metric_config()
        assert config.name == "groundedness"
        assert "query_results" in config.params["evaluation_params"]

    def test_chart_type_appropriateness_preset_shape(self):
        config = chart_type_appropriateness_metric_config()
        assert config.name == "chart_type_appropriateness"
        assert config.params["applicable_when"] == "widgets"
        assert "widgets" in config.params["evaluation_params"]

    def test_chart_type_appropriateness_preset_embeds_chart_rules(self):
        config = chart_type_appropriateness_metric_config(chart_rules="Prefer bar for breakdowns.")
        assert "Prefer bar for breakdowns." in config.params["criteria"]

    def test_dashboard_composition_preset_shape(self):
        config = dashboard_composition_metric_config()
        assert config.name == "dashboard_composition"
        assert config.params["applicable_when"] == "is_dashboard_turn"
        assert "tool_calls" in config.params["evaluation_params"]

    def test_presets_are_valid_llm_judge_metrics(self):
        call_log = []
        widget = WidgetSummary(chart_type="bar", columns=["region", "cnt"], row_count=5)
        for builder in (
            answer_correctness_metric_config,
            sql_soundness_metric_config,
            groundedness_metric_config,
            chart_type_appropriateness_metric_config,
            dashboard_composition_metric_config,
        ):
            config = builder()
            config.params["judge_model"] = _scripted_judge(call_log, score=1.0)
            metric = LLMJudgeMetric(config)
            asyncio.run(
                metric.a_measure(
                    _test_case(
                        reference_answer_text="There are 3 merchants.",
                        widgets=[widget],
                        is_dashboard_turn=True,
                    )
                )
            )
