import asyncio
import json

from pydantic_ai.messages import ModelResponse, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel

from weiser.evals.calibrate import calibrate_judge, spearman_correlation
from weiser.loader.models import MetricConfig


def _write_jsonl(path, rows):
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def _turn(turn_id):
    return {
        "turn_id": turn_id,
        "golden": {"id": turn_id, "input": "How many merchants?"},
        "trace": {"question": "How many merchants?", "final_answer": f"[{turn_id}] some answer"},
    }


def _label(turn_id, criterion, human_score):
    return {
        "turn_id": turn_id,
        "criterion": criterion,
        "human_score": human_score,
        "human_rationale": "test label",
        "labeled_by": "tester",
        "labeled_at": "2026-08-16",
    }


class TestSpearmanCorrelation:
    def test_perfect_positive_correlation(self):
        assert spearman_correlation([1, 2, 3, 4], [1, 2, 3, 4]) == 1.0

    def test_perfect_negative_correlation(self):
        assert spearman_correlation([1, 2, 3, 4], [4, 3, 2, 1]) == -1.0

    def test_no_variance_returns_zero(self):
        assert spearman_correlation([1, 1, 1], [1, 2, 3]) == 0.0

    def test_too_few_points_returns_zero(self):
        assert spearman_correlation([1], [1]) == 0.0


def _judge_that_matches_scores(scores_by_turn):
    def fn(messages, info: AgentInfo) -> ModelResponse:
        prompt = messages[-1].parts[-1].content
        turn_id = next(t for t in scores_by_turn if f"[{t}]" in prompt)
        tool_name = info.output_tools[0].name
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=tool_name,
                    args={"score": scores_by_turn[turn_id], "reason": "scripted"},
                )
            ]
        )

    return FunctionModel(fn)


class TestCalibrateJudge:
    def test_perfect_agreement_gives_full_correlation_and_zero_mae(self, tmp_path):
        turns_path = tmp_path / "turns.jsonl"
        labels_path = tmp_path / "labels.jsonl"
        scores = {"t1": 1.0, "t2": 0.5, "t3": 0.0}
        _write_jsonl(turns_path, [_turn(t) for t in scores])
        _write_jsonl(labels_path, [_label(t, "sql_soundness", s) for t, s in scores.items()])

        config = MetricConfig(
            type="llm_judge",
            name="sql_soundness",
            params={
                "criteria": "x",
                "evaluation_params": ["final_answer"],
                "judge_model": _judge_that_matches_scores(scores),
            },
        )

        report = asyncio.run(
            calibrate_judge(str(turns_path), str(labels_path), config, correlation_threshold=0.5)
        )
        assert report.n == 3
        assert report.mae == 0.0
        assert report.spearman_correlation == 1.0
        assert report.below_threshold is False

    def test_disagreement_flags_below_threshold(self, tmp_path):
        turns_path = tmp_path / "turns.jsonl"
        labels_path = tmp_path / "labels.jsonl"
        judge_scores = {"t1": 0.0, "t2": 0.0, "t3": 0.0}
        human_scores = {"t1": 1.0, "t2": 0.0, "t3": 1.0}
        _write_jsonl(turns_path, [_turn(t) for t in judge_scores])
        _write_jsonl(labels_path, [_label(t, "sql_soundness", s) for t, s in human_scores.items()])

        config = MetricConfig(
            type="llm_judge",
            name="sql_soundness",
            params={
                "criteria": "x",
                "evaluation_params": ["final_answer"],
                "judge_model": _judge_that_matches_scores(judge_scores),
            },
        )

        report = asyncio.run(
            calibrate_judge(str(turns_path), str(labels_path), config, correlation_threshold=0.5)
        )
        assert report.n == 3
        assert report.mae > 0
        assert report.below_threshold is True

    def test_unlabeled_turns_are_skipped(self, tmp_path):
        turns_path = tmp_path / "turns.jsonl"
        labels_path = tmp_path / "labels.jsonl"
        _write_jsonl(turns_path, [_turn("t1"), _turn("t2")])
        _write_jsonl(labels_path, [_label("t1", "sql_soundness", 1.0)])  # t2 unlabeled

        config = MetricConfig(
            type="llm_judge",
            name="sql_soundness",
            params={
                "criteria": "x",
                "evaluation_params": ["final_answer"],
                "judge_model": _judge_that_matches_scores({"t1": 1.0, "t2": 1.0}),
            },
        )

        report = asyncio.run(
            calibrate_judge(str(turns_path), str(labels_path), config, correlation_threshold=0.5)
        )
        assert report.n == 1

    def test_no_labels_for_criterion_yields_zero_n_and_below_threshold(self, tmp_path):
        turns_path = tmp_path / "turns.jsonl"
        labels_path = tmp_path / "labels.jsonl"
        _write_jsonl(turns_path, [_turn("t1")])
        _write_jsonl(labels_path, [_label("t1", "some_other_criterion", 1.0)])

        config = MetricConfig(
            type="llm_judge",
            name="sql_soundness",
            params={
                "criteria": "x",
                "evaluation_params": ["final_answer"],
                "judge_model": _judge_that_matches_scores({"t1": 1.0}),
            },
        )

        report = asyncio.run(
            calibrate_judge(str(turns_path), str(labels_path), config, correlation_threshold=0.5)
        )
        assert report.n == 0
        assert report.below_threshold is True
