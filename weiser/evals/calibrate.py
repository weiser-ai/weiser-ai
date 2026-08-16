import json

from dataclasses import dataclass
from typing import List

from weiser.evals.metrics.llm_judge import LLMJudgeMetric
from weiser.evals.models import AgentTrace, EvalTestCase
from weiser.loader.models import EvalGolden, MetricConfig


@dataclass
class CalibrationReport:
    criterion: str
    judge_prompt_version: str
    n: int
    mae: float
    spearman_correlation: float
    below_threshold: bool


def _load_jsonl(path: str) -> List[dict]:
    rows = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _rank(values: List[float]) -> List[float]:
    """Average ranks, ties broken by averaging -- the standard input to Spearman's
    rank correlation."""
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        avg_rank = (i + j) / 2 + 1
        for k in range(i, j + 1):
            ranks[order[k]] = avg_rank
        i = j + 1
    return ranks


def spearman_correlation(x: List[float], y: List[float]) -> float:
    """Pure-Python Spearman rank correlation (Pearson correlation of the ranks) --
    avoids pulling in scipy as a new dependency for one statistic."""
    n = len(x)
    if n < 2:
        return 0.0
    rx, ry = _rank(x), _rank(y)
    mean_rx, mean_ry = sum(rx) / n, sum(ry) / n
    cov = sum((a - mean_rx) * (b - mean_ry) for a, b in zip(rx, ry))
    var_x = sum((a - mean_rx) ** 2 for a in rx)
    var_y = sum((b - mean_ry) ** 2 for b in ry)
    if var_x == 0 or var_y == 0:
        return 0.0
    return cov / ((var_x * var_y) ** 0.5)


async def calibrate_judge(
    turns_path: str,
    labels_path: str,
    metric_config: MetricConfig,
    correlation_threshold: float = 0.5,
) -> CalibrationReport:
    """Mirrors eval_harness_improvement_spec.md's FIX-4. `turns_path` is a JSONL file of
    pre-recorded turns (`{"turn_id": ..., "golden": {...EvalGolden fields...}, "trace":
    {...AgentTrace fields...}}`), typically pulled from real held-out runs plus
    deliberately-injected edge cases. `labels_path` is a JSONL file of human labels, one
    row per (turn_id, criterion): `{"turn_id": ..., "criterion": ..., "human_score": ...,
    "human_rationale": ..., "labeled_by": ..., "labeled_at": ...}`.

    Runs the given llm_judge MetricConfig against every labeled turn and reports MAE and
    Spearman correlation against the human scores for that criterion -- there is no
    ground truth for what the judge's own numbers mean without this. `below_threshold`
    flags when the judge is not yet trustworthy for this criterion/prompt version;
    callers (the CLI, or FIX-6's gate) should refuse to silently pass in that case.
    """
    turns = _load_jsonl(turns_path)
    labels = _load_jsonl(labels_path)

    metric = LLMJudgeMetric(metric_config)
    criterion_name = metric.name
    labels_by_turn = {
        label["turn_id"]: label for label in labels if label["criterion"] == criterion_name
    }

    judge_scores: List[float] = []
    human_scores: List[float] = []

    for turn in turns:
        label = labels_by_turn.get(turn["turn_id"])
        if label is None:
            continue
        golden = EvalGolden(**turn["golden"])
        trace = AgentTrace(**turn["trace"])
        test_case = EvalTestCase(golden=golden, arm=turn.get("arm", "calibration"), trace=trace)
        criterion_score = await metric.a_measure(test_case)
        if criterion_score.score is None:
            continue
        judge_scores.append(criterion_score.score)
        human_scores.append(label["human_score"])

    n = len(judge_scores)
    mae = (
        sum(abs(j - h) for j, h in zip(judge_scores, human_scores)) / n
        if n
        else float("nan")
    )
    correlation = spearman_correlation(judge_scores, human_scores)

    return CalibrationReport(
        criterion=criterion_name,
        judge_prompt_version=metric.prompt_version,
        n=n,
        mae=mae,
        spearman_correlation=correlation,
        below_threshold=n == 0 or correlation < correlation_threshold,
    )
