from dataclasses import dataclass, field
from typing import List

from weiser.evals.compare import load_result_rows, summarize_arm


@dataclass
class GateResult:
    passed: bool
    candidate_summary: dict
    baseline_summary: dict
    delta_pp: float
    excluded_confounded_count: int
    reasons: List[str] = field(default_factory=list)


def gate(
    candidate_path: str,
    baseline_path: str,
    arm: str,
    max_regression_pp: float = 3.0,
    exclude_confounded: bool = True,
) -> GateResult:
    """Mirrors eval_harness_improvement_spec.md's FIX-6 CI/CD regression gate. Fails if
    mean accuracy drops more than `max_regression_pp` percentage points, or if the
    hit-limit rate increases at all (zero tolerance for new hard failures). Rows
    attributed to "data_quality" are segmented out of the regression delta by default --
    a live DQ incident shouldn't fail an unrelated agent PR -- and reported separately
    via `excluded_confounded_count` so the exclusion is visible, not silent."""
    candidate_rows = load_result_rows(candidate_path)
    baseline_rows = load_result_rows(baseline_path)

    excluded_count = 0
    if exclude_confounded:
        kept = []
        for r in candidate_rows:
            if (
                r["result"]["arm"] == arm
                and r["result"]["failure_attribution"] == "data_quality"
            ):
                excluded_count += 1
                continue
            kept.append(r)
        candidate_rows = kept

    candidate_summary = summarize_arm(candidate_rows, arm)
    baseline_summary = summarize_arm(baseline_rows, arm)

    reasons: List[str] = []
    passed = True

    if candidate_summary["n"] == 0:
        reasons.append("candidate has zero rows for this arm")
        passed = False
    if baseline_summary["n"] == 0:
        reasons.append("baseline has zero rows for this arm")
        passed = False

    delta_pp = 0.0
    if passed:
        cand_acc = candidate_summary.get("accuracy_overall") or 0.0
        base_acc = baseline_summary.get("accuracy_overall") or 0.0
        delta_pp = (cand_acc - base_acc) * 100
        if delta_pp < -max_regression_pp:
            passed = False
            reasons.append(
                f"accuracy regressed {delta_pp:.1f}pp, exceeds "
                f"--max-regression={max_regression_pp}pp"
            )

        cand_hit = candidate_summary.get("hit_limit_rate") or 0.0
        base_hit = baseline_summary.get("hit_limit_rate") or 0.0
        if cand_hit > base_hit:
            passed = False
            reasons.append(
                f"hit-limit rate increased ({base_hit * 100:.1f}% -> {cand_hit * 100:.1f}%), "
                "zero tolerance for new hard failures"
            )

    return GateResult(
        passed=passed,
        candidate_summary=candidate_summary,
        baseline_summary=baseline_summary,
        delta_pp=delta_pp,
        excluded_confounded_count=excluded_count,
        reasons=reasons,
    )
