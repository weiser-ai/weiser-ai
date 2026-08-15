import hashlib

from typing import List, Literal, Optional

from weiser.checks import CheckFactory
from weiser.evals.models import CriterionScore, DQContext
from weiser.evals.sql_util import extract_touched_datasets
from weiser.loader.models import Check

FailureAttribution = Literal["clean", "agent", "data_quality", "judge_uncertain"]


def _generate_check_id(datasource: str, check_name: str, dataset: str) -> str:
    """Mirrors weiser.checks.base.BaseCheck.generate_check_id's hash exactly, without
    needing a fully-built check instance just to look up an existing check_id."""
    encode = lambda s: str(s).encode("utf-8")
    m = hashlib.sha256()
    m.update(encode(datasource))
    m.update(encode(check_name))
    m.update(encode(dataset))
    return m.hexdigest()


def _as_list(value) -> List[str]:
    return value if isinstance(value, list) else [value]


def build_dq_context(
    predicted_sqls: List[str],
    checks: List[Check],
    connections: dict,
    metric_store,
    run_id: str,
    dq_scope: Optional[List[str]] = None,
    mode: Literal["live", "latest"] = "latest",
    verbose: bool = False,
) -> DQContext:
    """For every dataset an agent's SQL touched (or explicitly named in dq_scope), find
    weiser Checks already configured against it and pull their DQ status -- either by
    running them now (mode="live") or by reading the most recent stored result
    (mode="latest", the default, so an eval run doesn't silently multiply datasource
    load beyond the user's normal `weiser run` cadence). This is the module that makes
    data-quality confounding real instead of a hand-curated registry: it reuses the
    user's actual, live weiser DQ checks as the confounding evidence.
    """
    touched = extract_touched_datasets(predicted_sqls)
    if dq_scope:
        touched = touched | {d.lower() for d in dq_scope}

    matching_checks = [
        check
        for check in checks
        if {d.lower() for d in _as_list(check.dataset)} & touched
    ]

    dq_results: List[dict] = []
    has_failing = False
    has_stale = False

    for check in matching_checks:
        for datasource_name in _as_list(check.datasource):
            driver = connections.get(datasource_name)
            if driver is None:
                continue

            if mode == "live":
                try:
                    check_instance = CheckFactory.create_check(
                        run_id, check, driver, datasource_name, metric_store
                    )
                    results = check_instance.run(verbose)
                except Exception as e:  # noqa: BLE001
                    dq_results.append(
                        {
                            "check_name": check.name,
                            "dataset": check.dataset,
                            "success": False,
                            "actual_value": None,
                            "run_time": None,
                            "error": str(e),
                        }
                    )
                    has_failing = True
                    continue
                for r in results:
                    success = r.get("success", False)
                    dq_results.append(
                        {
                            "check_name": check.name,
                            "dataset": r.get("dataset"),
                            "success": success,
                            "actual_value": r.get("actual_value"),
                            "run_time": r.get("run_time"),
                        }
                    )
                    if not success:
                        has_failing = True
            else:  # "latest"
                for dataset in _as_list(check.dataset):
                    check_id = _generate_check_id(datasource_name, check.name, dataset)
                    recent = metric_store.get_metrics_for_check(check_id, limit=5)
                    if not recent:
                        continue
                    latest = max(recent, key=lambda r: r.run_time)
                    success = latest.success
                    dq_results.append(
                        {
                            "check_name": check.name,
                            "dataset": dataset,
                            "success": success,
                            "actual_value": latest.actual_value,
                            "run_time": latest.run_time,
                        }
                    )
                    if success is False:
                        has_failing = True

    return DQContext(
        touched_datasets=sorted(touched),
        dq_results=dq_results,
        has_failing_dq=has_failing,
        has_stale_dq=has_stale,
    )


def attribute_failure(
    criteria: List[CriterionScore],
    dq_context: DQContext,
    overall_score: float,
    threshold: float = 0.5,
) -> FailureAttribution:
    """Starting heuristic (tune after real disagreement cases -- this is explicitly a
    starting point, not a final answer, per eval_harness_improvement_spec.md's own
    caveat on the equivalent rule):
    - overall_score clears the threshold -> "clean"
    - deterministic agent-behavior checks are clean AND a touched dataset has a
      currently-failing DQ check -> "data_quality"
    - deterministic checks are clean and there's no DQ evidence -> "agent"
    - deterministic checks themselves failed -> "judge_uncertain" (isolating
      agent-reasoning-error from agent-execution-error needs deeper trajectory
      analysis, deferred to a later phase)
    """
    if overall_score >= threshold:
        return "clean"

    def _clean(criterion_name: str) -> bool:
        for c in criteria:
            if c.criterion == criterion_name:
                return (not c.applicable) or (c.score is not None and c.score >= 0.99)
        return True  # metric wasn't configured for this suite -- don't penalize its absence

    deterministic_clean = (
        _clean("schema_membership")
        and _clean("expected_view_recall")
        and _clean("step_efficiency")
    )

    if deterministic_clean and dq_context.has_failing_dq:
        return "data_quality"
    if deterministic_clean:
        return "agent"
    return "judge_uncertain"
