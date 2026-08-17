from typing import List, Literal, Optional

import sqlglot
from sqlglot.expressions import Table

from weiser.checks import CheckFactory
from weiser.evals.models import CriterionScore, DQContext
from weiser.evals.sql_util import extract_touched_datasets
from weiser.loader.models import Check

FailureAttribution = Literal["clean", "agent", "data_quality", "judge_uncertain"]


def _as_list(value) -> List[str]:
    return value if isinstance(value, list) else [value]


def _stored_dataset(dataset: str) -> str:
    """The dataset string BaseCheck.append_result stores and hashes: SQL-expression
    datasets are stored under their joined table names (e.g. 'orders_customers'),
    plain identifiers under the raw string. Mirrors weiser/checks/base.py
    parse_dataset + append_result exactly, so lookups match what `weiser run` wrote."""
    try:
        tables = list(sqlglot.parse_one(dataset).find_all(Table))
    except Exception:
        return dataset
    return "_".join(map(str, tables)) if tables else dataset


def _dataset_table_names(dataset: str) -> List[str]:
    """Plain lowercased table names a dataset references (the same granularity
    extract_touched_datasets produces), for matching checks against touched datasets
    and keying hints. Falls back to the raw string for plain identifiers."""
    try:
        tables = list(sqlglot.parse_one(dataset).find_all(Table))
    except Exception:
        tables = []
    names = [t.name.lower() for t in tables if t.name]
    return names or [dataset.lower()]


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
        if any(
            name in touched
            for dataset in _as_list(check.dataset)
            for name in _dataset_table_names(dataset)
        )
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
                    recent = metric_store.get_latest_metrics_for_check(
                        check.name, _stored_dataset(dataset), datasource_name
                    )
                    if not recent:
                        continue
                    failing = [r for r in recent if r.success is False]
                    success = not failing
                    dq_results.append(
                        {
                            "check_name": check.name,
                            "dataset": dataset,
                            "success": success,
                            "actual_value": (
                                failing[0].actual_value
                                if failing
                                else recent[0].actual_value
                            ),
                            "run_time": max(r.run_time for r in recent),
                        }
                    )
                    if not success:
                        has_failing = True

    return DQContext(
        touched_datasets=sorted(touched),
        dq_results=dq_results,
        has_failing_dq=has_failing,
        has_stale_dq=has_stale,
    )


def known_issue_hints(
    checks: List[Check],
    connections: dict,
    metric_store,
) -> "dict[str, List[str]]":
    """For every configured check, look up its most recent stored result; if failing,
    add a human-readable hint keyed by every dataset that check covers. Feeds
    synthesizer.generate_synthetic_goldens's dq_hints, biasing synthetic questions
    toward real messy edge cases -- reusing weiser's own live DQ checks as "known
    quirks" instead of a hand-maintained known_data_issues.yaml registry."""
    hints: "dict[str, List[str]]" = {}
    for check in checks:
        for datasource_name in _as_list(check.datasource):
            if datasource_name not in connections:
                continue
            for dataset in _as_list(check.dataset):
                recent = metric_store.get_latest_metrics_for_check(
                    check.name, _stored_dataset(dataset), datasource_name
                )
                if not recent:
                    continue
                failing = [r for r in recent if r.success is False]
                if not failing:
                    continue
                for table in _dataset_table_names(dataset):
                    hints.setdefault(table, []).append(
                        f"{check.name} ({check.type}) is currently failing "
                        f"(actual_value={failing[0].actual_value})"
                    )
    return hints


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
