import json
from typing import List, Optional

from weiser.evals.metrics.base import BaseEvalMetric
from weiser.evals.models import CriterionScore, EvalTestCase
from weiser.evals.sql_util import extract_columns, extract_touched_datasets, extract_touched_tables


class SchemaMembershipMetric(BaseEvalMetric):
    """Deterministic: does every table (and, best-effort, unaliased table.column pair)
    referenced in the trace's predicted SQL exist in the semantic layer's schema
    catalog? Requires EvalTestCase.schema_catalog (set by the runner from the arm's
    resolved SemanticLayerAdapter). Ported from eval_harness_improvement_spec.md's
    check_sql_schema_membership."""

    def measure(self, test_case: EvalTestCase) -> CriterionScore:
        catalog = test_case.schema_catalog
        sqls = test_case.trace.predicted_sqls
        if catalog is None or not sqls:
            self.score = None
            return CriterionScore(criterion=self.name, applicable=False)

        unknown_tables = set()
        unknown_columns = set()
        for sql in sqls:
            for table in extract_touched_tables(sql):
                if table not in catalog.views:
                    unknown_tables.add(table)
            for table_ref, col in extract_columns(sql):
                view = catalog.views.get(table_ref)
                if view is not None and col not in view.members:
                    unknown_columns.add(f"{table_ref}.{col}")

        clean = not unknown_tables and not unknown_columns
        self.score = 1.0 if clean else 0.0
        rationale = (
            "all identifiers known"
            if clean
            else f"unknown tables: {sorted(unknown_tables)}, unknown columns: {sorted(unknown_columns)}"
        )
        return CriterionScore(criterion=self.name, score=self.score, rationale=rationale)


class ExpectedViewRecallMetric(BaseEvalMetric):
    """Deterministic, applicable only when golden.expected_views is set. Recall-oriented
    (did the agent touch what it needed to), not precision-oriented -- a defensive extra
    fetch isn't penalized here."""

    def measure(self, test_case: EvalTestCase) -> CriterionScore:
        expected = test_case.golden.expected_views
        if not expected:
            self.score = None
            return CriterionScore(criterion=self.name, applicable=False)

        touched = extract_touched_datasets(test_case.trace.predicted_sqls)
        expected_lower = {v.lower() for v in expected}
        missing = expected_lower - touched
        self.score = 1.0 - (len(missing) / len(expected_lower))
        rationale = (
            "all expected views touched"
            if not missing
            else f"missing expected views: {sorted(missing)}"
        )
        return CriterionScore(criterion=self.name, score=self.score, rationale=rationale)


def _extract_metric_value(rows: List[dict], metric: str) -> Optional[float]:
    if metric == "row_count":
        return float(len(rows))
    if not rows:
        return None
    first = rows[0]
    for key, value in first.items():
        if key.lower() == metric.lower():
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
    return None


def _within_tolerance(actual: float, expected: float, tolerance_pct: float) -> bool:
    if tolerance_pct <= 0:
        return actual == expected
    if expected == 0:
        return actual == 0
    return abs(actual - expected) / abs(expected) <= tolerance_pct


class ReferenceValueMatchMetric(BaseEvalMetric):
    """Deterministic, no LLM call, applicable only when golden.reference_values is set.
    Ported from eval_harness_improvement_spec.md's check_reference_values. This is the
    check that catches "queried the adjacent view, got a plausible but wrong number" --
    the exact gap SQL-plausibility judging alone cannot close."""

    def measure(self, test_case: EvalTestCase) -> CriterionScore:
        reference_values = test_case.golden.reference_values
        if not reference_values:
            self.score = None
            return CriterionScore(criterion=self.name, applicable=False)

        rows = test_case.trace.query_results or []
        if test_case.trace.final_answer is None and not rows:
            self.score = 0.0
            return CriterionScore(
                criterion=self.name, score=0.0, rationale="no output to compare"
            )

        failures = []
        for rv in reference_values:
            actual = _extract_metric_value(rows, rv.metric)
            if actual is None or not _within_tolerance(
                actual, rv.expected, rv.tolerance_pct
            ):
                failures.append(rv.metric)

        self.score = 0.0 if failures else 1.0
        rationale = (
            "all pinned values matched"
            if not failures
            else f"mismatched: {failures}"
        )
        return CriterionScore(criterion=self.name, score=self.score, rationale=rationale)


class StepEfficiencyMetric(BaseEvalMetric):
    """Deterministic duplicate-tool-call (loop) detection. Ported from
    eval_harness_improvement_spec.md's detect_repeated_calls/step_efficiency_score."""

    def measure(self, test_case: EvalTestCase) -> CriterionScore:
        tool_calls = test_case.trace.tool_calls
        if not tool_calls:
            self.score = 1.0
            return CriterionScore(criterion=self.name, score=1.0, rationale="no tool calls")

        seen = set()
        duplicates = []
        for tc in tool_calls:
            key = (tc.tool_name, json.dumps(tc.args, sort_keys=True, default=str))
            if key in seen:
                duplicates.append(f"{tc.tool_name}@turn{tc.turn}")
            seen.add(key)

        total = len(tool_calls)
        self.score = 1.0 - (len(duplicates) / total)
        rationale = f"{len(duplicates)}/{total} calls were exact duplicates"
        if duplicates:
            rationale += f": {duplicates}"
        return CriterionScore(criterion=self.name, score=self.score, rationale=rationale)


class HitLimitMetric(BaseEvalMetric):
    """Trivial pass/fail on trace.hit_limit -- keeps "agent gave up without answering"
    a first-class, visible category rather than a silently-zeroed score, mirroring
    agentic-sql-mini's Correctness.HIT_LIMIT."""

    def measure(self, test_case: EvalTestCase) -> CriterionScore:
        hit = test_case.trace.hit_limit
        self.score = 0.0 if hit else 1.0
        rationale = (
            "agent hit the turn limit without submitting an answer"
            if hit
            else "agent finished within the turn budget"
        )
        return CriterionScore(criterion=self.name, score=self.score, rationale=rationale)
