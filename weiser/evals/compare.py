import json

from collections import defaultdict
from typing import Dict, List

from rich.console import Console
from rich.table import Table

from weiser.loader.models import AgentVariant, EvalSuite

console = Console()

VARIANT_COMPARE_FIELDS = [
    "model",
    "system_prompt",
    "tools",
    "model_settings",
    # Identity of the agent being built is itself a comparison dimension: two arms on
    # different frameworks, different adapter_class (framework: custom), or different
    # entrypoint factories are not "the same agent, one knob changed" even if every
    # other field matches.
    "framework",
    "entrypoint",
    "adapter_class",
]


def load_result_rows(path: str) -> List[dict]:
    rows = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def lint_arms(
    suite: EvalSuite,
    agent_variants: Dict[str, AgentVariant],
) -> List[str]:
    """Mirrors the "From Vibes to Evals" playbook's Section 4 warning: comparing two
    arms that differ in more than one dimension makes the resulting delta unexplainable.
    Doesn't block the run -- sometimes a multi-variable comparison is deliberate -- it
    just surfaces the trap."""
    if len(suite.arms) != 2:
        return []
    arm_a, arm_b = suite.arms
    variant_a = agent_variants.get(arm_a.agent_variant)
    variant_b = agent_variants.get(arm_b.agent_variant)
    if variant_a is None or variant_b is None:
        return []

    diffs = [
        field
        for field in VARIANT_COMPARE_FIELDS
        if getattr(variant_a, field) != getattr(variant_b, field)
    ]
    if arm_a.semantic_layer != arm_b.semantic_layer:
        diffs.append("semantic_layer")

    if len(diffs) > 1:
        return [
            f"arms '{arm_a.name}' and '{arm_b.name}' differ in: {', '.join(diffs)} -- "
            "are you testing more than one variable at once?"
        ]
    return []


def _arm_rows(rows: List[dict], arm: str) -> List[dict]:
    return [r for r in rows if r["result"]["arm"] == arm]


def summarize_arm(rows: List[dict], arm: str) -> dict:
    arm_rows = _arm_rows(rows, arm)
    if not arm_rows:
        return {"arm": arm, "n": 0}

    def _accuracy(level=None):
        subset = [
            r
            for r in arm_rows
            if level is None or r["result"]["level"] == level
        ]
        if not subset:
            return None
        clean = sum(
            1 for r in subset if r["result"]["failure_attribution"] == "clean"
        )
        return clean / len(subset)

    n = len(arm_rows)
    attribution_counts = defaultdict(int)
    for r in arm_rows:
        attribution_counts[r["result"]["failure_attribution"]] += 1

    return {
        "arm": arm,
        "n": n,
        "accuracy_overall": _accuracy(),
        "accuracy_easy": _accuracy("easy"),
        "accuracy_hard": _accuracy("hard"),
        "avg_cost_usd": sum(r["trace"]["cost_usd"] for r in arm_rows) / n,
        "avg_turns": sum(len(r["trace"]["tool_calls"]) for r in arm_rows) / n,
        "hit_limit_rate": sum(1 for r in arm_rows if r["trace"]["hit_limit"]) / n,
        "attribution": dict(attribution_counts),
    }


def _fmt_pct(value) -> str:
    return f"{value * 100:.1f}%" if value is not None else "n/a"


def print_scorecard(path: str, arm_names: List[str]) -> None:
    rows = load_result_rows(path)
    table = Table(
        "Arm",
        "N",
        "Acc (All)",
        "Acc (Easy)",
        "Acc (Hard)",
        "Avg Turns",
        "Avg Cost",
        "Hit-Limit %",
    )
    summaries = {}
    for arm in arm_names:
        s = summarize_arm(rows, arm)
        summaries[arm] = s
        table.add_row(
            arm,
            str(s["n"]),
            _fmt_pct(s.get("accuracy_overall")),
            _fmt_pct(s.get("accuracy_easy")),
            _fmt_pct(s.get("accuracy_hard")),
            f"{s['avg_turns']:.1f}" if s["n"] else "n/a",
            f"${s['avg_cost_usd']:.4f}" if s["n"] else "n/a",
            _fmt_pct(s.get("hit_limit_rate")),
        )
    console.print(table)

    for arm in arm_names:
        attribution = summaries[arm].get("attribution", {})
        failing = sum(v for k, v in attribution.items() if k != "clean")
        if failing:
            parts = ", ".join(f"{v} {k}" for k, v in attribution.items() if k != "clean")
            console.print(f"[yellow]{arm}[/yellow] failures (n={failing}): {parts}")

    if len(arm_names) >= 2:
        print_pairwise(summaries, arm_names)


def per_question_variance(rows: List[dict], arm: str) -> Dict[str, dict]:
    """Mirrors eval_harness_improvement_spec.md's FIX-7: pure aggregation over data
    already collected via `--repeats N`, no new instrumentation. A question whose score
    swings widely across identical reps is telling you something -- prompt ambiguity, a
    genuinely hard case, or judge noise -- well before it shows up as a mysterious
    regression in a later comparison."""
    arm_rows = _arm_rows(rows, arm)
    by_golden: Dict[str, List[float]] = defaultdict(list)
    for r in arm_rows:
        by_golden[r["result"]["golden_id"]].append(r["result"]["overall_score"])

    out = {}
    for golden_id, scores in by_golden.items():
        if len(scores) > 1:
            mean = sum(scores) / len(scores)
            variance = sum((s - mean) ** 2 for s in scores) / len(scores)
            out[golden_id] = {"mean": mean, "std": variance**0.5, "n": len(scores)}
    return out


def print_variance_report(path: str, arm_names: List[str]) -> None:
    rows = load_result_rows(path)
    for arm in arm_names:
        variance = per_question_variance(rows, arm)
        if not variance:
            continue
        table = Table(
            "Golden ID", "Mean", "Std Dev", "N", title=f"Repeat-run variance -- arm '{arm}'"
        )
        for golden_id, stats in sorted(variance.items(), key=lambda kv: -kv[1]["std"]):
            style = None
            if stats["std"] > 0.25:
                style = "red"
            elif stats["std"] > 0.15:
                style = "yellow"
            table.add_row(
                golden_id, f"{stats['mean']:.2f}", f"{stats['std']:.2f}", str(stats["n"]),
                style=style,
            )
        console.print(table)


def print_pairwise(summaries: dict, arm_names: List[str]) -> None:
    baseline = arm_names[0]
    baseline_summary = summaries[baseline]
    for arm in arm_names[1:]:
        s = summaries[arm]
        if (
            baseline_summary.get("accuracy_overall") is None
            or s.get("accuracy_overall") is None
        ):
            continue
        delta = (s["accuracy_overall"] - baseline_summary["accuracy_overall"]) * 100
        sign = "+" if delta >= 0 else ""
        console.print(
            f"[bold]{arm}[/bold] vs [bold]{baseline}[/bold]: {sign}{delta:.1f}pp overall accuracy"
        )
