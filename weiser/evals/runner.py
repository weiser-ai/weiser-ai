import json
import os

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Literal, Optional

from rich.progress import Progress

from weiser.evals.adapters import AgentAdapterFactory
from weiser.evals.dataquality import attribute_failure, build_dq_context
from weiser.evals.dataset import EvalDataset
from weiser.evals.metrics import MetricFactory
from weiser.evals.models import CriterionScore, EvalResultRow, EvalTestCase
from weiser.evals.semantic_layer import SemanticLayerFactory
from weiser.evals.semantic_layer.toolset import ToolsetState, build_toolset
from weiser.loader.models import BaseConfig, EvalSuite


@dataclass
class EvalRunOutcome:
    results_by_arm: Dict[str, List[EvalResultRow]]
    results_path: str


def _overall_score(criteria: List[CriterionScore]) -> float:
    applicable = [c.score for c in criteria if c.applicable and c.score is not None]
    if not applicable:
        return 1.0
    return sum(applicable) / len(applicable)


def _write_flat_row(
    metric_store,
    datasource_name: str,
    golden_id: str,
    arm_name: str,
    rep: int,
    dq_context,
    overall_score: float,
    run_id: str,
    run_time: datetime,
    final_sql: Optional[str],
    threshold: float,
) -> None:
    """One flat row per (golden, arm, rep) into weiser's existing metrics table, so eval
    results show up in the existing dashboard with zero dashboard changes. The richer
    per-criterion / per-arm structure lives in the JSONL trace substrate instead --
    cramming it into this table would need a schema migration this phase deliberately
    defers (see docs/eval_plan.md)."""
    success = overall_score >= threshold
    rep_suffix = f"#{rep}" if rep else ""
    metric_store.insert_results(
        {
            "check_id": f"{golden_id}::{arm_name}{rep_suffix}",
            "name": f"{golden_id}[{arm_name}]{rep_suffix}",
            "datasource": datasource_name,
            "dataset": ",".join(dq_context.touched_datasets) or golden_id,
            "actual_value": overall_score,
            "condition": "ge",
            "threshold": str(threshold),
            "success": success,
            "fail": not success,
            "run_id": run_id,
            "run_time": run_time.isoformat(),
            "type": "agent_eval",
            "measure": final_sql,
        }
    )


def _append_trace_jsonl(path: str, row: EvalResultRow, trace) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a") as f:
        f.write(
            json.dumps(
                {"result": row.model_dump(mode="json"), "trace": trace.model_dump(mode="json")},
                default=str,
            )
            + "\n"
        )


async def run_eval_suite(
    run_id: str,
    suite: EvalSuite,
    config: BaseConfig,
    connections: dict,
    metric_store,
    split: Optional[str] = None,
    dq_mode: Literal["live", "latest"] = "latest",
    results_dir: str = "eval_results",
    repeats: int = 1,
    verbose: bool = False,
) -> "EvalRunOutcome":
    agent_variants = {v.name: v for v in (config.agent_variants or [])}
    semantic_layers = {s.name: s for s in (config.semantic_layers or [])}
    datasources = {d.name: d for d in config.datasources}

    dataset = EvalDataset.load(suite.golden_set, suite.goldens)
    goldens = dataset.filter(split=split)
    if not goldens:
        raise Exception(
            f"Eval suite '{suite.name}': no goldens found for split={split!r}. "
            "Check golden_set/goldens and the --split filter."
        )

    results_by_arm: Dict[str, List[EvalResultRow]] = {}
    default_threshold = suite.metrics[0].threshold if suite.metrics else 0.5
    results_path = os.path.join(
        results_dir, f"{suite.name}_{split or 'all'}_{run_id}.jsonl"
    )

    with Progress(transient=False) as progress:
        task = progress.add_task(
            f"[cyan]Running eval suite '{suite.name}'",
            total=len(suite.arms) * len(goldens) * repeats,
        )
        for arm in suite.arms:
            variant = agent_variants.get(arm.agent_variant)
            if variant is None:
                raise Exception(
                    f"Eval suite '{suite.name}', arm '{arm.name}': agent_variant "
                    f"'{arm.agent_variant}' is not configured."
                )
            sl_config = semantic_layers.get(arm.semantic_layer)
            if sl_config is None:
                raise Exception(
                    f"Eval suite '{suite.name}', arm '{arm.name}': semantic_layer "
                    f"'{arm.semantic_layer}' is not configured."
                )
            datasource = datasources.get(sl_config.datasource)
            if datasource is None:
                raise Exception(
                    f"Semantic layer '{sl_config.name}': datasource "
                    f"'{sl_config.datasource}' is not configured."
                )

            sl_adapter = SemanticLayerFactory.create(sl_config, datasource)
            schema_catalog = sl_adapter.get_schema()
            agent_adapter = AgentAdapterFactory.create(variant)
            allowed_tools = set(variant.tools) if variant.tools is not None else None
            metrics = [MetricFactory.create(mc) for mc in suite.metrics]

            arm_rows: List[EvalResultRow] = []
            for golden in goldens:
                for rep in range(repeats):
                    state = ToolsetState()
                    tools = build_toolset(sl_adapter, state, allowed_tools)
                    built_agent = agent_adapter.build(variant, tools)
                    trace = await agent_adapter.run(
                        built_agent, golden.input, state, variant.max_turns
                    )

                    test_case = EvalTestCase(
                        golden=golden,
                        arm=arm.name,
                        trace=trace,
                        schema_catalog=schema_catalog,
                    )
                    criteria = [await metric.a_measure(test_case) for metric in metrics]
                    overall = _overall_score(criteria)

                    dq_context = build_dq_context(
                        trace.predicted_sqls,
                        config.checks,
                        connections,
                        metric_store,
                        run_id,
                        dq_scope=suite.dq_scope,
                        mode=dq_mode,
                        verbose=verbose,
                    )
                    attribution = attribute_failure(
                        criteria, dq_context, overall, threshold=default_threshold
                    )
                    run_time = datetime.now()
                    row = EvalResultRow(
                        golden_id=golden.id,
                        suite=suite.name,
                        arm=arm.name,
                        rep=rep,
                        level=golden.level,
                        split=golden.split,
                        criteria=criteria,
                        dq_context=dq_context,
                        failure_attribution=attribution,
                        overall_score=overall,
                        run_id=run_id,
                        run_time=run_time,
                    )
                    _write_flat_row(
                        metric_store,
                        sl_config.datasource,
                        golden.id,
                        arm.name,
                        rep,
                        dq_context,
                        overall,
                        run_id,
                        run_time,
                        trace.predicted_sqls[-1] if trace.predicted_sqls else None,
                        default_threshold,
                    )
                    _append_trace_jsonl(results_path, row, trace)
                    arm_rows.append(row)
                    progress.update(task, advance=1)
            results_by_arm[arm.name] = arm_rows

    return EvalRunOutcome(results_by_arm=results_by_arm, results_path=results_path)
