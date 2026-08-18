import asyncio
import json
from unittest.mock import Mock, patch

from weiser.evals.runner import run_eval_suite
from weiser.loader.models import (
    AgentFramework,
    AgentVariant,
    BaseConfig,
    EvalArm,
    EvalSuite,
    MetricConfig,
    PostgreSQLDatasource,
    SemanticLayerConfig,
    SemanticLayerType,
)

from tests.fixtures.config_fixtures import *  # noqa: F401,F403


class TestEvalWorkflowIntegration:
    def test_two_arm_suite_dual_writes_and_compares(
        self, tmp_path, sample_eval_golden, mock_semantic_layer, mock_agent_adapter, mock_metric_store
    ):
        config = BaseConfig(
            checks=[],
            datasources=[
                PostgreSQLDatasource(name="local_db", uri="duckdb:///:memory:")
            ],
            semantic_layers=[
                SemanticLayerConfig(
                    name="local_sl", type=SemanticLayerType.generic_sql, datasource="local_db"
                )
            ],
            agent_variants=[
                AgentVariant(
                    name="baseline",
                    framework=AgentFramework.pydantic_ai,
                    entrypoint="tests.fixtures.stub_agents.build_agent",
                ),
                AgentVariant(
                    name="with_lookup_tool",
                    framework=AgentFramework.pydantic_ai,
                    entrypoint="tests.fixtures.stub_agents.build_agent",
                    tools=["query", "submit_answer"],
                ),
            ],
        )
        suite = EvalSuite(
            name="tool_ablation",
            arms=[
                EvalArm(name="baseline", agent_variant="baseline", semantic_layer="local_sl"),
                EvalArm(
                    name="with_lookup_tool",
                    agent_variant="with_lookup_tool",
                    semantic_layer="local_sl",
                ),
            ],
            goldens=[sample_eval_golden],
            metrics=[MetricConfig(type="reference_value_match")],
        )

        with patch(
            "weiser.evals.runner.SemanticLayerFactory.create", return_value=mock_semantic_layer
        ), patch(
            "weiser.evals.runner.AgentAdapterFactory.create", return_value=mock_agent_adapter
        ):
            outcome = asyncio.run(
                run_eval_suite(
                    "run1",
                    suite,
                    config,
                    connections={},
                    metric_store=mock_metric_store,
                    results_dir=str(tmp_path / "eval_results"),
                )
            )

        assert set(outcome.results_by_arm.keys()) == {"baseline", "with_lookup_tool"}
        assert mock_metric_store.insert_results.call_count == 2

        rows = [json.loads(line) for line in open(outcome.results_path)]
        assert len(rows) == 2
        assert {r["result"]["arm"] for r in rows} == {"baseline", "with_lookup_tool"}
        for row in rows:
            assert row["result"]["failure_attribution"] == "clean"
            assert row["result"]["overall_score"] == 1.0
            assert row["trace"]["final_answer"] == "There are 3 merchants."

    def test_missing_agent_variant_raises(self, sample_eval_golden, mock_metric_store):
        config = BaseConfig(
            checks=[],
            datasources=[
                PostgreSQLDatasource(name="local_db", uri="duckdb:///:memory:")
            ],
            semantic_layers=[
                SemanticLayerConfig(
                    name="local_sl", type=SemanticLayerType.generic_sql, datasource="local_db"
                )
            ],
            agent_variants=[],
        )
        suite = EvalSuite(
            name="broken_suite",
            arms=[EvalArm(name="baseline", agent_variant="does_not_exist", semantic_layer="local_sl")],
            goldens=[sample_eval_golden],
            metrics=[MetricConfig(type="reference_value_match")],
        )

        try:
            asyncio.run(
                run_eval_suite("run1", suite, config, connections={}, metric_store=mock_metric_store)
            )
            assert False, "expected an exception for an unresolved agent_variant"
        except Exception as e:
            assert "does_not_exist" in str(e)
