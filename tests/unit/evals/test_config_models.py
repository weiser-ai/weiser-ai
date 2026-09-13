import yaml

from weiser.loader.config import load_config, update_namespace
from weiser.loader.models import BaseConfig


TOOL_ABLATION_YAML = """
version: 1
datasources:
  - name: local_db
    type: postgresql
    uri: duckdb:///merchants.duckdb

connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
    db_name: metricstore.db

semantic_layers:
  - name: local_sl
    type: generic_sql
    datasource: local_db

agent_variants:
  - name: baseline
    framework: pydantic_ai
    entrypoint: myapp.eval_agents.build_bi_agent
    model: anthropic:claude-sonnet-5
    system_prompt: prompts/bi_system.md
    tools: [list_views, describe_view, query, submit_answer]

  - name: with_lookup_tool
    framework: pydantic_ai
    entrypoint: myapp.eval_agents.build_bi_agent
    model: anthropic:claude-sonnet-5
    system_prompt: prompts/bi_system.md
    tools: [list_views, describe_view, query, submit_answer, lookup_customer]

eval_suites:
  - name: lookup_tool_ablation
    arms:
      - {name: baseline, agent_variant: baseline, semantic_layer: local_sl}
      - {name: with_lookup_tool, agent_variant: with_lookup_tool, semantic_layer: local_sl}
    golden_set: evals/bi_questions.yaml
    metrics:
      - {type: schema_membership, threshold: 1.0}
      - {type: reference_value_match}
      - {type: step_efficiency, threshold: 0.8}
"""


INLINE_GOLDENS_YAML = """
version: 1
datasources:
  - name: local_db
    type: postgresql
    uri: duckdb:///merchants.duckdb

connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
    db_name: metricstore.db

semantic_layers:
  - name: local_sl
    type: generic_sql
    datasource: local_db

agent_variants:
  - name: baseline
    framework: pydantic_ai
    entrypoint: myapp.eval_agents.build_bi_agent

eval_suites:
  - name: quick_smoke_test
    arms: [{name: baseline, agent_variant: baseline, semantic_layer: local_sl}]
    goldens:
      - id: q1
        input: "What was total revenue in Q1 2026?"
        split: train
        level: easy
        reference_values: [{metric: total_revenue, expected: 128400.50, tolerance_pct: 0.01}]
      - id: q2
        input: "Which merchants had no transactions last month?"
        split: held_out
        level: hard
        expected_views: [merchants, payments]
    metrics: [{type: reference_value_match}, {type: expected_view_recall}]
"""


class TestEvalConfigParsing:
    def test_tool_ablation_config_round_trips(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(TOOL_ABLATION_YAML)

        raw = load_config(str(cfg_file), verbose=False)
        config = BaseConfig(**raw)

        assert len(config.agent_variants) == 2
        assert config.agent_variants[0].tools == [
            "list_views",
            "describe_view",
            "query",
            "submit_answer",
        ]
        assert config.agent_variants[1].tools[-1] == "lookup_customer"

        assert len(config.eval_suites) == 1
        suite = config.eval_suites[0]
        assert len(suite.arms) == 2
        assert suite.arms[0].agent_variant == "baseline"
        assert suite.arms[1].agent_variant == "with_lookup_tool"
        assert suite.golden_set == "evals/bi_questions.yaml"
        assert suite.metrics[0].type == "schema_membership"
        assert suite.metrics[0].threshold == 1.0

        assert len(config.semantic_layers) == 1
        assert config.semantic_layers[0].type == "generic_sql"

    def test_inline_goldens_config_round_trips(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(INLINE_GOLDENS_YAML)

        raw = load_config(str(cfg_file), verbose=False)
        config = BaseConfig(**raw)

        suite = config.eval_suites[0]
        assert len(suite.goldens) == 2
        assert suite.goldens[0].id == "q1"
        assert suite.goldens[0].reference_values[0].expected == 128400.50
        assert suite.goldens[1].split == "held_out"
        assert suite.goldens[1].level == "hard"
        assert suite.goldens[1].expected_views == ["merchants", "payments"]

    def test_config_without_checks_or_datasources_still_parses(self):
        """An eval-only config shouldn't require a `checks:`/`datasources:` section."""
        config = BaseConfig(
            eval_suites=[],
        )
        assert config.checks == []
        assert config.datasources == []

    def test_update_namespace_merges_new_eval_keys(self):
        namespace = {"checks": [], "datasources": [], "agent_variants": [{"name": "a"}]}
        new_file = {"agent_variants": [{"name": "b"}], "semantic_layers": [{"name": "sl"}]}
        merged = update_namespace(namespace, new_file, verbose=False)
        assert len(merged["agent_variants"]) == 2
        assert merged["semantic_layers"] == [{"name": "sl"}]

    def test_custom_framework_variant_round_trips(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(
            """
version: 1
agent_variants:
  - name: baseline
    framework: custom
    adapter_class: myapp.eval_adapter.MyRouterAdapter
    entrypoint: myapp.eval_agents.build_router_config

eval_suites:
  - name: smoke
    arms: [{name: baseline, agent_variant: baseline, semantic_layer: local_sl}]
    goldens:
      - id: q1
        input: "How many merchants are there?"
        extra: {tags: ["smoke"], reference_semantic_sql: "SELECT 1"}
    metrics: [{type: reference_value_match}]
"""
        )
        raw = load_config(str(cfg_file), verbose=False)
        config = BaseConfig(**raw)

        variant = config.agent_variants[0]
        assert variant.framework == "custom"
        assert variant.adapter_class == "myapp.eval_adapter.MyRouterAdapter"
        assert variant.entrypoint == "myapp.eval_agents.build_router_config"

        golden = config.eval_suites[0].goldens[0]
        assert golden.extra == {"tags": ["smoke"], "reference_semantic_sql": "SELECT 1"}
