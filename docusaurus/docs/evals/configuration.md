# Agent Eval Configuration

Agent evals are configured in the same Weiser YAML file as your data quality checks, using three new optional top-level sections: `agent_variants`, `semantic_layers`, and `eval_suites`. All existing sections (`datasources`, `checks`, `connections`, `includes`, `slack_url`) work unchanged, and environment variable templating (`{{VARIABLE_NAME}}`) applies to the new sections too.

```yaml
version: 1

datasources:
  - name: local_db
    type: postgresql
    uri: duckdb:///examples/eval_example.duckdb

checks:
  - name: merchants_not_empty
    dataset: merchants
    datasource: local_db
    type: row_count
    condition: gt
    threshold: 0

connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
    db_name: eval_example_metricstore.db

semantic_layers:
  - name: local_sl
    type: generic_sql
    datasource: local_db

agent_variants:
  - name: baseline
    framework: pydantic_ai
    entrypoint: examples.eval_agents.build_bi_agent
    system_prompt: "You are a careful BI analyst. Always verify column names before querying."
    tools: [list_views, describe_view, query, submit_answer]

  - name: no_describe_view
    framework: pydantic_ai
    entrypoint: examples.eval_agents.build_bi_agent
    system_prompt: "You are a careful BI analyst. Always verify column names before querying."
    tools: [list_views, query, submit_answer]

eval_suites:
  - name: lookup_tool_ablation
    arms:
      - name: baseline
        agent_variant: baseline
        semantic_layer: local_sl
      - name: no_describe_view
        agent_variant: no_describe_view
        semantic_layer: local_sl
    goldens:
      - id: merchant_count
        input: "How many merchants are there in total?"
        split: train
        level: easy
        reference_values:
          - metric: cnt
            expected: 3
            tolerance_pct: 0
        expected_views: [merchants]
    metrics:
      - type: schema_membership
        threshold: 1.0
      - type: reference_value_match
      - type: llm_judge
        name: sql_soundness
        threshold: 0.5
        params:
          evaluation_params: [input, predicted_sqls]
          criteria: >
            Given the question, is the submitted SQL structurally sound and a
            plausible way to answer it? Do not penalize style choices.
```

## `semantic_layers`

A semantic layer tells the harness which schema the agent may query against. It references an existing datasource by name and reuses its connection details.

| Parameter | Required | Description |
| --------- | -------- | ----------- |
| `name` | Yes | Unique identifier, referenced by `eval_suites[].arms[].semantic_layer` |
| `type` | Yes | `cube` or `generic_sql` |
| `datasource` | Yes | Name of an existing `datasources` entry |
| `meta_api_url` | No | Cube.js only — base URL of the Cube API (e.g. `http://localhost:4000`) |
| `meta_api_token` | No | Cube.js only — API token; use `{{CUBE_API_TOKEN}}` for secrets |

### Generic SQL

Introspects tables/views directly from any Weiser SQL datasource via SQLAlchemy. No extra parameters needed:

```yaml
semantic_layers:
  - name: local_sl
    type: generic_sql
    datasource: local_db
```

### Cube.js

Reads measures/dimensions from the Cube REST meta API; queries still execute through the datasource connection:

```yaml
semantic_layers:
  - name: cube_prod
    type: cube
    datasource: cube_prod_ds
    meta_api_url: "{{CUBE_META_URL}}"
    meta_api_token: "{{CUBE_API_TOKEN}}"
```

## `agent_variants`

An agent variant is a declaratively-configured agent shape. `entrypoint` is a dotted path to a factory function you write **once per agent family**; every other field is a knob the harness passes into that factory, so variants of the same agent are pure config.

| Parameter | Required | Default | Description |
| --------- | -------- | ------- | ----------- |
| `name` | Yes | — | Unique identifier, referenced by `eval_suites[].arms[].agent_variant` |
| `framework` | Yes | — | Agent framework; currently `pydantic_ai` |
| `entrypoint` | Yes | — | Dotted path to a factory function, e.g. `examples.eval_agents.build_bi_agent` |
| `model` | No | framework default | Model string, e.g. `anthropic:claude-sonnet-5` |
| `system_prompt` | No | — | Inline prompt text or a path to a prompt file (Jinja2-rendered) |
| `tools` | No | all tools | Subset of `list_views`, `describe_view`, `query`, `submit_answer` to expose |
| `model_settings` | No | — | Passed through verbatim to the framework (e.g. `temperature`) |
| `max_turns` | No | `40` | Turn budget per question; hitting it is recorded as a first-class failure category |
| `extra` | No | — | Framework-specific escape hatch for anything not modeled above |

### The entrypoint factory

For `pydantic_ai`, the entrypoint is a function that takes the variant and the (already-filtered) toolset and returns a built `Agent`. Write one per agent family and reuse it across every variant:

```python
# examples/eval_agents.py
from pydantic_ai import Agent


def build_bi_agent(variant, tools) -> Agent:
    return Agent(
        model=variant.model or "anthropic:claude-sonnet-5",
        system_prompt=variant.system_prompt or "You are a careful BI analyst.",
        tools=tools,
        model_settings=variant.model_settings,
    )
```

The tools handed to the factory are already filtered to `variant.tools` by the harness — that is what makes "compare an agent with/without a tool" a pure-YAML experiment. For a single-arm baseline of "whatever is actually in prod right now", the factory can ignore the knobs and return an already-built agent; `extra` carries anything framework-specific.

### Ablation example

The canonical use case — add a tool, compare with/without. Same factory, same prompt, only the tool set differs:

```yaml
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
```

## `eval_suites`

An eval suite pairs one or more arms against a shared set of goldens, scored by a set of metrics.

| Parameter | Required | Description |
| --------- | -------- | ----------- |
| `name` | Yes | Unique suite name, passed to `weiser eval --suite` |
| `arms` | Yes | One or more `EvalArm` entries; 2+ arms enables the pairwise comparison report |
| `golden_set` | No* | Path to an external golden file (`.yaml`, `.yml`, or `.jsonl`) |
| `goldens` | No* | Inline list of goldens; merged with `golden_set` if both are given |
| `metrics` | Yes | List of metric configurations (see [Metrics](./metrics.md)) |
| `dq_scope` | No | Explicit dataset names to use for data-quality attribution; default is auto (derived from the tables the agent's SQL touched) |

\* At least one of `golden_set` or `goldens` must provide goldens — `weiser eval-compile` fails otherwise.

### Arms

An arm is a named pairing of an agent variant with a semantic layer:

```yaml
arms:
  - name: baseline
    agent_variant: baseline
    semantic_layer: local_sl
  - name: no_describe_view
    agent_variant: no_describe_view
    semantic_layer: local_sl
```

`agent_variant` and `semantic_layer` reference entries by name; `weiser eval-compile` resolves all references and fails on unknown names.

### Goldens

A golden is a single test case. It can be declared inline in the suite or loaded from an external file via `golden_set`. External YAML files are `{goldens: [...]}`-shaped and go through the normal Weiser config loader (Jinja2 templating and `includes:` work); `.jsonl` files hold one golden dict per line for bulk, synthetic, or production-mined sets.

| Parameter | Required | Default | Description |
| --------- | -------- | ------- | ----------- |
| `id` | Yes | — | Unique identifier within the suite |
| `input` | Yes | — | The natural-language question |
| `split` | No | `train` | `train` or `held_out`; use `--split` to filter a run |
| `level` | No | `easy` | `easy` or `hard`; the scorecard reports accuracy per level |
| `source` | No | `hand_written` | `hand_written`, `synthetic`, or `production` |
| `reference_values` | No | — | Pinned expected values (see below); enables the `reference_value_match` metric |
| `reference_answer_text` | No | — | A reference natural-language answer; enables `applicable_when: reference_answer_text` judge criteria |
| `reference_source` | No | — | Provenance of the reference: `human_verified`, `independent_query`, or `unverified` |
| `expected_views` | No | — | Views the agent should touch; enables the `expected_view_recall` metric |

`reference_values` entries:

| Parameter | Required | Default | Description |
| --------- | -------- | ------- | ----------- |
| `metric` | Yes | — | Column name in the agent's final query results (or `row_count`) |
| `expected` | Yes | — | Expected value |
| `tolerance_pct` | No | `0.0` | Allowed relative deviation, e.g. `0.01` for 1% |
| `widget_index` | No | — | Which result row to read the metric from |

Keep the held-out set as its own versioned, git-committed artifact (`golden_set: evals/held_out.yaml`) so each revision stays immutable.

### Data-quality scope

`dq_scope` restricts which configured `checks:` count as data-quality attribution evidence for the suite. By default it is derived automatically: any check whose `dataset` overlaps a table the agent's SQL touched.

## Configuration Includes

The new sections merge identically to existing ones when using `includes:`, so you can keep evals in their own file:

```yaml
# main.yaml
version: 1
includes:
  - evals/agent_variants.yaml
  - evals/suites.yaml
```

## Validate Your Config

```bash
weiser eval-compile evals.yaml                      # all suites
weiser eval-compile evals.yaml --suite my_suite     # a single suite
```

This validates the new sections, resolves all `agent_variant`/`semantic_layer`/`datasource` references, checks that the suite has goldens, and runs the multi-variable lint — without executing anything.
