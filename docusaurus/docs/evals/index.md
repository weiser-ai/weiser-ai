# Agent Evals

Weiser can evaluate natural-language-to-SQL / BI agents that sit on top of a semantic layer — question in, SQL + answer out. Like Weiser's own data quality checks, agent evals are configured **declaratively in YAML** (Evals-As-YAML): which tools the agent may call, which prompt and model it runs with, and the test cases themselves all live in the config file, not in code.

Agent evals are a second, complementary capability to data quality checks: checks validate that your data is trustworthy; evals validate that your agent answers questions about that data correctly.

## Core Concepts

| Concept | Config section | What it is |
| ------- | -------------- | ---------- |
| **Semantic layer** | `semantic_layers` | The schema the agent queries against — Cube.js or generic SQL introspection over any Weiser datasource |
| **Agent variant** | `agent_variants` | A declaratively-configured agent shape: framework, model, system prompt, tool set. Variants of the same agent differ by config, not by Python |
| **Arm** | `eval_suites[].arms` | A named pairing of an agent variant with a semantic layer — one point in a comparison |
| **Eval suite** | `eval_suites` | One or more arms compared against a shared set of goldens, scored by a set of metrics |
| **Golden** | `eval_suites[].goldens` / `golden_set` | A single test case: a natural-language question plus optional reference values, expected views, and metadata |
| **Metric** | `eval_suites[].metrics` | How each run is scored — deterministic checks and/or LLM-judge criteria |

The typical experiment is an **ablation**: two arms that are identical except for one variable (a tool on/off, a prompt change, a model swap) compared against the same goldens. When a suite has exactly two arms, Weiser prints a warning if the arms differ in more than one dimension — comparing arms that change multiple variables at once makes the resulting delta unexplainable.

## How a Run Works

For each arm, each golden, and each repeat:

1. The semantic layer adapter exposes a standard four-tool toolset — `list_views`, `describe_view`, `query`, `submit_answer` — filtered to the variant's `tools` list.
2. The agent (built by a per-framework factory function you write once) runs the question and produces a trace: tool calls, predicted SQL, query results, final answer, cost, and turn count.
3. Each configured metric scores the trace (0.0–1.0 per criterion).
4. Weiser's own DQ checks are wired in: the tables the agent's SQL touched are cross-referenced against your configured `checks:` (or their most recent stored results), and each failing row gets a **failure attribution** — `clean`, `agent`, `data_quality`, or `judge_uncertain`. A wrong answer caused by a known data incident is attributed to data quality, not to the agent.
5. Results are dual-written: a flat summary row per (golden, arm) into your existing metric store (visible in the Weiser dashboard with no changes), plus a full JSONL trace substrate under `eval_results/` for comparison, gating, and failure analysis.

## Typical Workflow

```bash
# 1. Validate the eval config without executing anything
weiser eval-compile evals.yaml --suite my_suite

# 2. Run the suite (train split while iterating)
weiser eval evals.yaml --suite my_suite --split train -v

# 3. Gauge score stability before trusting a delta
weiser eval evals.yaml --suite my_suite --split train --repeats 3

# 4. Generate more seed questions from the schema (review before committing)
weiser eval-synth evals.yaml --semantic-layer my_sl --out evals/synthetic.yaml

# 5. Gate a candidate run against a saved baseline in CI
weiser eval-gate --candidate candidate.jsonl --baseline baseline.jsonl --arm baseline

# 6. Verify an LLM-judge metric agrees with human labels
weiser eval-calibrate evals.yaml --suite my_suite --metric-name sql_soundness \
  --turns turns.jsonl --labels labels.jsonl
```

## Results

- **Scorecard**: per-arm accuracy (overall / easy / hard), average turns, average cost, hit-limit rate, and a failure-attribution breakdown.
- **Pairwise delta**: with 2+ arms, the overall-accuracy delta of each arm versus the first arm.
- **Variance report**: with `--repeats N`, per-question mean/std across identical repeats — a question whose score swings widely is telling you something (prompt ambiguity, a genuinely hard case, or judge noise) before it shows up as a mysterious regression later.
- **Trace substrate**: `eval_results/{suite}_{split}_{run_id}.jsonl` — one JSON line per (golden, arm, repeat) containing the full result row and agent trace. This is what `eval-gate` reads.

## Documentation

- [Configuration](./configuration.md) — the `agent_variants`, `semantic_layers`, and `eval_suites` sections in detail
- [Commands](./commands.md) — `weiser eval`, `eval-compile`, `eval-synth`, `eval-gate`, `eval-calibrate`
- [Metrics](./metrics.md) — deterministic metrics, the LLM judge, and ready-made judge presets
