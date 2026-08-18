# Agent Eval Commands

Weiser ships five commands for the agent eval workflow. All commands that read a config file support `--env-file` / `-e` (default `.env`) and `--verbose` / `-v`, the same as `weiser run` and `weiser compile`.

| Command | Purpose |
| ------- | ------- |
| `weiser eval` | Run an eval suite |
| `weiser eval-compile` | Validate eval config without executing |
| `weiser eval-synth` | Generate synthetic seed questions from a semantic layer's schema |
| `weiser eval-gate` | CI regression gate comparing a candidate run against a baseline |
| `weiser eval-calibrate` | Verify an LLM-judge metric against human labels |

## `weiser eval`

Run an agent eval suite.

```bash
weiser eval <config.yaml> --suite <name> [options]
```

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--suite` | required | Name of the `eval_suites` entry to run |
| `--split` | all | Filter goldens by split (`train` / `held_out`) |
| `--dq-mode` | `latest` | `latest`: read the most recent stored DQ check result. `live`: run the configured DQ checks now |
| `--repeats` | `1` | Run each golden this many times per arm and print a per-question variance report — use to gauge how stable a score is before trusting a single run's delta |
| `-v`, `--verbose` | off | Print parsed config to stdout |
| `-e`, `--env-file` | `.env` | Path to a custom `.env` file |

```bash
# Train split, 3 repeats for a variance report
weiser eval evals.yaml --suite lookup_tool_ablation --split train --repeats 3 -v
```

Output: a scorecard table per arm (accuracy overall/easy/hard, average turns, average cost, hit-limit rate), a failure-attribution breakdown for any failing rows, a pairwise delta table when the suite has 2+ arms, and the path to the JSONL trace substrate (`eval_results/{suite}_{split}_{run_id}.jsonl`). Flat summary rows are also written to your metric store and show up in the Weiser dashboard.

## `weiser eval-compile`

Validate the eval config sections (`agent_variants` / `semantic_layers` / `eval_suites`) and resolve their references without executing anything.

```bash
weiser eval-compile <config.yaml> [--suite <name>] [options]
```

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--suite` | all | Validate a single `eval_suites` entry instead of all |
| `-v`, `--verbose` | off | Print parsed config to stdout |
| `-e`, `--env-file` | `.env` | Path to a custom `.env` file |

```bash
weiser eval-compile evals.yaml --suite lookup_tool_ablation
```

Fails with a clear error on unknown `agent_variant`, `semantic_layer`, or `datasource` references and on suites with no goldens. Prints a warning (without blocking) when a two-arm suite's arms differ in more than one dimension — e.g. `arms 'baseline' and 'no_describe_view' differ in: tools, model -- are you testing more than one variable at once?`.

## `weiser eval-synth`

Generate synthetic seed questions from a semantic layer's schema, optionally biased toward views with a currently-failing Weiser DQ check. Writes a golden YAML file (`source: synthetic`, `split: train`) directly usable as an `eval_suite`'s `golden_set`.

```bash
weiser eval-synth <config.yaml> --semantic-layer <name> --out <path> [options]
```

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--semantic-layer` | required | Name of the `semantic_layers` entry to introspect |
| `--out` | required | Path to write the generated golden YAML file |
| `--n-per-view` | `3` | Number of synthetic questions to generate per view |
| `--views` | all | Comma-separated view names to restrict generation to |
| `--model` | `anthropic:claude-sonnet-5` | Seed-writer model — deliberately separate from both the agent under test and any judge model, to avoid contaminating grading with generation |
| `--use-dq-hints` / `--no-dq-hints` | on | Bias generated questions toward views with a currently-failing Weiser DQ check |
| `-v`, `--verbose` | off | Print parsed config to stdout |
| `-e`, `--env-file` | `.env` | Path to a custom `.env` file |

```bash
weiser eval-synth evals.yaml --semantic-layer local_sl --out evals/synthetic.yaml
```

Synthetic goldens never come with `held_out` splits or `reference_values` — both stay human-curated. Review the output before promoting anything to `held_out` or adding reference values.

## `weiser eval-gate`

CI regression gate: compares a candidate run's JSONL trace substrate against a baseline's for one arm, and exits non-zero on regression.

```bash
weiser eval-gate --candidate <run.jsonl> --baseline <baseline.jsonl> --arm <name> [options]
```

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--candidate` | required | Path to the candidate run's JSONL trace substrate |
| `--baseline` | required | Path to the baseline run's JSONL trace substrate |
| `--arm` | required | Arm name to compare (must exist in both files) |
| `--max-regression` | `3.0` | Max allowed accuracy drop, in percentage points |
| `--exclude-confounded` / `--include-confounded` | exclude | Exclude data_quality-attributed candidate rows from the regression delta — a live DQ incident shouldn't fail an unrelated agent PR |

```bash
weiser eval-gate \
  --candidate eval_results/candidate.jsonl \
  --baseline eval_results/baseline.jsonl \
  --arm baseline \
  --max-regression 3.0
```

Fails (exit code 1) if accuracy drops more than `--max-regression` percentage points, if the hit-limit rate increases at all (zero tolerance for new hard failures), or if either file has no rows for the arm. Prints a JSON summary (`passed`, `delta_pp`, `excluded_confounded_count`, `reasons`, per-arm summaries) for CI logs.

## `weiser eval-calibrate`

Score a calibration set with an `llm_judge` metric and report agreement with human labels (MAE + Spearman correlation). Exits non-zero if the correlation falls below `--threshold` — an uncalibrated judge should not silently pass.

```bash
weiser eval-calibrate <config.yaml> --suite <name> --metric-name <name> \
  --turns <turns.jsonl> --labels <labels.jsonl> [options]
```

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--suite` | required | Name of the `eval_suites` entry the metric is defined in |
| `--metric-name` | required | Name of the `llm_judge` metric within that suite to calibrate |
| `--turns` | required | Path to the calibration turns JSONL file |
| `--labels` | required | Path to the human labels JSONL file |
| `--threshold` | `0.5` | Minimum acceptable Spearman correlation against human labels |
| `--out` | `eval_results/calibration` | Directory to write the calibration report JSON to |
| `-v`, `--verbose` | off | Print parsed config to stdout |
| `-e`, `--env-file` | `.env` | Path to a custom `.env` file |

The turns file holds pre-recorded agent turns, one JSON object per line:

```json
{"turn_id": "t1", "golden": {"id": "q1", "input": "..."}, "trace": {"question": "...", "tool_calls": [], "predicted_sqls": ["..."], "final_answer": "...", "query_results": [], "hit_limit": false, "elapsed_s": 1.0, "cost_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0}}
```

The labels file holds one row per (turn, criterion):

```json
{"turn_id": "t1", "criterion": "sql_soundness", "human_score": 0.8, "human_rationale": "sound but suboptimal join", "labeled_by": "paco", "labeled_at": "2026-08-01"}
```

Typical source of turns: real held-out runs plus deliberately-injected edge cases. The report is written to `--out/{judge_prompt_version}.json` and includes the judge prompt version, so a rubric edit is never silently compared against results scored under the old rubric.
