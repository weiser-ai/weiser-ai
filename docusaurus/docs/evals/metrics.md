# Eval Metrics

Metrics score each (golden, arm, repeat) run. They are declared in a suite's `metrics:` list and resolved by name — no Python needed. Each metric produces a score from 0.0 to 1.0 per criterion; some metrics are only *applicable* when the golden carries the matching reference data, in which case they are skipped (and skipped LLM calls cost nothing).

The **overall score** for a run is the mean of all applicable criteria. The flat metric-store row (and the scorecard) treat a run as passing when the overall score meets the suite's threshold — the `threshold` of the suite's first metric.

```yaml
metrics:
  - type: schema_membership
    threshold: 1.0
  - type: reference_value_match
  - type: llm_judge
    name: sql_soundness
    threshold: 0.5
    params:
      evaluation_params: [input, predicted_sqls]
      criteria: "Is the SQL structurally sound and a plausible way to answer the question?"
```

## Common Parameters

| Parameter | Default | Description |
| --------- | ------- | ----------- |
| `type` | — | Metric type (see below) |
| `name` | the type | Display name; required to use the same `type` (e.g. `llm_judge`) more than once in a suite |
| `threshold` | `0.5` | Score threshold for the criterion |
| `params` | — | Type-specific configuration (used by `llm_judge`) |

## Deterministic Metrics

No LLM calls, zero cost, fully reproducible.

| Type | What it checks | Applicable when |
| ---- | -------------- | --------------- |
| `schema_membership` | Every table and (best-effort) `table.column` referenced in the agent's SQL exists in the semantic layer's schema catalog | The trace contains SQL |
| `expected_view_recall` | Recall over `golden.expected_views` — did the agent touch the views it needed to? Defensive extra fetches are not penalized | `expected_views` is set |
| `reference_value_match` | The agent's final query results match `golden.reference_values` within tolerance — catches "queried the adjacent view, got a plausible but wrong number" | `reference_values` is set |
| `step_efficiency` | Duplicate tool-call (loop) detection: `1 - duplicates / total tool calls` | Always |
| `hit_limit` | Pass/fail on whether the agent hit its turn budget without submitting an answer — keeps "agent gave up" a first-class, visible category | Always |

```yaml
metrics:
  - type: schema_membership
    threshold: 1.0
  - type: expected_view_recall
  - type: reference_value_match
  - type: step_efficiency
    threshold: 0.8
  - type: hit_limit
```

## LLM Judge

`llm_judge` is a generic declarative LLM-as-judge metric: the rubric lives entirely in `params`, so multiple judge criteria (answer correctness, SQL soundness, groundedness, ...) can be defined in one suite without any new code. The judge is asked directly for a 0.0–1.0 score via structured output.

| `params` key | Required | Description |
| ------------ | -------- | ----------- |
| `criteria` | one of | A single rubric statement. Mutually exclusive with `evaluation_steps` |
| `evaluation_steps` | one of | An ordered list of rubric steps the judge must follow in order |
| `evaluation_params` | No (default `[input, final_answer]`) | Which context fields to render for the judge — see below |
| `judge_model` | No (default `anthropic:claude-sonnet-5`) | Model used for judging |
| `applicable_when` | No | A context field name; the metric (and its LLM call) is skipped unless that field is truthy on the golden/trace |

Available `evaluation_params` values:

| Value | Source |
| ----- | ------ |
| `input` | The golden's question |
| `final_answer` | The agent's final natural-language answer |
| `predicted_sqls` | All SQL the agent ran |
| `query_results` | Rows returned by the final query |
| `reference_answer_text` | The golden's reference answer |
| `tool_calls` | The agent's tool-call history |
| `schema_catalog` | The semantic layer's view names |

**Judge-prompt versioning is built in**: every score is stamped with a hash of the rendered judge prompt, so a rubric edit is never silently compared against results scored under the old rubric. Use `weiser eval-calibrate` to verify a judge agrees with human labels before trusting it.

```yaml
metrics:
  - type: llm_judge
    name: sql_soundness
    threshold: 0.7
    params:
      criteria: "Is the SQL structurally sound given the schema, even without live execution?"
      evaluation_params: [predicted_sqls, schema_catalog]
      judge_model: anthropic:claude-sonnet-5
```

### Ready-Made Presets

Three documented, reusable shapes for the common judge criteria (the mechanical parts — schema membership, expected-view recall, reference values — are already covered by the deterministic metrics, so these stay scoped to what is genuinely subjective):

**Answer correctness** — compare the agent's substantive claims against a reference answer. Applicable only when the golden has `reference_answer_text`:

```yaml
- type: llm_judge
  name: answer_correctness
  threshold: 0.7
  params:
    applicable_when: reference_answer_text
    evaluation_params: [input, final_answer, reference_answer_text]
    criteria: >
      Compare the agent's substantive claims (numbers, trends, named entities)
      against the reference answer. Score 0.0 for a material factual or numeric
      contradiction, 1.0 when the substance matches even if phrasing differs. Do
      not penalize stylistic differences, extra correct detail, or a different but
      equally valid framing of the same facts.
```

**SQL soundness** — is the submitted SQL structurally sound and plausible, given the schema?

```yaml
- type: llm_judge
  name: sql_soundness
  threshold: 0.5
  params:
    evaluation_params: [input, predicted_sqls, schema_catalog]
    criteria: >
      Given the question and the known schema, is the submitted SQL structurally
      sound and a plausible way to answer the question? Judge plausibility only --
      you do not have live execution results here. Do not penalize style choices
      (aliasing, formatting, CTE vs. subquery).
```

**Groundedness** — does the final answer accurately reflect the query results, without fabricating or omitting material facts?

```yaml
- type: llm_judge
  name: groundedness
  threshold: 0.7
  params:
    evaluation_params: [final_answer, query_results]
    criteria: >
      Does the final natural-language answer accurately reflect the query_results
      returned, without fabricating or omitting material facts? This checks
      internal consistency only -- it does not check whether query_results are
      themselves correct.
```

LLM-judge metrics cost a real LLM call per question — comment them out for a fully deterministic, zero-cost run.

## Combining Metrics

A typical suite mixes cheap deterministic gates with one or two subjective judge criteria:

```yaml
metrics:
  - type: schema_membership
    threshold: 1.0
  - type: expected_view_recall
  - type: reference_value_match
  - type: step_efficiency
    threshold: 0.5
  - type: hit_limit
  - type: llm_judge
    name: sql_soundness
    threshold: 0.5
    params:
      evaluation_params: [input, predicted_sqls]
      criteria: >
        Given the question, is the submitted SQL structurally sound and a
        plausible way to answer it? Do not penalize style choices.
```
