# Agent Eval Framework for Weiser (`weiser/evals/`) — Evals-As-YAML

## Context

Weiser is a data-quality (DQ) framework: it runs configurable checks (row_count, sum,
anomaly, not_empty, etc.) against datasources and stores results in a metric store. The
goal of this work is to add a **second, complementary capability**: evaluating NL-to-SQL /
BI agents (question in, SQL + answer out) that sit on top of a semantic layer — configured
declaratively, the same way weiser's own `checks:` are declared in YAML rather than in
code.

The motivating research:

- **MotherDuck's "From Vibes to Evals" playbook** (`~/FromVibestoEvalspdf.pdf`) — a
  7-step eval methodology (question set → stabilize data → baseline → apply lever → train
  the lever → iterate the recipe → close the outer loop with production usage), validated
  on `agentic-sql-mini` against the DABstep benchmark. Central concepts carried forward:
  a **"lever"/"arm"** is whatever single variable you're testing (tools on/off, prompt
  variant, schema naming, model swap — "whatever decision is actually in front of you"),
  train/held-out split, Easy/Hard slicing, three scorecard dimensions (accuracy, speed,
  cost), and the hard-won discipline of **never batching more than one changed variable
  into a single arm-vs-arm comparison** (Step 4's "batching changes" trap, Section 4's
  sign-reversal war story).
- **`agentic-sql-mini`** (`/home/paco/labs/projects/agentic-sql-mini`) — a working,
  minimal harness proving this exact "arm" concept: `schemas/baseline.sql` vs.
  `schemas/explicit.sql` (identical data, only column/table naming differs) swing
  Hard-slice accuracy 3-4x, run through `asm evaluate --arm X` / `asm compare
  baseline.jsonl explicit.jsonl`. Also: a `RunState`/`AgentRun` trace model, a
  DABstep-specific `score()`, and a `controllog` observability ledger.
- **`eval_harness_improvement_spec.md`** — a gap-analysis against DeepEval for an
  already-shipped PydanticAI agent. FIX-9 ("data-quality confounding and failure
  attribution") is the direct ancestor of this framework's core differentiator.
- **DeepEval** (deepeval.com/docs) — architecture to reach parity with: `Golden` →
  `LLMTestCase` → `EvaluationDataset`, a `BaseMetric` contract, declarative LLM-judge
  metrics (`GEval`: criteria/evaluation_steps/evaluation_params — i.e. **metrics
  themselves are YAML-expressible, not just Python classes**), deterministic
  decision-tree metrics (`DAGMetric`), tool/trajectory metrics, a `Synthesizer`, and
  `deepeval test run` for CI gating.
- **Weiser's own architecture** — three parallel factory patterns (`CheckFactory`,
  `DriverFactory`, `MetricStoreFactory`), a `BaseCheck` template-method lifecycle, and a
  Pydantic `AnyDatasource` discriminated union, all driven entirely by YAML with Jinja2
  templating (`weiser/loader/config.py`). Confirmed gap: no schema-introspection or
  semantic-layer metadata client exists anywhere today.

**The differentiator vs. DeepEval and vs. the spec's FIX-9:** weiser already *has* a
working DQ check engine. Instead of hand-curating a `known_data_issues.yaml` registry or
writing independent shadow SQL per question, this framework extracts the tables/views an
agent's SQL touched and directly runs/queries **weiser's own configured checks**
(not_empty, anomaly, row_count) against them, live. Failure attribution (agent vs. data
quality) becomes close to free wherever weiser DQ checks are already configured.

**User decisions locked in for this design:**
1. Lives inside this repo as a new module: `weiser/evals/` (not a standalone package).
2. Two semantic-layer adapters ship in Phase 1: **Cube.js** (real meta API) and
   **generic SQL introspection** (SQLAlchemy `Inspector`). The adapter protocol must
   support adding **Snowflake Semantic Views** later without redesign (documented as
   planned, not built now).
3. **PydanticAI** is the reference agent adapter (matches the user's existing production
   agent). The adapter protocol must support adding **Strands (AWS)** later (documented
   as planned, not built now).
4. Data-quality confounding wires into **real, live weiser `CheckFactory` checks** from
   Phase 1.
5. **Evals-As-YAML (added in review):** the *design variables* of an eval — which tools
   an agent may call, which prompt it runs with, which model it uses, and the test
   cases/goldens themselves — must be expressible entirely in the eval YAML, not hidden
   in Python. Python is only needed for framework-specific glue (how to construct *a*
   PydanticAI `Agent` object at all); everything about *which* agent variant to build is
   config. This reframes "agent target" into **"agent variant"** and "eval suite" into a
   **set of arms** compared against a shared golden set — directly generalizing
   `agentic-sql-mini`'s baseline-vs-explicit pattern to N declarative arms, and directly
   generalizing DeepEval's `GEval` (declarative per-metric criteria) to full suite
   definitions.

---

## Module layout (new)

```
weiser/evals/
  __init__.py
  models.py              # AgentTrace, ToolCall, EvalGolden, EvalTestCase, CriterionScore, DQContext, EvalResultRow, AgentVariant, EvalArm
  adapters/
    __init__.py           # AgentAdapterFactory (dict dispatch, mirrors CheckFactory)
    base.py                # AgentAdapter Protocol: build(variant, toolset) -> BuiltAgent; run(built_agent, question, max_turns) -> AgentTrace
    pydantic_ai.py          # PydanticAIAdapter
                            # (strands.py: planned, not built in this phase)
  semantic_layer/
    __init__.py            # SemanticLayerFactory (dict dispatch)
    base.py                 # SemanticLayerAdapter Protocol, SchemaCatalog/SchemaView models
    cube.py                  # CubeSemanticLayer (real /cubejs-api/v1/meta client + reuses BaseDriver for execute_query)
    generic_sql.py            # GenericSQLSemanticLayer (SQLAlchemy Inspector + reuses BaseDriver)
    toolset.py                 # builds the standard 4-tool toolset (list_views/describe_view/query/submit_answer) from any SemanticLayerAdapter
                                # (snowflake_semantic_views.py: planned, not built in this phase)
  metrics/
    __init__.py              # MetricFactory (dict dispatch on MetricConfig.type) — 4th parallel factory, matches weiser's existing idiom
    base.py                   # BaseEvalMetric contract (score/threshold/reason/success, measure/a_measure)
    deterministic.py           # SchemaMembershipMetric, ExpectedViewRecallMetric, ReferenceValueMatchMetric, StepEfficiencyMetric (loop detection), HitLimitMetric
                                # (llm_judge.py — GEval-style declarative judge, YAML-configured criteria/evaluation_steps — is Phase 2)
  dataquality.py            # touched-table extraction, DQContext builder (wired to CheckFactory + metric store), attribute_failure()
  dataset.py                 # EvalGolden loading — inline YAML, external YAML file, or JSONL — via weiser.loader.config.load_config + a JSONL reader
  compare.py                  # cross-arm comparison report (mirrors agentic-sql-mini's asm compare), + the "batching changes" lint
  runner.py                   # run_eval_suite(): orchestration mirroring weiser/runner/__init__.py, now arm-aware
```

---

## Config schema (`weiser/loader/models.py`) — the YAML surface

```python
class AgentFramework(str, Enum):
    pydantic_ai = "pydantic_ai"
    # strands = "strands"  # planned

class SemanticLayerType(str, Enum):
    cube = "cube"
    generic_sql = "generic_sql"
    # snowflake_semantic_view = "snowflake_semantic_view"  # planned

class AgentVariant(BaseModel):
    """One declaratively-configured agent shape. `entrypoint` is a dotted path to a
    factory function the user writes ONCE per framework/agent-family; everything else
    on this model is a knob the harness passes into that factory so that *variants* of
    the same agent are pure config, not new Python."""
    name: str
    framework: AgentFramework
    entrypoint: str                      # dotted path: (AgentVariant, ToolSet) -> BuiltAgent
    model: Optional[str] = None           # e.g. "anthropic:claude-sonnet-5"; passed through to the framework's model resolution (OpenRouter/LiteLLM strings work too)
    system_prompt: Optional[str] = None    # inline text OR a file path (resolved relative to the config file, Jinja2-rendered like the rest of weiser's YAML)
    tools: Optional[List[str]] = None       # subset of the semantic-layer toolset's tool names; None = all tools enabled
    model_settings: Optional[dict] = None    # temperature, reasoning_effort, etc. — passed through verbatim to the framework
    max_turns: int = 40
    extra: Optional[dict] = None              # framework-specific escape hatch for anything not modeled above

class SemanticLayerConfig(BaseModel):
    name: str
    type: SemanticLayerType
    datasource: str                       # references an existing Datasource by name (reuses connection info)
    meta_api_url: Optional[str] = None      # cube-specific
    meta_api_token: Optional[str] = None    # cube-specific

class ReferenceValue(BaseModel):
    metric: str
    expected: float
    tolerance_pct: float = 0.0
    widget_index: Optional[int] = None

class EvalGolden(BaseModel):
    """A single test case / 'seed', matching agentic-sql-mini's terminology. Can be
    declared inline in the suite YAML or loaded from an external YAML/JSONL file — the
    playbook's Step 2 guidance is that the held-out set should be its own versioned,
    git-committed artifact, so external-file goldens are the expected path for anything
    beyond a handful of examples."""
    id: str
    input: str
    split: Literal["train", "held_out"] = "train"
    level: Literal["easy", "hard"] = "easy"
    source: Literal["hand_written", "synthetic", "production"] = "hand_written"
    reference_values: Optional[List[ReferenceValue]] = None
    reference_answer_text: Optional[str] = None
    reference_source: Optional[Literal["human_verified", "independent_query", "unverified"]] = None
    expected_views: Optional[List[str]] = None

class MetricConfig(BaseModel):
    """Declarative metric selection/configuration — deliberately mirrors DeepEval's
    GEval shape so Phase 2's LLM-judge metrics can be defined entirely in YAML
    (criteria/evaluation_steps go in `params`), not just Phase 1's deterministic ones."""
    type: str                    # resolved via MetricFactory, e.g. "schema_membership", "reference_value_match", "llm_judge"
    name: Optional[str] = None    # display name override; required when using the same `type` (e.g. llm_judge) more than once in a suite
    threshold: float = 0.5
    params: Optional[dict] = None  # type-specific config, e.g. {criteria: "...", evaluation_params: [...]} for llm_judge

class EvalArm(BaseModel):
    """One point in the comparison: a named pairing of an agent variant with a semantic
    layer. A suite with 2+ arms gets a cross-arm comparison report for free."""
    name: str
    agent_variant: str      # references AgentVariant by name
    semantic_layer: str      # references SemanticLayerConfig by name

class EvalSuite(BaseModel):
    name: str
    arms: List[EvalArm]                       # 1+ arms; 2+ enables weiser eval compare
    golden_set: Optional[str] = None            # path to an external YAML or JSONL golden file (glob/includes-compatible)
    goldens: Optional[List[EvalGolden]] = None    # inline goldens; merged with golden_set if both given
    metrics: List[MetricConfig]
    dq_scope: Optional[List[str]] = None          # explicit dataset names to confound-check; None = "auto" (derive from touched tables)
```

`BaseConfig` gains `agent_variants`, `semantic_layers`, `eval_suites` (all
`Optional[List[...]] = None`), mirroring `checks`/`datasources`/`connections`.
`weiser/loader/config.py:update_namespace` (`config.py:14-29`) needs the same three keys
added to its merge logic so `includes:` works identically to existing config sections.

### Example: the exact "add a tool, compare with/without" use case

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
    entrypoint: myapp.eval_agents.build_bi_agent   # same factory
    model: anthropic:claude-sonnet-5
    system_prompt: prompts/bi_system.md             # same prompt — only the tool set differs
    tools: [list_views, describe_view, query, submit_answer, lookup_customer]

semantic_layers:
  - name: cube_prod
    type: cube
    datasource: cube_prod_ds
    meta_api_url: "{{CUBE_META_URL}}"
    meta_api_token: "{{CUBE_API_TOKEN}}"

eval_suites:
  - name: lookup_tool_ablation
    arms:
      - {name: baseline, agent_variant: baseline, semantic_layer: cube_prod}
      - {name: with_lookup_tool, agent_variant: with_lookup_tool, semantic_layer: cube_prod}
    golden_set: evals/bi_questions.yaml
    metrics:
      - {type: schema_membership, threshold: 1.0}
      - {type: reference_value_match}
      - {type: step_efficiency, threshold: 0.8}
```

`weiser eval-compile` performs a **"batching changes" lint** (mirrors the playbook's
Section 4 sign-reversal lesson directly): when a suite has exactly 2 arms, diff their
resolved `AgentVariant`s field-by-field (tools/system_prompt/model/model_settings) plus
whether `semantic_layer` differs, and print a warning if more than one dimension changed
— "arms 'baseline' and 'with_lookup_tool' differ in: tools, model — are you testing more
than one variable at once?" This doesn't block the run (sometimes that's deliberate), it
just surfaces the trap the PDF spent a whole section on.

### Example: inline goldens (small hand-curated suite, no external file needed)

```yaml
eval_suites:
  - name: quick_smoke_test
    arms: [{name: baseline, agent_variant: baseline, semantic_layer: cube_prod}]
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
```

Large/bulk sets (synthetic-expanded, production-mined — playbook Step 1 Phases 2-3) use
`golden_set: evals/held_out.jsonl` instead — `weiser/evals/dataset.py` picks the parser by
file extension (`.yaml`/`.yml` → list of `EvalGolden` dicts via `load_config`'s Jinja2 +
includes machinery; `.jsonl` → one `EvalGolden` per line). Both forms validate through the
same `EvalGolden` Pydantic model, so nothing downstream cares which form was used.

---

## Core models (`weiser/evals/models.py`)

```python
class ToolCall(BaseModel):
    tool_name: str
    args: dict
    result: Any | None = None
    turn: int

class AgentTrace(BaseModel):
    question: str
    tool_calls: list[ToolCall]
    predicted_sqls: list[str]
    final_answer: str | None
    query_results: list[dict] | None   # rows returned by the final/submitted query, for reference-value checks
    hit_limit: bool = False
    elapsed_s: float
    cost_usd: float
    prompt_tokens: int
    completion_tokens: int
    error: str | None = None

class CriterionScore(BaseModel):
    criterion: str
    score: float | None = None
    applicable: bool = True
    rationale: str | None = None

class DQContext(BaseModel):
    touched_datasets: list[str]
    dq_results: list[dict]      # {check_name, dataset, success, actual_value, run_time}
    has_failing_dq: bool
    has_stale_dq: bool

class EvalTestCase(BaseModel):
    golden: EvalGolden
    arm: str
    trace: AgentTrace

class EvalResultRow(BaseModel):
    golden_id: str
    suite: str
    arm: str
    criteria: list[CriterionScore]          # deterministic + (later) LLM-judge criteria, one flat list
    dq_context: DQContext
    failure_attribution: Literal["clean", "agent", "data_quality", "judge_uncertain"]
    overall_score: float
    run_id: str
    run_time: datetime
```

This mirrors `agentic-sql-mini`'s `AgentRun`/`ScoreResult` (typed error surfaces,
first-class `hit_limit`) and DeepEval's `LLMTestCase`/`CriterionScore` shape (the spec
file's `CriterionScore.applicable` pattern), with `arm` threaded through every layer so
comparison reporting never has to reconstruct which run produced which row.

---

## Semantic layer adapters (`weiser/evals/semantic_layer/`)

```python
class SchemaView(BaseModel):
    name: str
    members: set[str]           # measure/dimension/column names, lowercased

class SchemaCatalog(BaseModel):
    views: dict[str, SchemaView]
    has_semantics: bool          # True for a real semantic layer (measures/dimensions); False for raw SQL introspection
    fetched_at: datetime

class SemanticLayerAdapter(Protocol):
    def get_schema(self) -> SchemaCatalog: ...
    def execute_query(self, sql: str) -> list[dict]: ...
    def get_freshness(self, view: str) -> datetime | None: ...   # optional; return None if unsupported
```

- **`CubeSemanticLayer`**: `get_schema()` calls Cube's REST meta API
  (`{meta_api_url}/cubejs-api/v1/meta`) and parses `cubes[].measures`/`.dimensions` into a
  `SchemaCatalog` — genuinely new (today's `CubeDriver` in `weiser/drivers/postgres.py:8-9`
  is a dead-code empty subclass; Cube is only ever hit via the Postgres wire protocol per
  `weiser/drivers/base.py:48-51`). `execute_query()` reuses the *existing*
  `BaseDriver`/`DriverFactory` machinery unchanged.
- **`GenericSQLSemanticLayer`**: `get_schema()` uses SQLAlchemy
  `Inspector.from_engine(driver.engine)` against any existing weiser SQL driver, setting
  `has_semantics=False`. `execute_query()` also reuses `BaseDriver.execute_query`.
- Both adapters are constructed from a `SemanticLayerConfig` + the `Datasource` it
  references, via `DriverFactory.create_driver` — they sit on top of weiser's existing
  driver layer, they don't replace it.
- **`SnowflakeSemanticViewsAdapter` — planned, not implemented this phase.** Stub file
  `semantic_layer/snowflake_semantic_views.py` with a `NotImplementedError` and a comment
  pointing at `DESCRIBE SEMANTIC VIEW` as the eventual `get_schema()` source.

`SemanticLayerFactory` (`semantic_layer/__init__.py`) mirrors `CheckFactory`/`DriverFactory`:
a `SEMANTIC_LAYER_MAP: Dict[SemanticLayerType, Type[SemanticLayerAdapter]]` dict + a static
`create(config, datasource) -> SemanticLayerAdapter` method.

`semantic_layer/toolset.py` builds the standard 4 tools (`list_views`, `describe_view`,
`query`, `submit_answer` — modeled on `agentic-sql-mini`'s `src/agent.py:297-366`) from any
`SemanticLayerAdapter`. `AgentVariant.tools` (a list of names) is filtered against this
toolset by the runner *before* handing tools to the adapter's `build()` — filtering is
generic harness logic, not per-framework code, which is what makes "add a tool, compare
with/without" a pure-YAML experiment.

---

## Agent adapters (`weiser/evals/adapters/`)

```python
class BuiltAgent:
    """Opaque, framework-specific handle returned by build() and passed back into run()."""

class AgentAdapter(Protocol):
    def build(self, variant: AgentVariant, toolset: SemanticLayerToolset) -> BuiltAgent: ...
    async def run(self, built: BuiltAgent, question: str, max_turns: int) -> AgentTrace: ...
```

`PydanticAIAdapter.build()` resolves `variant.entrypoint` via `importlib`, and calls it
with `(variant, filtered_tools)`. The user writes **one factory function per agent
family**, e.g.:

```python
# myapp/eval_agents.py
def build_bi_agent(variant: AgentVariant, tools: list[pydantic_ai.Tool]) -> pydantic_ai.Agent:
    return pydantic_ai.Agent(
        model=variant.model or "anthropic:claude-sonnet-5",
        system_prompt=variant.system_prompt,   # already resolved (file loaded + Jinja2-rendered) by the harness
        tools=tools,                             # already filtered to variant.tools by the harness
        model_settings=variant.model_settings,
    )
```

— all the *experiment design* (which tools, which prompt, which model) lives in the YAML
`AgentVariant` entries; the Python factory is reused unchanged across every arm that
shares an agent family. `PydanticAIAdapter.run()` executes the agent and normalizes
PydanticAI's `RunResult`/message history into `AgentTrace`.

**Two supported modes, both matter:**
1. **Config-driven variants (primary mode for arm comparisons)** — the factory builds a
   fresh agent per variant as shown above; all standard knobs are honored.
2. **Fixed/bring-your-own-agent mode** — for suites that just want to evaluate "whatever
   is actually in prod right now" as a single-arm baseline, the factory can ignore the
   knobs and return/wrap an already-fully-built agent object. `extra: dict` is the escape
   hatch for anything framework-specific that doesn't fit the standard knobs.

`adapters/strands.py` — **planned, not implemented this phase.** Same treatment as the
Snowflake adapter: a stub referencing the Strands quickstart
(strandsagents.com/docs/user-guide/quickstart/python/) as the future integration point.

`AgentAdapterFactory` (`adapters/__init__.py`) mirrors `CheckFactory`: dict dispatch on
`AgentFramework`.

---

## Metrics (`weiser/evals/metrics/`)

`MetricFactory` (`metrics/__init__.py`) is a **fourth parallel factory**, matching
`CheckFactory`/`DriverFactory`/`MetricStoreFactory`/`SemanticLayerFactory`/`AgentAdapterFactory`'s
established shape exactly: dict dispatch on `MetricConfig.type` → `Type[BaseEvalMetric]`,
instantiated with `(config: MetricConfig)`.

`BaseEvalMetric` contract (`metrics/base.py`) mirrors DeepEval's `BaseMetric`:

```python
class BaseEvalMetric(ABC):
    def __init__(self, config: MetricConfig): self.config = config; self.threshold = config.threshold
    score: float | None = None
    success: bool | None = None
    reason: str | None = None

    @abstractmethod
    def measure(self, test_case: EvalTestCase) -> CriterionScore: ...
    async def a_measure(self, test_case: EvalTestCase) -> CriterionScore:
        return self.measure(test_case)   # deterministic metrics rarely need async
    def is_successful(self) -> bool:
        return self.score is not None and self.score >= self.threshold
```

**Phase 1 — deterministic only** (no LLM judge, mirrors the spec's FIX-1/FIX-2/FIX-5,
the explicitly zero-curation-cost, ship-first items):

- `SchemaMembershipMetric` — parses `trace.predicted_sqls` with `sqlglot`
  (`exp.Table`/`exp.Column`), checks every identifier against `SchemaCatalog.views`.
  Port the spec's `check_sql_schema_membership` logic directly.
- `ExpectedViewRecallMetric` — `applicable=False` unless `golden.expected_views` is set;
  set-based recall over touched views (permissive of defensive extra fetches).
- `ReferenceValueMatchMetric` — `applicable=False` unless `golden.reference_values` is
  set; deterministic tolerance check against `trace.query_results`. Port the spec's
  `check_reference_values`/`_within_tolerance` logic verbatim.
- `StepEfficiencyMetric` — duplicate-tool-call detection, port of the spec's
  `detect_repeated_calls`/`step_efficiency_score`.
- `HitLimitMetric` — trivial pass/fail on `trace.hit_limit`, mirrors `agentic-sql-mini`'s
  `Correctness.HIT_LIMIT` as a first-class category rather than a silent zero score.

`params: dict` on `MetricConfig` carries type-specific config (e.g. a tolerance override
for `reference_value_match`, or which criteria set a `step_efficiency` variant should flag).

**Phase 2 — LLM-judge (`metrics/llm_judge.py`)**: a generic `LLMJudgeMetric` reading its
`criteria`/`evaluation_steps`/`evaluation_params` straight out of `MetricConfig.params` —
this is the direct YAML-native generalization of DeepEval's `GEval`, e.g.:

```yaml
metrics:
  - type: llm_judge
    name: sql_soundness
    threshold: 0.7
    params:
      criteria: "Is the SQL structurally sound given the schema, even without live execution?"
      evaluation_params: [predicted_sqls, schema_catalog]
```

Judge-prompt versioning (hash the rendered prompt template, stamp it on every
`EvalResultRow`) is built into `LLMJudgeMetric` from the start — mirrors the spec's FIX-3,
built in now rather than retrofitted later. Suites can define multiple `llm_judge`
instances (distinguished by `name`) for different criteria (`answer_correctness`,
`sql_soundness`, `groundedness`) without any new Python.

---

## The data-quality cornerstone (`weiser/evals/dataquality.py`)

This is the module that makes this framework different from both DeepEval and the spec's
FIX-9 proposal, and it's real (not a registry) from Phase 1:

```python
def extract_touched_datasets(predicted_sqls: list[str]) -> set[str]:
    # same sqlglot table-extraction approach as SchemaMembershipMetric — factor out and share

def build_dq_context(
    touched: set[str],
    checks: list[Check],          # from the SAME BaseConfig.checks the user already has configured
    connections: dict,             # driver map, reused from pre_run_config
    metric_store: MetricStoreDB,
    mode: Literal["live", "latest"] = "latest",
) -> DQContext:
    """For every touched dataset with a matching weiser Check (Check.dataset overlaps
    touched), either (mode="live") run it now via CheckFactory.create_check(...).run(),
    or (mode="latest") look up its most recent result via
    metric_store.get_metrics_for_check(check_id). has_failing_dq=True if any matching
    check's most recent/live result has success=False; has_stale_dq=True if the most
    recent result predates a configurable freshness window."""

def attribute_failure(
    criteria: list[CriterionScore],
    dq_context: DQContext,
    overall_score: float,
) -> Literal["clean", "agent", "data_quality", "judge_uncertain"]:
    """Starting heuristic (tune after real disagreement cases, per the spec's own caveat
    on this exact rule):
    - overall_score >= threshold -> "clean"
    - else if schema_membership/expected_view_recall/step_efficiency all clean AND
      dq_context.has_failing_dq -> "data_quality"
    - else if those deterministic checks are clean but no DQ evidence -> "agent"
    - else -> "judge_uncertain" (deterministic checks themselves failed — isolating
      agent-execution-error from agent-reasoning-error needs deeper trajectory analysis;
      deferred to Phase 2/3)
    """
```

`mode="latest"` is the sane default: reuses whatever DQ checks the user already runs on
their normal `weiser run` cadence via `metric_store.get_metrics_for_check()`
(`weiser/drivers/metric_stores/duckdb.py:646-673`), so an eval run doesn't silently
multiply datasource load. `mode="live"` is available for held-out/CI runs where freshness
matters more than cost. `EvalSuite.dq_scope` lets a suite explicitly restrict/expand which
checks count as confounding evidence, defaulting to "any check whose `dataset` overlaps a
touched table."

Directly reuses `CheckFactory.create_check` (`weiser/checks/__init__.py:36-47`),
`BaseDriver`, and `MetricStoreDB.get_metrics_for_check`
(`weiser/drivers/metric_stores/duckdb.py:646-660`) — no new DQ-check machinery, only new
*wiring* between the existing check engine and the new eval loop.

---

## Dataset loading (`weiser/evals/dataset.py`)

Reuse `weiser/loader/config.py:load_config` (`config.py:32-104`) for external YAML golden
files — glob + Jinja2 templating + `includes` merging come for free, directly satisfying
the playbook's "version the held-out set, commit it to git, treat each revision as
immutable" guidance (Step 2). A thin JSONL reader handles bulk/synthetic/production-mined
sets. Both funnel into `EvalGolden` validation; `EvalSuite.goldens` (inline) and
`EvalSuite.golden_set` (external) are merged if both are present. `EvalDataset` holds the
merged `list[EvalGolden]` and exposes `.filter(split=..., level=...)` for the CLI's
`--split` flag.

---

## Runner, comparison & CLI

`weiser/evals/runner.py::run_eval_suite(run_id, suite: EvalSuite, config: BaseConfig, connections, metric_store, split, verbose) -> dict[str, list[EvalResultRow]]`
(keyed by arm name) mirrors `weiser/runner/__init__.py:run_checks` structurally: for each
`EvalArm`, resolve its `AgentVariant` + `SemanticLayerConfig`, filter the toolset by
`variant.tools`, `AgentAdapterFactory.create(...).build(variant, filtered_tools)`, then for
each golden in the (split-filtered) dataset: `.run()` → run each configured `MetricConfig`
via `MetricFactory` → `build_dq_context` → `attribute_failure` → collect `EvalResultRow`.

`weiser/evals/compare.py` — when a suite has 2+ arms, produces the headline comparison
(mirrors `agentic-sql-mini`'s `asm compare`): per-arm accuracy (overall/Easy/Hard), cost,
turns, hit-limit rate, and failure-attribution breakdown, plus pairwise deltas. This reads
from the JSONL trace substrate (see below), not the flat metric-store rows, since
arm-comparison is inherently multi-dimensional. Also runs the "batching changes" lint
described above.

Result storage is **dual-write**, matching the playbook's Step 3 "two layers of logging":
1. **Result substrate**: one flat row per `(golden, arm)` into the *existing* metrics
   table via `metric_store.insert_results(dict)` (pattern from
   `weiser/checks/base.py:86-138`) — `name` encodes `f"{golden.id}[{arm}]"` (no schema
   migration needed), `type="agent_eval"`, `actual_value=overall_score`,
   `success`/`fail` set accordingly. Existing `weiser-ui` dashboard shows eval summary
   rows with zero dashboard changes.
2. **Trace substrate**: full `EvalResultRow` + `AgentTrace`, one JSON line per
   `(golden, arm)`, written to `eval_results/{suite}_{split}_{run_id}.jsonl` — mirrors
   `agentic-sql-mini`'s `results/*.jsonl`. This is what `compare.py` reads, and what
   failure-analysis / hill-climbing (playbook Step 5) reads. A dedicated
   `agent_eval_results` metric-store table (proper `arm` column, indexed) is explicitly
   **deferred to Phase 3** once the row shape has stabilized from real use.

`weiser/main.py` gains new Typer commands, structurally identical to `run`/`compile`:

```
weiser eval <config.yaml> --suite <name> --split held_out -v [--env-file ...]
weiser eval-compile <config.yaml> --suite <name>   # validates config, resolves adapters/variants, runs the batching-changes lint, no execution
```

CLI output: a `rich` table per arm matching the playbook's three-dimension scorecard
(accuracy — overall/Easy/Hard, avg turns, avg cost, hit-limit rate), a
**failure-attribution breakdown** per arm (`failures (n=12): 8 agent, 3 data_quality, 1
judge_uncertain`), and — when 2+ arms — the `compare.py` pairwise delta table.

---

## Phased roadmap

**Phase 1 (this implementation pass, if approved):** `weiser/evals/` module skeleton
including the full YAML-first config surface (`AgentVariant`, `EvalArm`, `EvalSuite`,
`MetricConfig`, inline/external `EvalGolden`), `CubeSemanticLayer` +
`GenericSQLSemanticLayer`, `PydanticAIAdapter` (both usage modes), Phase-1 deterministic
metrics via `MetricFactory`, the DQ confounding + attribution layer wired to real
`CheckFactory` checks, `EvalDataset` loading, arm-aware `run_eval_suite`, `compare.py`
(including the batching-changes lint), `weiser eval` / `weiser eval-compile` CLI commands,
dual result storage. Config model additions in `weiser/loader/models.py` +
`update_namespace` extension in `weiser/loader/config.py`.

**Phase 2 (follow-up):** `metrics/llm_judge.py` (YAML-configured `GEval`-style judge +
judge-prompt versioning), pre-built judge presets (AnswerCorrectness/SQLSoundness/
Groundedness) expressible as `MetricConfig` templates, `synthesizer.py` (schema-driven
synthetic golden generation biased toward datasets with open/recent DQ failures — reusing
`dataquality.py`'s own DQContext instead of a hand-maintained YAML registry), repeat-run
variance reporting (spec FIX-7), train-set failure export tooling supporting the
playbook's Step 5 hill-climb loop (human-in-the-loop; not full automation).

**Phase 3 (future):** `weiser eval-gate` CI regression command (spec FIX-6, excludes
`data_quality`-attributed rows from the regression delta), judge calibration set +
`calibrate_judge` command (spec FIX-4), a dedicated `agent_eval_results` metric-store
table + weiser-ui dashboard views (arm comparison, DQ-attribution breakdown — richer than
the flat summary row Phase 1 ships with), the `SnowflakeSemanticViewsAdapter`, the
`StrandsAdapter`, and production-trace mining for the playbook's Step 7 outer loop
(explicitly "still in-flight work" even at MotherDuck — aspirational, not a near-term
commitment).

---

## Testing & verification

Mirror existing conventions in `tests/unit/` and `tests/fixtures/config_fixtures.py`
(star-import fixtures, `Mock(spec=...)` for drivers/metric stores, dict-shape assertions):

- `tests/fixtures/config_fixtures.py`: add `sample_agent_variant`, `sample_eval_arm`,
  `sample_eval_golden` (inline + external-file variants), `sample_metric_config`,
  `sample_cube_semantic_layer_config`, `sample_generic_sql_semantic_layer_config`,
  `sample_eval_suite` (both 1-arm and 2-arm versions, the latter for testing the
  batching-changes lint).
- `tests/conftest.py`: add `mock_agent_adapter` (returns a canned `AgentTrace`) and
  `mock_semantic_layer` (returns a canned `SchemaCatalog`), mirroring
  `mock_driver`/`mock_metric_store`.
- `tests/unit/evals/test_config_models.py` — round-trip a YAML config containing
  `agent_variants`/`semantic_layers`/`eval_suites` (including both the tool-ablation
  2-arm example and the inline-goldens example from this plan) through `load_config` +
  `BaseConfig(**config)`, asserting the parsed structure matches.
- `tests/unit/evals/test_semantic_layer.py` — `GenericSQLSemanticLayer.get_schema()`
  against a mocked SQLAlchemy `Inspector`; `CubeSemanticLayer.get_schema()` against a
  mocked HTTP response shaped like Cube's real `/meta` payload.
- `tests/unit/evals/test_metrics_deterministic.py` — one test class per Phase-1 metric
  via `MetricFactory`, including the exact "correct-looking but wrong" fixture the spec
  calls out, proving `ReferenceValueMatchMetric` catches what SQL-plausibility reasoning
  alone would miss.
- `tests/unit/evals/test_dataquality.py` — the critical one: a touched dataset with a
  configured, currently-failing `not_empty` check should produce
  `failure_attribution="data_quality"` when all deterministic agent-behavior metrics are
  clean, and `"agent"` when a deterministic metric also failed.
- `tests/unit/evals/test_adapters_pydantic_ai.py` — variant→tool-filtering→`build()`
  wiring, plus normalization of a canned PydanticAI `RunResult` into `AgentTrace`.
- `tests/unit/evals/test_compare.py` — the batching-changes lint: 2 arms differing only
  in `tools` → no warning; differing in `tools` AND `model` → warning fires.
- `tests/integration/test_eval_workflow.py` — end-to-end `run_eval_suite()` for a 2-arm
  suite against a fully mocked stack, asserting both arms' dual-write happens and that
  `compare.py` produces a pairwise delta.

**Manual verification**, once implemented:
1. `weiser eval-compile examples/eval-example.yaml --suite <name>` — validates the new
   config sections parse, adapters/variants resolve, and the batching-changes lint runs.
2. `weiser eval examples/eval-example.yaml --suite <name> --split train -v` against a
   local DuckDB example dataset with `GenericSQLSemanticLayer`, a 2-arm suite (tool
   ablation), and a trivial stub `AgentAdapter` factory — proves the full YAML-to-CLI
   pipeline end-to-end without external infra.
3. If a test Cube instance is available, repeat against `CubeSemanticLayer`. Optional/manual
   if none exists in this environment — do not block Phase 1 completion on it.
4. Confirm eval summary rows appear in the existing `weiser-ui` dashboard
   (`weiser-ui/app.py`) without dashboard code changes.
