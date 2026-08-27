# Using Weiser with Dagster

This guide is for teams who already run Dagster and want to attach Weiser
data quality checks to their asset graph. It assumes familiarity with Dagster
concepts (`@asset`, `@asset_check`, `ConfigurableResource`, `Definitions`) and
focuses on the parts that are specific to wiring Weiser in, not on Dagster
basics.

It's written from a real production integration: a `cross_source_row_count`
check comparing a dbt model built into two different warehouses (Postgres and
Snowflake) during a migration. The check type and the two-warehouse detail are
incidental — the pattern generalizes to any Weiser check type and any driver.
Where it helps, the guide points at that check type's own
[reference doc](./check-types/cross-source-row-count.md); the point here is
the Dagster wiring, not that specific check.

## Why an asset check, not an asset

Dagster asset checks are the native surface for "is this materialization
trustworthy," as opposed to "here is more data." A Weiser check almost always
maps to an `@asset_check`, not an `@asset`:

- It attaches to the asset it's validating, shows up on that asset's page,
  and is queryable in run history — exactly where an engineer debugging a
  stale/wrong table will look first.
- It doesn't fabricate a materialization event for a table nobody wrote. An
  `@asset` that "runs Weiser and returns nothing useful" is a check wearing
  an asset's clothes.
- If you already use `dagster-dbt`, this precedent exists in your graph
  today: dbt tests are loaded as asset checks by default
  (`enable_asset_checks=True` on `DagsterDbtTranslator`), so your dbt
  `unique`/`not_null` tests already render as checks on their models. A
  Weiser check belongs in the same place, for the assertions dbt can't make
  on its own (e.g. anything that spans two datasources).

Use `blocking=False` while a check is new — Weiser checks are usually
*informational* at first (see [Rollout](#rollout-dont-ship-blocking-on-day-one)
below), and a false positive on a brand-new check should not stop a
downstream refresh.

## The integration shape

There are three pieces, and they map directly onto Weiser's own layering:

```
┌─────────────────────────────┐
│ @dg.asset_check              │  Dagster-facing: pass/fail + metadata
├─────────────────────────────┤
│ ConfigurableResource         │  Owns "how to run Weiser here": config
│ (e.g. WeiserResource)        │  path, env interpolation, calling the
│                              │  programmatic API, shaping the result
├─────────────────────────────┤
│ weiser.yml                   │  Declarative: datasources, checks,
│ (git-tracked config)         │  metric store connection
└─────────────────────────────┘
```

### 1. A git-tracked Weiser config

Nothing Dagster-specific here — it's the same YAML shape described in
[Configuration](./configuration.md). The only habit worth calling out: don't
hardcode dataset names or credentials. Let the `@asset_check` (or the
resource) inject the resolved values as extra environment variables into
`load_config`'s `context=` dict at call time, keyed to whatever `{{VAR}}`
placeholders your config uses:

```yaml
version: 1

datasources:
  - name: source_a
    type: postgresql
    host: "{{POSTGRES_HOST}}"
    port: "{{POSTGRES_PORT}}"
    db_name: "{{POSTGRES_DB}}"
    user: "{{POSTGRES_USER}}"
    password: "{{POSTGRES_PASSWORD}}"

  - name: source_b
    type: snowflake
    account: "{{SNOWFLAKE_ACCOUNT}}"
    warehouse: "{{SNOWFLAKE_WAREHOUSE}}"
    role: "{{SNOWFLAKE_ROLE}}"
    db_name: "{{SNOWFLAKE_DATABASE}}"
    user: "{{SNOWFLAKE_USER}}"

connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
    db_name: "{{WEISER_METRICSTORE_PATH}}"

checks:
  - name: my_table_row_count_parity
    type: cross_source_row_count
    datasource: source_a
    dataset: "{{WEISER_A_DATASET}}"
    compare_datasource: source_b
    compare_dataset: "{{WEISER_B_DATASET}}"
    condition: eq
    threshold: 0
```

`WEISER_A_DATASET` / `WEISER_B_DATASET` aren't real environment variables
anywhere — they're computed by your resource (next section) and merged into
the `context` dict passed to `load_config`, alongside `os.environ`. This
keeps the config generic while letting Dagster resolve environment-specific
naming (schema prefixes, per-deploy suffixes, etc.) at run time instead of
baking it into a file.

### 2. A `ConfigurableResource` wrapping Weiser's programmatic API

Don't shell out to the `weiser` CLI from Dagster. Two reasons:

- **The CLI's exit code lies.** `weiser run`'s `run` command
  (`weiser/main.py`) prints results and returns normally regardless of
  whether any check passed — only a config/connection error raises. If you
  invoke the CLI as a subprocess and check `returncode`, a real check
  failure looks identical to success. You'd have to parse stdout or read the
  metric store back out, which is strictly more code than calling the
  library directly.
- **Weiser has a programmatic API that returns structured results.** You get
  a Python object back, not text to scrape:

  ```python
  from weiser.loader.config import load_config
  from weiser.runner import pre_run_config, run_checks

  config = load_config(config_path, context=env_dict)
  ctx = pre_run_config(config, verbose=False)
  results = run_checks(
      ctx["run_id"], ctx["config"], ctx["connections"], ctx["metric_store"],
      verbose=False,
  )
  ```

  `results` is a list of `{"check_instance": <name>, "results": [...], "run_id": ...}`.
  Each inner result carries `success`, `actual_value`, `check_id`, `run_time`.
  Note `pre_run_config` eagerly opens every datasource in the config and runs
  `SELECT 1` against each — so don't put datasources you don't intend to use
  yet into a config a given check will load, or you'll get a spurious
  connectivity failure.

Wrap that sequence in a `ConfigurableResource` so it participates in
Dagster's resource system like any other connection (registered once in
`Definitions(resources=...)`, injected into whichever asset checks need it):

```python
from dataclasses import dataclass
import os

from dagster import ConfigurableResource
from weiser.loader.config import load_config
from weiser.runner import pre_run_config, run_checks


@dataclass
class CheckResult:
    success: bool
    actual_value: float
    run_id: str
    check_id: str


class WeiserResource(ConfigurableResource):
    config_path: str
    metricstore_path: str

    def run_check(self, check_instance_name: str, extra_context: dict) -> CheckResult:
        context = dict(os.environ)
        context.update(extra_context)
        context.setdefault("WEISER_METRICSTORE_PATH", self.metricstore_path)

        config = load_config(self.config_path, context=context)
        ctx = pre_run_config(config, verbose=False)
        results = run_checks(
            ctx["run_id"], ctx["config"], ctx["connections"], ctx["metric_store"],
            verbose=False,
        )

        result_row = next(r for r in results if r["check_instance"] == check_instance_name)
        row = result_row["results"][0]
        return CheckResult(
            success=row["success"],
            actual_value=float(row["actual_value"]),
            run_id=result_row["run_id"],
            check_id=row["check_id"],
        )
```

A couple of things worth deciding deliberately here, not by default:

- **Where do dataset names come from?** If they depend on a runtime-resolved
  schema prefix, environment name, or similar, compute them in a small
  helper the resource calls (or in the asset check, passed in via
  `extra_context`) — never hardcode them into the YAML. Test that
  derivation directly; it's cheap to get subtly wrong (see
  [Identifier quoting](#identifier-quoting-a-warning-that-generalizes) below).
- **Metric store lifecycle.** If your Dagster deployment runs on ephemeral
  compute (containers, serverless), a local DuckDB file metric store loses
  history on every redeploy. That's often fine — history only matters for
  checks that need a trend (e.g. `anomaly`), and if your check types are all
  point-in-time (`row_count`, `cross_source_row_count`, etc.) there's nothing
  to lose. But make the call explicitly and write it down; don't let it be
  an accident of "DuckDB was the default." The alternative (DuckDB-on-S3, or
  a Postgres metric store) trades durability for either new credentials or a
  write path into a database you may not want Weiser writing into.

### 3. An `@asset_check` that calls the resource and returns `AssetCheckResult`

```python
import dagster as dg


@dg.asset_check(
    asset=dg.AssetKey(["my_schema", "my_table"]),
    blocking=False,
    name="row_count_parity",
)
def my_table_row_count_parity(
    context: dg.AssetCheckExecutionContext, weiser: WeiserResource
) -> dg.AssetCheckResult:
    result = weiser.run_check(
        "my_table_row_count_parity",
        extra_context={
            "WEISER_A_DATASET": "my_schema.my_table",
            "WEISER_B_DATASET": '"my_schema"."my_table"',
        },
    )
    return dg.AssetCheckResult(
        passed=result.success,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "actual_value": result.actual_value,
            "weiser_run_id": result.run_id,
            "weiser_check_id": result.check_id,
        },
    )
```

Then register both in `Definitions`:

```python
defs = dg.Definitions(
    assets=[...],
    asset_checks=[my_table_row_count_parity],
    resources={"weiser": WeiserResource(config_path="...", metricstore_path="...")},
)
```

Note `Definitions()` rejects duplicate asset keys outright — if the thing
you're validating spans two Dagster assets (see the next section), attach the
check to one asset and express the dependency on the other via
`additional_deps`, rather than trying to declare a second asset for the same
underlying table.

**Make results actionable, always — pass or fail.** Put every number the
check computed into `metadata`, not just a boolean. A failing check should
tell whoever's on call *what* diverged and by how much without them having
to re-run anything by hand. `AssetCheckResult.metadata` shows up directly on
the asset's check history in the Dagster UI.

## Cross-datasource checks: the "which asset does this attach to" problem

If your check compares two datasources that Dagster materializes as two
separate assets (e.g. the same table built into two different warehouses, or
a source table and its replicated copy), the check has two "parents," but
Dagster's `@asset_check(asset=...)` only takes one. Use `additional_deps` for
the other:

```python
@dg.asset_check(
    asset=dg.AssetKey(["warehouse_a", "my_table"]),
    additional_deps=[dg.AssetKey(["warehouse_b", "my_table"])],
    blocking=False,
    name="cross_source_row_count",
)
def my_table_cross_source_row_count(...) -> dg.AssetCheckResult:
    ...
```

This expresses *graph* dependency (the check node depends on both assets),
but **not freshness coordination** — Dagster won't wait for both sides to be
simultaneously fresh before running the check just because both are listed as
deps. If the two assets materialize independently (e.g. both are
`AutoMaterializePolicy.eager()` with no shared trigger), a check firing
because one side lagged behind the other is a real failure mode, not a
theoretical one. Options, roughly in order of effort:

1. **Do nothing, and classify failures by hand initially.** Acceptable for a
   new check during its soak period (see [Rollout](#rollout-dont-ship-blocking-on-day-one)).
2. **Record both sides' last-materialization timestamps in the check's own
   metadata**, and downgrade `AssetCheckSeverity` from `ERROR` to `WARN` when
   they differ by more than some threshold you're willing to defend:

   ```python
   pg_ts = context.instance.get_latest_materialization_event(asset_key_a).materialized_at
   sf_ts = context.instance.get_latest_materialization_event(asset_key_b).materialized_at
   severity = dg.AssetCheckSeverity.ERROR
   if not result.success and pg_ts and sf_ts and abs((pg_ts - sf_ts).total_seconds()) > 6 * 3600:
       severity = dg.AssetCheckSeverity.WARN
       metadata["likely_cause"] = "timing — materialization timestamps differ by >6h"
   ```

   This doesn't prevent the false positive, but it tells the reader "this is
   probably a timing artifact, not real drift" without them having to dig.
3. **Define an explicit job selecting both upstream assets plus the check**,
   or use a `run_status_sensor` that only triggers the check once both sides'
   latest run succeeded. More correct, more moving parts — worth it once
   you've observed that timing-artifact failures are frequent enough to be
   noise.

Don't silently pick option 2 or 3 without first observing whether timing
races actually happen for your specific assets' materialization cadence — it
adds real complexity, and R4-style "is this a real failure or a race"
questions are usually rarer than they sound until you've watched a check run
in production for a week.

## Writing a custom driver: the general pattern

Weiser ships drivers for Postgres, MySQL, Snowflake, Databricks, and BigQuery
(`weiser/drivers/`), each a thin subclass of `BaseDriver` that builds a
SQLAlchemy engine from a `Datasource` config object. If your environment's
auth story doesn't fit what a stock driver supports — the most common case is
**key-pair (or any non-password) authentication**, since every stock driver
in `weiser/drivers/*.py` builds its connection URL from
`data_source.password.get_secret_value()` with no other auth path — you can
override the driver Weiser uses for a given `type:` without forking the
library.

**The extension point.** `weiser.drivers.DriverFactory.create_driver` looks
up the driver class for a datasource by `DBType` in a plain module-level
dict, `weiser.drivers.DB_DRIVER_MAP` (`weiser/drivers/__init__.py`). There's
no config-supplied override — the only way in is mutating that dict, and it
has to happen *before* `pre_run_config()` constructs drivers for the
datasources in your config (i.e. at import time of whatever module does the
mutation, imported before you call `pre_run_config`):

```python
import weiser.drivers
from weiser.loader.models import DBType

from my_project.my_custom_driver import MyCustomDriver

weiser.drivers.DB_DRIVER_MAP[DBType.snowflake] = MyCustomDriver
```

**The driver itself.** Subclass the stock driver (or `BaseDriver` directly)
and override `__init__` to build the engine your way, without calling
`super().__init__()` if the parent's logic is exactly the password path you
need to avoid. You still need to set the same three attributes
`BaseDriver.__init__` would have set — `self.data_source`, `self.engine`,
`self.dialect` — since `execute_query` reads all three:

```python
from sqlalchemy import create_engine
from sqlglot.dialects import Snowflake
from snowflake.sqlalchemy import URL
from weiser.drivers.snowflake import SnowflakeDriver
from weiser.loader.models import Datasource


class MyCustomDriver(SnowflakeDriver):
    """Key-pair auth instead of Weiser's stock password-only Snowflake driver."""

    def __init__(self, data_source: Datasource) -> None:
        if not (data_source.account and data_source.warehouse and data_source.role and data_source.db_name):
            raise ValueError("account, warehouse, role, and db_name are all required.")

        private_key_bytes = load_private_key(...)  # however your platform loads it

        url = URL(
            account=data_source.account,
            user=data_source.user,
            schema=data_source.schema_name or "public",
            warehouse=data_source.warehouse,
            role=data_source.role,
            database=data_source.db_name,
            # no `password` argument at all
        )

        self.data_source = data_source
        self.dialect = Snowflake()
        self.engine = create_engine(
            url,
            connect_args={
                "private_key": private_key_bytes,
                # anything else your platform's SQLAlchemy connections
                # normally pass — session tags, timeouts, etc. — goes here
                "session_parameters": {"QUERY_TAG": "MY_APP_NAME"},
            },
        )
```

This pattern isn't Snowflake-specific — it generalizes to any datasource type
where your platform's existing connection helper does something a stock
driver doesn't (mTLS client certs, IAM auth tokens, a custom connection-pool
wrapper, non-default session parameters). The recipe is always the same
three steps:

1. Find the connection logic your platform already uses successfully for
   this database elsewhere (you almost certainly have one — reuse it rather
   than inventing new auth code for Weiser specifically).
2. Subclass the closest stock driver, skip `super().__init__()`, and set
   `data_source` / `engine` / `dialect` yourself using that existing logic.
3. Register the subclass in `DB_DRIVER_MAP` for the relevant `DBType`, via a
   module that's imported before any config referencing that datasource type
   is loaded — e.g. at the top of your `ConfigurableResource`'s check method,
   or in whatever module registers your Dagster resources.

**If your platform tags every connection with something** (a query tag, an
application name, a correlation ID) **for observability** — anything that
lets you distinguish this tool's traffic from human/BI traffic in your
warehouse's query history — a custom driver is also the place to enforce
that, since it's the one path a stock driver's `create_engine(data_source.uri)`
call (no `connect_args`) doesn't give you. Treat a new driver as a new
connection path in whatever inventory of "things that connect to this
database" your platform already keeps, the same way you'd treat a new ORM
connection pool or a new service account — it's easy to add a tool that talks
to your warehouse and forget to route it through your tagging convention.

### Identifier quoting: a warning that generalizes

Weiser parses `dataset` strings with `sqlglot.parse_one` and renders the
resulting SQL through the dialect matching the datasource's `type`
(`weiser/drivers/base.py`, `DIALECT_TYPE_MAP`). If your warehouse folds
unquoted identifiers to a different case than how the relation actually
exists (Snowflake's default is uppercase-folding, and many teams run
lowercase-quoted schemas deliberately), an unquoted `dataset: my_schema.my_table`
will render with the *wrong* case and fail with something like `"Schema
'MY_SCHEMA' does not exist or not authorized"` — a message that reads exactly
like "this table doesn't exist," which is not what actually happened.

If you're in that situation, pre-quote the dataset string yourself
(`'"my_schema"."my_table"'`) rather than relying on the dialect renderer to
do it, and cover the rendered SQL with a unit test rather than trusting it —
this is cheap to verify once and expensive to debug live against a real
warehouse.

## Testing without hitting real databases

You don't need live credentials to unit-test the Dagster/Weiser boundary.
Mock `run_checks` (or the resource method that calls it) to return a
Weiser-shaped result and assert your `@asset_check` maps it to the
`AssetCheckResult` you expect:

```python
def test_check_fails_on_mismatch(monkeypatch):
    monkeypatch.setattr(
        my_module,
        "run_checks",
        lambda *a, **k: [{
            "check_instance": "my_table_row_count_parity",
            "run_id": "abc123",
            "results": [{"success": False, "actual_value": 0.15, "check_id": "def456"}],
        }],
    )
    result = my_table_cross_source_row_count.__wrapped__(fake_context, weiser_resource)
    assert result.passed is False
    assert result.metadata["actual_value"].value == 0.15
```

Worth covering explicitly:

- **Pass and fail mapping**, from a stubbed `run_checks` result to
  `AssetCheckResult.passed`.
- **Dataset-name derivation**, if you compute names at runtime (env-prefix
  set/unset, quoting behavior for the warehouse that needs it).
- **A guard test that the asset keys your check references still exist** in
  whatever produces your asset graph (e.g. a dbt manifest). A check silently
  pointed at a renamed/removed asset key doesn't error — it just never
  fires, and a permanently-green check on nothing is worse than no check,
  because it looks like coverage. This is cheap: assert the asset key
  appears in your manifest/asset list, not that the check runs correctly.

For local end-to-end verification before writing any Dagster code at all,
use the CLI directly against a throwaway config and a local database:

```bash
weiser compile my_config.yaml -v   # validates config + connectivity, runs nothing
weiser run my_config.yaml -v       # actually runs the checks
```

Remember the exit-code caveat from above applies here too: `-v` prints a
results table you should read, not just the shell's `$?`.

## Rollout: don't ship blocking on day one

A cross-datasource or otherwise novel check has never been observed failing
in your environment until it has. Sequence the rollout so the first red
result is informative rather than a page:

1. Ship with `blocking=False`. Nothing downstream should be gated on a
   check's first weeks of life.
2. Deliberately force a failure once (point `compare_dataset` at a filtered
   subquery, or similarly perturb one side) and confirm the check actually
   goes red with readable metadata. A parity check that has never been
   observed failing hasn't been tested, it's been hoped at.
3. Run for real over enough elapsed time to cover your data's natural
   cadence (a full week if anything is daily-batched, a month-end window if
   anything is monthly/incremental) and classify every failure: real
   data-quality signal vs. timing artifact vs. infrastructure hiccup.
4. Only after a clean soak, consider promoting to `blocking=True` and/or
   wiring `slack_url` alerting — each as its own decision, not bundled into
   the initial rollout.

## Summary checklist

- [ ] Config is git-tracked YAML, no literal credentials, dataset names
      resolved at run time rather than hardcoded.
- [ ] A `ConfigurableResource` calls `load_config` → `pre_run_config` →
      `run_checks` directly — no CLI subprocess, so pass/fail comes from
      parsed results, never an exit code.
- [ ] One `@asset_check` per logical check, attached to the asset(s) it
      validates (`additional_deps` for a second datasource's asset),
      registered via `Definitions(asset_checks=[...])`.
- [ ] Every result's metadata carries the actual value(s) and Weiser's
      `run_id`/`check_id` — pass or fail, not just a boolean.
- [ ] If you wrote a custom driver: it reuses your platform's existing
      connection logic, it's registered in `DB_DRIVER_MAP` before any config
      referencing it loads, and if your platform tags connections for
      observability, this driver does too.
- [ ] Shipped `blocking=False` first, with a deliberately-forced failure
      demonstrated before trusting the check's green state.
