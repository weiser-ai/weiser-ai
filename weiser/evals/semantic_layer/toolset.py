from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Set

import sqlglot

from weiser.evals.models import ToolCall
from weiser.evals.semantic_layer.base import _READ_STATEMENTS, SemanticLayerAdapter

STANDARD_TOOL_NAMES = ("list_views", "describe_view", "query", "submit_answer")

_QUERY_ROW_LIMIT = 50


def _cap_query_rows(sql: str, limit: int, dialect=None) -> str:
    """Rewrite a read-only query so the engine materializes at most `limit` rows
    instead of fetching the full result set and slicing it client-side.

    An existing numeric LIMIT is capped rather than duplicated. SQL that cannot be
    parsed, is not a select-family statement, or carries a non-literal LIMIT is
    returned unchanged: the adapter's read-only validation still runs before
    execution, and the client-side slice in query() remains the backstop."""
    try:
        statement = sqlglot.parse_one(sql.strip().rstrip(";"), read=dialect)
    except Exception:  # noqa: BLE001 -- unparseable SQL falls back to the original query
        return sql
    if not isinstance(statement, _READ_STATEMENTS):
        return sql
    existing = statement.args.get("limit")
    if existing is not None:
        try:
            if int(existing.expression.this) <= limit:
                return sql
        except (AttributeError, TypeError, ValueError):
            return sql
    return statement.limit(limit).sql(dialect=dialect)


@dataclass
class ToolSpec:
    """A framework-agnostic tool definition. `fn` is a plain async callable with type
    hints/docstring an agent framework's tool-calling layer can introspect."""

    name: str
    description: str
    fn: Callable


class ToolsetState:
    """Per-question, mutable bookkeeping shared by every tool closure built for that
    question. This is the source of truth an AgentAdapter builds an AgentTrace from --
    any framework that uses this toolset gets consistent trace capture for free,
    independent of whatever tracing (or lack thereof) that framework provides itself.
    Directly modeled on agentic-sql-mini's RunState."""

    def __init__(self) -> None:
        self.tool_calls: List[ToolCall] = []
        self.turn: int = 0
        self.submitted: bool = False
        self.final_sql: Optional[str] = None
        self.final_answer: Optional[str] = None
        self.final_rows: Optional[List[dict]] = None
        self.error: Optional[str] = None

    def record(self, tool_name: str, args: dict, result: Any) -> None:
        self.turn += 1
        self.tool_calls.append(
            ToolCall(tool_name=tool_name, args=args, result=result, turn=self.turn)
        )


def build_toolset(
    adapter: SemanticLayerAdapter,
    state: ToolsetState,
    allowed: Optional[Set[str]] = None,
) -> List[ToolSpec]:
    """The standard 4-tool toolset (list_views/describe_view/query/submit_answer),
    modeled on agentic-sql-mini's src/agent.py tools, built fresh per question so each
    closure is bound to that question's ToolsetState. `allowed` (from
    AgentVariant.tools) filters which of the 4 are exposed -- this is what makes
    "compare an agent with/without a tool" a pure-YAML experiment: the harness does the
    filtering, not per-framework code."""

    schema = adapter.get_schema()

    async def list_views() -> str:
        """List all available views/tables in the semantic layer."""
        names = sorted(schema.views.keys())
        result = "\n".join(names) if names else "(no views found)"
        state.record("list_views", {}, result)
        return result

    async def describe_view(view_name: str) -> str:
        """Describe the measures/dimensions/columns available on a given view."""
        view = schema.views.get(view_name.lower())
        if view is None:
            result = f"Unknown view: {view_name}"
        else:
            result = "\n".join(sorted(view.members))
        state.record("describe_view", {"view_name": view_name}, result)
        return result

    async def query(sql: str) -> str:
        """Run a read-only SQL query against the semantic layer and return up to 50
        rows."""
        capped_sql = _cap_query_rows(sql, _QUERY_ROW_LIMIT, adapter.get_dialect())
        try:
            rows = adapter.execute_query(capped_sql)
        except Exception as e:  # noqa: BLE001 -- surfaced to the agent as tool output
            error_result = f"ERROR: {e}"
            state.record("query", {"sql": sql}, error_result)
            return error_result
        preview = rows[:_QUERY_ROW_LIMIT]
        state.record("query", {"sql": sql}, preview)
        return "\n".join(str(row) for row in preview) if preview else "(no rows)"

    async def submit_answer(sql: str, answer: str) -> str:
        """Submit the final SQL query and natural-language answer. Call exactly once,
        when done."""
        if state.submitted:
            return "ERROR: submit_answer was already called for this question."
        try:
            rows = adapter.execute_query(sql)
            state.final_rows = rows
        except Exception as e:  # noqa: BLE001
            state.error = str(e)
            rows = None
        state.final_sql = sql
        state.final_answer = answer
        state.submitted = True
        result = "submitted"
        state.record("submit_answer", {"sql": sql, "answer": answer}, result)
        return result

    tools = {
        "list_views": ToolSpec(
            "list_views", "List all available views/tables.", list_views
        ),
        "describe_view": ToolSpec(
            "describe_view",
            "Describe a view's measures/dimensions/columns.",
            describe_view,
        ),
        "query": ToolSpec(
            "query", "Run a read-only SQL query, returns up to 50 rows.", query
        ),
        "submit_answer": ToolSpec(
            "submit_answer",
            "Submit the final SQL + natural-language answer. Call exactly once.",
            submit_answer,
        ),
    }
    names = allowed if allowed is not None else set(STANDARD_TOOL_NAMES)
    return [tools[n] for n in STANDARD_TOOL_NAMES if n in names]
