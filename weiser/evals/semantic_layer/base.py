import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Set

import sqlglot
from pydantic import BaseModel
from sqlalchemy import text
from sqlglot import exp


class SchemaView(BaseModel):
    name: str
    members: Set[str] = set()


class SchemaCatalog(BaseModel):
    views: Dict[str, SchemaView] = {}
    has_semantics: bool = False
    fetched_at: datetime


class SemanticLayerAdapter(ABC):
    """A target system that (a) exposes a schema of queryable views/measures/dimensions
    and (b) can execute SQL against it. Cube's meta API and plain SQL introspection are
    both concrete forms of this; more (e.g. Snowflake Semantic Views) can be added later
    without changing this contract."""

    @abstractmethod
    def get_schema(self, refresh: bool = False) -> SchemaCatalog: ...

    @abstractmethod
    def execute_query(self, sql: str) -> List[dict]: ...

    def get_freshness(self, view: str) -> Optional[datetime]:
        """Optional: when the target can report last-materialized time for a view."""
        return None

    def get_dialect(self):
        """Optional: the sqlglot dialect the target executes SQL in, so the toolset can
        rewrite queries (e.g. capping LIMIT) without transpiling across dialects."""
        return None  # noqa: RET501 -- mirrors get_freshness's explicit None default


_READ_STATEMENTS = (exp.Select, exp.Union, exp.Intersect, exp.Except)
_WRITE_STATEMENTS = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Drop,
    exp.Create,
    exp.AlterTable,
    exp.Command,
)
_WRITE_KEYWORD_RE = re.compile(
    r"\b(insert|update|delete|merge|drop|alter|create|truncate|grant|revoke|copy|call|exec)\b",
    re.IGNORECASE,
)


def _assert_read_only_sql(driver, sql: str) -> None:
    """Reject anything that is not a single read-only SELECT statement.

    Agent SQL is untrusted (it originates from an LLM), so it must be validated
    before touching the engine. Primary check: parse with sqlglot using the
    driver's dialect and require exactly one select-family statement with no
    write/DDL/command nodes anywhere in the tree (this also catches
    data-modifying CTEs like `WITH x AS (DELETE ...)`). If the SQL cannot be
    parsed (dialect-specific syntax sqlglot doesn't know), fall back to a
    conservative keyword/statement-count guardrail instead of executing blindly.
    """
    stripped = sql.strip()
    if not stripped:
        raise ValueError("Empty SQL query")
    try:
        statements = sqlglot.parse(stripped, read=driver.dialect)
    except Exception:
        statements = None
    if statements is None:
        if ";" in stripped.rstrip(";"):
            raise ValueError("Multiple SQL statements are not allowed")
        if _WRITE_KEYWORD_RE.search(stripped):
            raise ValueError("Only read-only SELECT queries are allowed")
        return
    if len(statements) != 1 or not isinstance(statements[0], _READ_STATEMENTS):
        raise ValueError("Only read-only SELECT queries are allowed")
    if any(statements[0].find_all(*_WRITE_STATEMENTS)):
        raise ValueError("Only read-only SELECT queries are allowed")


def execute_sql_via_driver(driver, sql: str) -> List[dict]:
    """Run agent-submitted SQL through a weiser BaseDriver's raw SQLAlchemy engine.

    Deliberately does not reuse BaseDriver.execute_query: that method raises on an
    empty/None result because it's built for scalar DQ-check queries, whereas an
    agent's SQL may legitimately return zero rows as a correct answer.

    Agent SQL is untrusted, so it is first validated to be a single read-only
    SELECT statement (see _assert_read_only_sql).
    """
    _assert_read_only_sql(driver, sql)
    with driver.engine.connect() as conn:
        result = conn.execute(text(sql))
        keys = list(result.keys())
        return [dict(zip(keys, row)) for row in result.fetchall()]
