from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Set

from pydantic import BaseModel
from sqlalchemy import text


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


def execute_sql_via_driver(driver, sql: str) -> List[dict]:
    """Run agent-submitted SQL through a weiser BaseDriver's raw SQLAlchemy engine.

    Deliberately does not reuse BaseDriver.execute_query: that method raises on an
    empty/None result because it's built for scalar DQ-check queries, whereas an
    agent's SQL may legitimately return zero rows as a correct answer.
    """
    with driver.engine.connect() as conn:
        result = conn.execute(text(sql))
        keys = list(result.keys())
        return [dict(zip(keys, row)) for row in result.fetchall()]
