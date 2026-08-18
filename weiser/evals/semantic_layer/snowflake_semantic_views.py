"""Planned, not implemented yet.

Snowflake Semantic Views (`CREATE SEMANTIC VIEW`, introspectable via
`DESCRIBE SEMANTIC VIEW` / `SHOW SEMANTIC VIEWS`) would back a
`SnowflakeSemanticViewsAdapter(SemanticLayerAdapter)` here, following the exact same
get_schema()/execute_query() contract as CubeSemanticLayer and GenericSQLSemanticLayer.
Deferred until the Cube and generic-SQL adapters have proven the adapter protocol is
actually sufficient in practice.
"""

from weiser.evals.semantic_layer.base import SemanticLayerAdapter


class SnowflakeSemanticViewsAdapter(SemanticLayerAdapter):
    def get_schema(self, refresh: bool = False):
        raise NotImplementedError(
            "Snowflake Semantic Views support is planned but not implemented yet."
        )

    def execute_query(self, sql: str):
        raise NotImplementedError(
            "Snowflake Semantic Views support is planned but not implemented yet."
        )
