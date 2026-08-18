from datetime import datetime
from typing import List, Optional

from sqlalchemy import inspect as sa_inspect

from weiser.drivers import DriverFactory
from weiser.drivers.base import BaseDriver
from weiser.loader.models import Datasource, SemanticLayerConfig
from weiser.evals.semantic_layer.base import (
    SchemaCatalog,
    SchemaView,
    SemanticLayerAdapter,
    execute_sql_via_driver,
)


class GenericSQLSemanticLayer(SemanticLayerAdapter):
    """Schema introspection via SQLAlchemy's Inspector against any existing weiser SQL
    driver (Postgres/Snowflake/BigQuery/etc). No measure/dimension distinction, just
    tables/views and their columns -- flagged via has_semantics=False so downstream
    code can tell "real semantic layer" from "raw SQL introspection"."""

    def __init__(
        self,
        config: SemanticLayerConfig,
        datasource: Datasource,
        driver: Optional[BaseDriver] = None,
    ) -> None:
        self.config = config
        self.datasource = datasource
        self.driver = driver or DriverFactory.create_driver(datasource)
        self._schema: Optional[SchemaCatalog] = None

    def get_schema(self, refresh: bool = False) -> SchemaCatalog:
        if self._schema is not None and not refresh:
            return self._schema
        inspector = sa_inspect(self.driver.engine)
        views = {}
        for table_name in inspector.get_table_names():
            columns = {c["name"].lower() for c in inspector.get_columns(table_name)}
            views[table_name.lower()] = SchemaView(name=table_name, members=columns)
        try:
            view_names = inspector.get_view_names()
        except NotImplementedError:
            view_names = []
        for view_name in view_names:
            if view_name.lower() in views:
                continue
            columns = {c["name"].lower() for c in inspector.get_columns(view_name)}
            views[view_name.lower()] = SchemaView(name=view_name, members=columns)
        self._schema = SchemaCatalog(
            views=views, has_semantics=False, fetched_at=datetime.now()
        )
        return self._schema

    def execute_query(self, sql: str) -> List[dict]:
        return execute_sql_via_driver(self.driver, sql)

    def get_dialect(self):
        return self.driver.dialect
