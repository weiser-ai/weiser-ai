from datetime import datetime
from typing import List, Optional

import httpx

from weiser.drivers import DriverFactory
from weiser.drivers.base import BaseDriver
from weiser.loader.models import Datasource, SemanticLayerConfig
from weiser.evals.semantic_layer.base import (
    SchemaCatalog,
    SchemaView,
    SemanticLayerAdapter,
    execute_sql_via_driver,
)


class CubeSemanticLayer(SemanticLayerAdapter):
    """Real Cube.js semantic-layer metadata client (cubejs-api/v1/meta), a genuine new
    capability -- weiser previously only ever spoke to Cube via its Postgres-wire SQL
    API, with no metadata/schema introspection at all. Query execution still goes
    through that existing SQL API via the normal weiser driver/engine.

    Known limitation, documented rather than silently assumed correct: Cube measure and
    dimension names are meta-API-qualified as "CubeName.memberName", but how the SQL API
    surfaces those in generated SQL (aliased vs. qualified) can vary. Both the full
    qualified name and the bare member name are added to SchemaView.members so a
    membership check has a reasonable chance to match either form; treat column-level
    membership results as best-effort, not a hard signal, until verified against real
    generated SQL for a given Cube deployment.
    """

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
        if not self.config.meta_api_url:
            raise ValueError(
                f"semantic_layer '{self.config.name}': meta_api_url is required for "
                "Cube schema introspection"
            )
        headers = {}
        if self.config.meta_api_token:
            headers["Authorization"] = self.config.meta_api_token.get_secret_value()
        url = f"{self.config.meta_api_url.rstrip('/')}/cubejs-api/v1/meta"
        response = httpx.get(url, headers=headers, timeout=30.0)
        response.raise_for_status()
        payload = response.json()

        views = {}
        for cube in payload.get("cubes", []):
            cube_name = cube["name"]
            members = set()
            for member in cube.get("measures", []) + cube.get("dimensions", []):
                full_name = member["name"].lower()
                members.add(full_name)
                if "." in full_name:
                    members.add(full_name.split(".", 1)[1])
            views[cube_name.lower()] = SchemaView(name=cube_name, members=members)

        self._schema = SchemaCatalog(
            views=views, has_semantics=True, fetched_at=datetime.now()
        )
        return self._schema

    def execute_query(self, sql: str) -> List[dict]:
        return execute_sql_via_driver(self.driver, sql)

    def get_dialect(self):
        return self.driver.dialect
