from typing import Dict, Type

from weiser.evals.semantic_layer.base import SemanticLayerAdapter
from weiser.evals.semantic_layer.cube import CubeSemanticLayer
from weiser.evals.semantic_layer.generic_sql import GenericSQLSemanticLayer
from weiser.loader.models import Datasource, SemanticLayerConfig, SemanticLayerType

SEMANTIC_LAYER_MAP: Dict[SemanticLayerType, Type[SemanticLayerAdapter]] = {
    SemanticLayerType.cube: CubeSemanticLayer,
    SemanticLayerType.generic_sql: GenericSQLSemanticLayer,
}


class SemanticLayerFactory:
    @staticmethod
    def create(
        config: SemanticLayerConfig, datasource: Datasource
    ) -> SemanticLayerAdapter:
        adapter_class = SEMANTIC_LAYER_MAP.get(config.type, None)
        if not adapter_class:
            raise Exception(f"Semantic layer type {config.type} not implemented yet")
        return adapter_class(config, datasource)
