from unittest.mock import Mock, patch

import pytest

from weiser.evals.semantic_layer.cube import CubeSemanticLayer
from weiser.evals.semantic_layer.generic_sql import GenericSQLSemanticLayer

from tests.fixtures.config_fixtures import *  # noqa: F401,F403


class TestGenericSQLSemanticLayer:
    def test_get_schema_uses_inspector(self, sample_semantic_layer_config, sample_postgresql_datasource, mock_driver):
        mock_inspector = Mock()
        mock_inspector.get_table_names.return_value = ["merchants"]
        mock_inspector.get_view_names.return_value = []
        mock_inspector.get_columns.return_value = [
            {"name": "id"}, {"name": "name"}, {"name": "country"}
        ]

        with patch("weiser.evals.semantic_layer.generic_sql.sa_inspect", return_value=mock_inspector):
            adapter = GenericSQLSemanticLayer(
                sample_semantic_layer_config, sample_postgresql_datasource, driver=mock_driver
            )
            schema = adapter.get_schema()

        assert "merchants" in schema.views
        assert schema.views["merchants"].members == {"id", "name", "country"}
        assert schema.has_semantics is False

    def test_get_schema_is_cached_until_refresh(self, sample_semantic_layer_config, sample_postgresql_datasource, mock_driver):
        mock_inspector = Mock()
        mock_inspector.get_table_names.return_value = ["merchants"]
        mock_inspector.get_view_names.return_value = []
        mock_inspector.get_columns.return_value = [{"name": "id"}]

        with patch("weiser.evals.semantic_layer.generic_sql.sa_inspect", return_value=mock_inspector) as mock_inspect:
            adapter = GenericSQLSemanticLayer(
                sample_semantic_layer_config, sample_postgresql_datasource, driver=mock_driver
            )
            adapter.get_schema()
            adapter.get_schema()
            assert mock_inspect.call_count == 1
            adapter.get_schema(refresh=True)
            assert mock_inspect.call_count == 2

    def test_get_view_names_not_implemented_is_tolerated(self, sample_semantic_layer_config, sample_postgresql_datasource, mock_driver):
        mock_inspector = Mock()
        mock_inspector.get_table_names.return_value = ["merchants"]
        mock_inspector.get_view_names.side_effect = NotImplementedError()
        mock_inspector.get_columns.return_value = [{"name": "id"}]

        with patch("weiser.evals.semantic_layer.generic_sql.sa_inspect", return_value=mock_inspector):
            adapter = GenericSQLSemanticLayer(
                sample_semantic_layer_config, sample_postgresql_datasource, driver=mock_driver
            )
            schema = adapter.get_schema()

        assert "merchants" in schema.views


class TestCubeSemanticLayer:
    def test_get_schema_parses_meta_api_response(self, sample_cube_semantic_layer_config, sample_cube_datasource, mock_driver):
        fake_response = Mock()
        fake_response.raise_for_status = Mock()
        fake_response.json.return_value = {
            "cubes": [
                {
                    "name": "Orders",
                    "measures": [{"name": "Orders.count"}],
                    "dimensions": [{"name": "Orders.status"}],
                }
            ]
        }

        with patch("weiser.evals.semantic_layer.cube.httpx.get", return_value=fake_response) as mock_get:
            adapter = CubeSemanticLayer(
                sample_cube_semantic_layer_config, sample_cube_datasource, driver=mock_driver
            )
            schema = adapter.get_schema()

        mock_get.assert_called_once()
        assert schema.has_semantics is True
        assert "orders" in schema.views
        members = schema.views["orders"].members
        assert "orders.count" in members
        assert "count" in members
        assert "orders.status" in members
        assert "status" in members

    def test_get_schema_requires_meta_api_url(self, sample_postgresql_datasource, mock_driver):
        from weiser.loader.models import SemanticLayerConfig, SemanticLayerType

        config = SemanticLayerConfig(name="cube_sl", type=SemanticLayerType.cube, datasource="cube_db")
        adapter = CubeSemanticLayer(config, sample_postgresql_datasource, driver=mock_driver)

        with pytest.raises(ValueError):
            adapter.get_schema()
