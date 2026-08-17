from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlglot.dialects import Postgres

from weiser.drivers.base import BaseDriver
from weiser.evals.semantic_layer.base import execute_sql_via_driver
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


def _driver_with_rows(rows=([(3,)], ["cnt"])):
    """Mock driver with a real sqlglot dialect and an engine returning canned rows."""
    fetchall, keys = rows
    driver = Mock(spec=BaseDriver)
    driver.dialect = Postgres()
    result = Mock()
    result.keys.return_value = keys
    result.fetchall.return_value = fetchall
    conn = Mock()
    conn.execute.return_value = result
    context_manager = MagicMock()
    context_manager.__enter__ = Mock(return_value=conn)
    context_manager.__exit__ = Mock(return_value=None)
    engine = Mock()
    engine.connect.return_value = context_manager
    driver.engine = engine
    return driver, conn


class TestExecuteSqlViaDriver:
    def test_select_is_executed_and_rows_returned(self):
        driver, conn = _driver_with_rows()
        rows = execute_sql_via_driver(driver, "SELECT COUNT(*) AS cnt FROM merchants")
        assert rows == [{"cnt": 3}]
        conn.execute.assert_called_once()

    def test_select_with_zero_rows_is_a_valid_result(self):
        driver, conn = _driver_with_rows(rows=([], ["cnt"]))
        assert execute_sql_via_driver(driver, "SELECT COUNT(*) AS cnt FROM merchants") == []

    @pytest.mark.parametrize(
        "sql",
        [
            "DROP TABLE merchants",
            "DELETE FROM merchants",
            "INSERT INTO merchants (id) VALUES (1)",
            "UPDATE merchants SET name = 'x'",
            "TRUNCATE TABLE merchants",
            "ALTER TABLE merchants ADD COLUMN x int",
            "CREATE TABLE t (id int)",
            "GRANT ALL ON merchants TO u",
            "SELECT 1; DROP TABLE merchants",
            "WITH d AS (DELETE FROM merchants RETURNING *) SELECT count(*) FROM d",
        ],
    )
    def test_non_read_only_sql_is_rejected_without_touching_engine(self, sql):
        driver, conn = _driver_with_rows()
        with pytest.raises(ValueError):
            execute_sql_via_driver(driver, sql)
        conn.execute.assert_not_called()

    def test_union_query_is_allowed(self):
        driver, conn = _driver_with_rows()
        execute_sql_via_driver(driver, "SELECT id FROM a UNION SELECT id FROM b")
        conn.execute.assert_called_once()

    def test_semicolon_inside_string_literal_is_allowed(self):
        driver, conn = _driver_with_rows()
        execute_sql_via_driver(driver, "SELECT * FROM t WHERE note = 'a;b'")
        conn.execute.assert_called_once()

    def test_unparseable_sql_without_write_keywords_falls_back_to_execution(self):
        driver, conn = _driver_with_rows()
        with patch("weiser.evals.semantic_layer.base.sqlglot.parse", side_effect=Exception("unparseable")):
            execute_sql_via_driver(driver, "SELECT 1")
        conn.execute.assert_called_once()

    def test_unparseable_sql_with_write_keywords_is_rejected(self):
        driver, conn = _driver_with_rows()
        with patch("weiser.evals.semantic_layer.base.sqlglot.parse", side_effect=Exception("unparseable")):
            with pytest.raises(ValueError):
                execute_sql_via_driver(driver, "EXOTIC SYNTAX DROP TABLE t")
        conn.execute.assert_not_called()

    def test_unparseable_multi_statement_sql_is_rejected(self):
        driver, conn = _driver_with_rows()
        with patch("weiser.evals.semantic_layer.base.sqlglot.parse", side_effect=Exception("unparseable")):
            with pytest.raises(ValueError):
                execute_sql_via_driver(driver, "SELECT 1; SELECT 2")
        conn.execute.assert_not_called()
