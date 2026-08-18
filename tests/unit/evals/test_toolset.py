import asyncio
from unittest.mock import Mock

from sqlglot.dialects import BigQuery, Postgres

from weiser.evals.semantic_layer.toolset import ToolsetState, _cap_query_rows, build_toolset

from tests.fixtures.config_fixtures import *  # noqa: F401,F403


class TestCapQueryRows:
    def test_adds_limit_when_missing(self):
        assert _cap_query_rows("SELECT * FROM t", 50) == "SELECT * FROM t LIMIT 50"

    def test_caps_existing_larger_limit(self):
        assert _cap_query_rows("SELECT * FROM t LIMIT 1000", 50) == "SELECT * FROM t LIMIT 50"

    def test_keeps_existing_smaller_limit(self):
        assert _cap_query_rows("SELECT * FROM t LIMIT 10", 50) == "SELECT * FROM t LIMIT 10"

    def test_keeps_existing_equal_limit(self):
        assert _cap_query_rows("SELECT * FROM t LIMIT 50", 50) == "SELECT * FROM t LIMIT 50"

    def test_keeps_limit_with_offset(self):
        assert (
            _cap_query_rows("SELECT * FROM t LIMIT 1000 OFFSET 5", 50)
            == "SELECT * FROM t LIMIT 50 OFFSET 5"
        )

    def test_handles_trailing_semicolon(self):
        assert _cap_query_rows("SELECT * FROM t;", 50) == "SELECT * FROM t LIMIT 50"

    def test_union_gets_outer_limit(self):
        result = _cap_query_rows("SELECT id FROM a UNION SELECT id FROM b", 50)
        assert result.endswith("LIMIT 50")

    def test_non_literal_limit_is_left_unchanged(self):
        sql = "SELECT * FROM t LIMIT ALL"
        assert _cap_query_rows(sql, 50, dialect=Postgres()) == sql

    def test_unparseable_sql_is_left_unchanged(self):
        sql = "EXOTIC SYNTAX FROM nowhere"
        assert _cap_query_rows(sql, 50) == sql

    def test_non_select_statement_is_left_unchanged(self):
        sql = "DROP TABLE t"
        assert _cap_query_rows(sql, 50) == sql

    def test_dialect_is_preserved_on_rewrite(self):
        result = _cap_query_rows("SELECT * FROM `proj.dataset.t`", 50, dialect=BigQuery())
        assert "proj.dataset.t" in result
        assert result.endswith("LIMIT 50")


class TestQueryTool:
    def test_query_executes_capped_sql_but_records_original(self, mock_semantic_layer):
        state = ToolsetState()
        tools = build_toolset(mock_semantic_layer, state, allowed={"query"})
        query_tool = {t.name: t for t in tools}["query"]

        result = asyncio.run(query_tool.fn("SELECT * FROM merchants"))

        mock_semantic_layer.execute_query.assert_called_once_with(
            "SELECT * FROM merchants LIMIT 50"
        )
        assert result == "{'cnt': 3}"
        assert state.tool_calls[0].args == {"sql": "SELECT * FROM merchants"}
        assert state.tool_calls[0].result == [{"cnt": 3}]

    def test_query_caps_existing_larger_limit(self, mock_semantic_layer):
        state = ToolsetState()
        tools = build_toolset(mock_semantic_layer, state, allowed={"query"})
        query_tool = {t.name: t for t in tools}["query"]

        asyncio.run(query_tool.fn("SELECT * FROM merchants LIMIT 10000"))

        mock_semantic_layer.execute_query.assert_called_once_with(
            "SELECT * FROM merchants LIMIT 50"
        )

    def test_query_keeps_existing_smaller_limit(self, mock_semantic_layer):
        state = ToolsetState()
        tools = build_toolset(mock_semantic_layer, state, allowed={"query"})
        query_tool = {t.name: t for t in tools}["query"]

        asyncio.run(query_tool.fn("SELECT * FROM merchants LIMIT 10"))

        mock_semantic_layer.execute_query.assert_called_once_with(
            "SELECT * FROM merchants LIMIT 10"
        )

    def test_query_slices_rows_client_side_as_backstop(self, mock_semantic_layer):
        mock_semantic_layer.execute_query = Mock(return_value=[{"i": i} for i in range(120)])
        state = ToolsetState()
        tools = build_toolset(mock_semantic_layer, state, allowed={"query"})
        query_tool = {t.name: t for t in tools}["query"]

        result = asyncio.run(query_tool.fn("SELECT i FROM t"))

        assert len(result.splitlines()) == 50
        assert state.tool_calls[0].result == [{"i": i} for i in range(50)]

    def test_query_records_original_sql_on_error(self, mock_semantic_layer):
        mock_semantic_layer.execute_query = Mock(side_effect=Exception("boom"))
        state = ToolsetState()
        tools = build_toolset(mock_semantic_layer, state, allowed={"query"})
        query_tool = {t.name: t for t in tools}["query"]

        result = asyncio.run(query_tool.fn("SELECT * FROM merchants"))

        assert result == "ERROR: boom"
        assert state.tool_calls[0].args == {"sql": "SELECT * FROM merchants"}

    def test_query_returns_no_rows_message_for_empty_result(self, mock_semantic_layer):
        mock_semantic_layer.execute_query = Mock(return_value=[])
        state = ToolsetState()
        tools = build_toolset(mock_semantic_layer, state, allowed={"query"})
        query_tool = {t.name: t for t in tools}["query"]

        result = asyncio.run(query_tool.fn("SELECT * FROM merchants"))

        assert result == "(no rows)"
