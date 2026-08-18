import asyncio

import pytest

from weiser.evals.adapters.pydantic_ai import PydanticAIAdapter, _resolve_entrypoint
from weiser.evals.semantic_layer.toolset import ToolsetState, build_toolset

from tests.fixtures.config_fixtures import *  # noqa: F401,F403


class TestPydanticAIAdapter:
    def test_build_and_run_normalizes_trace(self, sample_agent_variant, mock_semantic_layer):
        state = ToolsetState()
        tools = build_toolset(mock_semantic_layer, state, allowed=None)
        adapter = PydanticAIAdapter()
        built = adapter.build(sample_agent_variant, tools)

        trace = asyncio.run(
            adapter.run(built, "How many merchants are there?", state, max_turns=10)
        )

        assert trace.final_answer == "There are 3 merchants."
        assert trace.predicted_sqls == [
            "SELECT COUNT(*) AS cnt FROM merchants",
            "SELECT COUNT(*) AS cnt FROM merchants",
        ]
        assert trace.query_results == [{"cnt": 3}]
        assert trace.hit_limit is False
        assert len(trace.tool_calls) == 2
        assert trace.error is None

    def test_tool_filtering_only_exposes_allowed_tools(self, sample_agent_variant, mock_semantic_layer):
        state = ToolsetState()
        tools = build_toolset(mock_semantic_layer, state, allowed={"query", "submit_answer"})
        assert sorted(t.name for t in tools) == ["query", "submit_answer"]


class TestResolveEntrypoint:
    def test_raises_for_invalid_path(self):
        with pytest.raises(ValueError):
            _resolve_entrypoint("not_a_dotted_path")

    def test_raises_for_missing_attribute(self):
        with pytest.raises(ImportError):
            _resolve_entrypoint("tests.fixtures.stub_agents.does_not_exist")

    def test_resolves_real_factory(self):
        from tests.fixtures.stub_agents import build_agent

        assert _resolve_entrypoint("tests.fixtures.stub_agents.build_agent") is build_agent
