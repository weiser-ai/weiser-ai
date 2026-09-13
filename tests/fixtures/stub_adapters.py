"""A minimal `framework: custom` AgentAdapter, resolved via AgentVariant.adapter_class in
tests. Proves the custom-adapter escape hatch end-to-end without any real agent
framework -- the whole point of `framework: custom` is that the harness never needs to
know what's inside `build()`/`run()`."""

from typing import Any, List

from weiser.evals.adapters.base import AgentAdapter
from weiser.evals.models import AgentTrace
from weiser.evals.semantic_layer.toolset import ToolSpec, ToolsetState
from weiser.loader.models import AgentVariant


class StubCustomAdapter(AgentAdapter):
    def build(self, variant: AgentVariant, tools: List[ToolSpec]) -> Any:
        return variant

    async def run(
        self,
        built_agent: Any,
        question: str,
        state: ToolsetState,
        max_turns: int,
    ) -> AgentTrace:
        return AgentTrace(
            question=question,
            final_answer="There are 3 merchants.",
            predicted_sqls=["SELECT COUNT(*) AS cnt FROM merchants"],
            query_results=[{"cnt": 3}],
        )
