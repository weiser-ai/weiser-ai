"""Planned, not implemented yet.

A StrandsAdapter(AgentAdapter) targeting AWS Strands
(https://strandsagents.com/docs/user-guide/quickstart/python/) would follow the exact
same build()/run() contract as PydanticAIAdapter. Deferred until the PydanticAI adapter
has proven the AgentAdapter protocol is actually sufficient in practice.
"""

from typing import Any, List

from weiser.evals.adapters.base import AgentAdapter
from weiser.evals.models import AgentTrace
from weiser.evals.semantic_layer.toolset import ToolSpec, ToolsetState
from weiser.loader.models import AgentVariant


class StrandsAdapter(AgentAdapter):
    def build(self, variant: AgentVariant, tools: List[ToolSpec]) -> Any:
        raise NotImplementedError("Strands agent support is planned but not implemented yet.")

    async def run(
        self,
        built_agent: Any,
        question: str,
        state: ToolsetState,
        max_turns: int,
    ) -> AgentTrace:
        raise NotImplementedError("Strands agent support is planned but not implemented yet.")
