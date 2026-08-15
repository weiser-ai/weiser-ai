from abc import ABC, abstractmethod
from typing import Any, List

from weiser.evals.models import AgentTrace
from weiser.evals.semantic_layer.toolset import ToolSpec, ToolsetState
from weiser.loader.models import AgentVariant


class AgentAdapter(ABC):
    """Bridges a declaratively-configured AgentVariant to a concrete agent-framework
    object. build() constructs a fresh agent for one question (cheap -- no network calls
    at construction time), wired to that question's already-state-bound tools; run()
    executes it and normalizes the framework's own result shape into the
    framework-agnostic AgentTrace."""

    @abstractmethod
    def build(self, variant: AgentVariant, tools: List[ToolSpec]) -> Any: ...

    @abstractmethod
    async def run(
        self,
        built_agent: Any,
        question: str,
        state: ToolsetState,
        max_turns: int,
    ) -> AgentTrace: ...
