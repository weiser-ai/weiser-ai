import importlib
from abc import ABC, abstractmethod
from typing import Any, List

from weiser.evals.models import AgentTrace
from weiser.evals.semantic_layer.toolset import ToolSpec, ToolsetState
from weiser.loader.models import AgentVariant


def resolve_dotted_path(dotted_path: str, field_name: str = "entrypoint"):
    """Shared by every adapter that resolves a dotted config path (AgentVariant.
    entrypoint for pydantic_ai factories, AgentVariant.adapter_class for `framework:
    custom`). `field_name` is only used to label errors -- a bad adapter_class must not
    read as an entrypoint problem, and vice versa."""
    module_path, _, attr = dotted_path.rpartition(".")
    if not module_path:
        raise ValueError(
            f"Invalid {field_name} '{dotted_path}': expected 'module.submodule.attr'"
        )
    module = importlib.import_module(module_path)
    try:
        return getattr(module, attr)
    except AttributeError as e:
        raise ImportError(
            f"{field_name} '{dotted_path}' not found: module '{module_path}' has no "
            f"attribute '{attr}'"
        ) from e


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
