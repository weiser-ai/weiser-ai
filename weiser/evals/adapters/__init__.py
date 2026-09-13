from typing import Dict, Type

from weiser.evals.adapters.base import AgentAdapter, resolve_dotted_path
from weiser.evals.adapters.pydantic_ai import PydanticAIAdapter
from weiser.loader.models import AgentFramework, AgentVariant

AGENT_ADAPTER_MAP: Dict[AgentFramework, Type[AgentAdapter]] = {
    AgentFramework.pydantic_ai: PydanticAIAdapter,
}


class AgentAdapterFactory:
    @staticmethod
    def create(variant: AgentVariant) -> AgentAdapter:
        if variant.framework == AgentFramework.custom:
            if not variant.adapter_class:
                raise Exception(
                    f"Agent variant '{variant.name}': framework 'custom' requires "
                    "'adapter_class' (a dotted path to an AgentAdapter subclass)"
                )
            adapter_class = resolve_dotted_path(variant.adapter_class, field_name="adapter_class")
            return adapter_class()

        adapter_class = AGENT_ADAPTER_MAP.get(variant.framework, None)
        if not adapter_class:
            raise Exception(f"Agent framework {variant.framework} not implemented yet")
        return adapter_class()
