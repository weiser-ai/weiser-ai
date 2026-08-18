from typing import Dict, Type

from weiser.evals.adapters.base import AgentAdapter
from weiser.evals.adapters.pydantic_ai import PydanticAIAdapter
from weiser.loader.models import AgentFramework

AGENT_ADAPTER_MAP: Dict[AgentFramework, Type[AgentAdapter]] = {
    AgentFramework.pydantic_ai: PydanticAIAdapter,
}


class AgentAdapterFactory:
    @staticmethod
    def create(framework: AgentFramework) -> AgentAdapter:
        adapter_class = AGENT_ADAPTER_MAP.get(framework, None)
        if not adapter_class:
            raise Exception(f"Agent framework {framework} not implemented yet")
        return adapter_class()
