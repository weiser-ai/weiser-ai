import pytest

from weiser.evals.adapters import AgentAdapterFactory
from weiser.loader.models import AgentFramework, AgentVariant


class TestCustomFrameworkDispatch:
    def test_resolves_adapter_class(self):
        variant = AgentVariant(
            name="baseline",
            framework=AgentFramework.custom,
            entrypoint="tests.fixtures.stub_agents.build_agent",
            adapter_class="tests.fixtures.stub_adapters.StubCustomAdapter",
        )
        adapter = AgentAdapterFactory.create(variant)

        from tests.fixtures.stub_adapters import StubCustomAdapter

        assert isinstance(adapter, StubCustomAdapter)

    def test_requires_adapter_class(self):
        variant = AgentVariant(
            name="baseline",
            framework=AgentFramework.custom,
            entrypoint="tests.fixtures.stub_agents.build_agent",
        )
        with pytest.raises(Exception, match="adapter_class"):
            AgentAdapterFactory.create(variant)

    def test_bad_adapter_class_error_names_the_right_field(self):
        variant = AgentVariant(
            name="baseline",
            framework=AgentFramework.custom,
            entrypoint="tests.fixtures.stub_agents.build_agent",
            adapter_class="tests.fixtures.stub_adapters.DoesNotExist",
        )
        with pytest.raises(ImportError, match="adapter_class"):
            AgentAdapterFactory.create(variant)

    def test_pydantic_ai_framework_still_dispatches_via_map(self):
        variant = AgentVariant(
            name="baseline",
            framework=AgentFramework.pydantic_ai,
            entrypoint="tests.fixtures.stub_agents.build_agent",
        )
        adapter = AgentAdapterFactory.create(variant)

        from weiser.evals.adapters.pydantic_ai import PydanticAIAdapter

        assert isinstance(adapter, PydanticAIAdapter)
