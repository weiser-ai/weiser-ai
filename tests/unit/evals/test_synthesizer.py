import asyncio
from datetime import datetime

from pydantic_ai.messages import ModelResponse, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel

from weiser.evals.semantic_layer.base import SchemaCatalog, SchemaView
from weiser.evals.synthesizer import generate_synthetic_goldens

SCHEMA = SchemaCatalog(
    views={
        "merchants": SchemaView(name="merchants", members={"id", "name", "country"}),
        "payments": SchemaView(name="payments", members={"id", "merchant_id", "amount"}),
    },
    has_semantics=False,
    fetched_at=datetime.now(),
)


def _scripted_synthesizer(prompts_seen):
    def fn(messages, info: AgentInfo) -> ModelResponse:
        prompts_seen.append(messages[-1].parts[-1].content)
        tool_name = info.output_tools[0].name
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=tool_name,
                    args={
                        "goldens": [
                            {"input": "How many rows are there?", "level": "easy", "rationale": "single table"},
                            {"input": "What is the trend over time?", "level": "hard", "rationale": "needs a join"},
                        ]
                    },
                )
            ]
        )

    return FunctionModel(fn)


class TestGenerateSyntheticGoldens:
    def test_generates_goldens_per_view(self):
        prompts_seen = []
        goldens = asyncio.run(
            generate_synthetic_goldens(
                SCHEMA, n_per_view=2, model=_scripted_synthesizer(prompts_seen)
            )
        )
        assert len(goldens) == 4  # 2 views x 2 goldens each
        assert {g.source for g in goldens} == {"synthetic"}
        assert {g.split for g in goldens} == {"train"}
        assert len(prompts_seen) == 2

    def test_restricts_to_requested_views(self):
        prompts_seen = []
        goldens = asyncio.run(
            generate_synthetic_goldens(
                SCHEMA,
                n_per_view=2,
                model=_scripted_synthesizer(prompts_seen),
                views=["merchants"],
            )
        )
        assert len(goldens) == 2
        assert all(g.id.startswith("synthetic-merchants-") for g in goldens)

    def test_dq_hints_are_injected_into_the_prompt(self):
        prompts_seen = []
        asyncio.run(
            generate_synthetic_goldens(
                SCHEMA,
                n_per_view=1,
                model=_scripted_synthesizer(prompts_seen),
                views=["merchants"],
                dq_hints={"merchants": ["merchants_not_empty is currently failing"]},
            )
        )
        assert any("merchants_not_empty is currently failing" in p for p in prompts_seen)

    def test_unknown_requested_view_is_skipped(self):
        prompts_seen = []
        goldens = asyncio.run(
            generate_synthetic_goldens(
                SCHEMA,
                n_per_view=1,
                model=_scripted_synthesizer(prompts_seen),
                views=["ghost_view"],
            )
        )
        assert goldens == []
        assert prompts_seen == []
