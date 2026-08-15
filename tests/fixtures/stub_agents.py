"""A minimal PydanticAI agent factory backed by FunctionModel, used to exercise
weiser.evals.adapters.pydantic_ai.PydanticAIAdapter end-to-end without hitting a real
LLM API. Resolved via AgentVariant.entrypoint in tests."""

from pydantic_ai import Agent
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel


def _scripted_model_fn(messages, info: AgentInfo) -> ModelResponse:
    responses_so_far = sum(1 for m in messages if isinstance(m, ModelResponse))
    if responses_so_far == 0:
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name="query",
                    args={"sql": "SELECT COUNT(*) AS cnt FROM merchants"},
                )
            ]
        )
    if responses_so_far == 1:
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name="submit_answer",
                    args={
                        "sql": "SELECT COUNT(*) AS cnt FROM merchants",
                        "answer": "There are 3 merchants.",
                    },
                )
            ]
        )
    return ModelResponse(parts=[TextPart(content="done")])


def build_agent(variant, tools) -> Agent:
    return Agent(
        model=FunctionModel(_scripted_model_fn),
        system_prompt=variant.system_prompt or "You are a BI agent.",
        tools=tools,
    )
