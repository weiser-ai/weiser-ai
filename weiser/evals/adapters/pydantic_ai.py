import importlib
import time
from typing import List

from pydantic_ai import Agent, Tool
from pydantic_ai.exceptions import UsageLimitExceeded
from pydantic_ai.usage import UsageLimits

from weiser.evals.adapters.base import AgentAdapter
from weiser.evals.models import AgentTrace
from weiser.evals.semantic_layer.toolset import ToolSpec, ToolsetState
from weiser.loader.models import AgentVariant


def _resolve_entrypoint(dotted_path: str):
    module_path, _, attr = dotted_path.rpartition(".")
    if not module_path:
        raise ValueError(
            f"Invalid entrypoint '{dotted_path}': expected 'module.submodule.factory_fn'"
        )
    module = importlib.import_module(module_path)
    try:
        return getattr(module, attr)
    except AttributeError as e:
        raise ImportError(
            f"Entrypoint '{dotted_path}' not found: module '{module_path}' has no "
            f"attribute '{attr}'"
        ) from e


class PydanticAIAdapter(AgentAdapter):
    """Reference agent adapter targeting PydanticAI. `variant.entrypoint` is a factory
    written once per agent family: (variant: AgentVariant, tools: list[pydantic_ai.Tool])
    -> pydantic_ai.Agent. All experiment-design knobs (tools/prompt/model) live in the
    YAML AgentVariant; the factory just wires them into a real Agent object. A factory
    may also ignore the knobs entirely and return an already-fully-built prod agent
    (bring-your-own-agent / fixed-agent mode) for suites that just want to evaluate
    "whatever is actually in prod right now" as a single-arm baseline."""

    def build(self, variant: AgentVariant, tools: List[ToolSpec]) -> Agent:
        factory = _resolve_entrypoint(variant.entrypoint)
        pai_tools = [
            Tool(t.fn, name=t.name, description=t.description) for t in tools
        ]
        return factory(variant, pai_tools)

    async def run(
        self,
        built_agent: Agent,
        question: str,
        state: ToolsetState,
        max_turns: int,
    ) -> AgentTrace:
        start = time.monotonic()
        error = None
        hit_limit = False
        result = None
        try:
            result = await built_agent.run(
                question, usage_limits=UsageLimits(request_limit=max_turns)
            )
        except UsageLimitExceeded:
            hit_limit = True
        except Exception as e:  # noqa: BLE001 -- surfaced on the row, not raised, so
            error = str(e)  # one bad question doesn't kill an entire suite run
        elapsed = time.monotonic() - start

        usage = result.usage if result is not None else None
        final_answer = state.final_answer
        if final_answer is None and result is not None:
            # AgentRunResult's parsed output lives on `.output`, not `.response`
            # (`.response` exists too, but returns the raw last ModelResponse message).
            final_answer = (
                result.output if isinstance(result.output, str) else str(result.output)
            )

        predicted_sqls = [
            tc.args["sql"]
            for tc in state.tool_calls
            if tc.tool_name in ("query", "submit_answer") and tc.args.get("sql")
        ]

        return AgentTrace(
            question=question,
            tool_calls=state.tool_calls,
            predicted_sqls=predicted_sqls,
            final_answer=final_answer,
            query_results=state.final_rows,
            hit_limit=hit_limit,
            elapsed_s=elapsed,
            cost_usd=float(usage.cost) if usage is not None and usage.cost else 0.0,
            prompt_tokens=usage.input_tokens if usage is not None else 0,
            completion_tokens=usage.output_tokens if usage is not None else 0,
            error=error or state.error,
        )
