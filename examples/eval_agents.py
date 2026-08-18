"""Reference PydanticAI agent factory for weiser/evals.

Write one factory like this per agent family. Every AgentVariant that references
`examples.eval_agents.build_bi_agent` as its entrypoint reuses this exact function --
the *experiment design* (which tools, which prompt, which model) lives entirely in the
eval YAML's `agent_variants:` section, not here. See examples/eval-example.yaml.
"""

from pydantic_ai import Agent


def build_bi_agent(variant, tools) -> Agent:
    return Agent(
        model=variant.model or "anthropic:claude-sonnet-5",
        system_prompt=variant.system_prompt or "You are a careful BI analyst.",
        tools=tools,
        model_settings=variant.model_settings,
    )
