from typing import Dict, List, Literal, Optional

from pydantic import BaseModel
from pydantic_ai import Agent

from weiser.evals.semantic_layer.base import SchemaCatalog
from weiser.loader.models import EvalGolden

DEFAULT_SYNTHESIS_MODEL = "anthropic:claude-sonnet-5"

SYSTEM_PROMPT = (
    "You are generating synthetic seed questions for evaluating a natural-language-to-SQL "
    "BI agent. Given a database view and its columns/measures/dimensions, write plausible "
    "business questions an analyst would actually ask -- the kind that would land in the "
    "analytics queue on a Tuesday: single-table lookups, filters, awkward time grains, and "
    "the metrics teams argue about. Bucket each as 'easy' (answerable from a single table, "
    "no join) or 'hard' (needs a join, an aggregation across a filter, or careful handling "
    "of an edge case). Prefer concrete, specific questions over generic ones."
)


class SynthesizedGolden(BaseModel):
    input: str
    level: Literal["easy", "hard"]
    rationale: str


class SynthesizerOutput(BaseModel):
    goldens: List[SynthesizedGolden]


async def generate_synthetic_goldens(
    schema_catalog: SchemaCatalog,
    n_per_view: int = 3,
    dq_hints: Optional[Dict[str, List[str]]] = None,
    model: str = DEFAULT_SYNTHESIS_MODEL,
    views: Optional[List[str]] = None,
) -> List[EvalGolden]:
    """Schema-driven synthetic golden generation -- the YAML-native analogue of
    DeepEval's Synthesizer and eval_harness_improvement_spec.md's FIX-8. Uses a
    dedicated "seed-writer" model, deliberately separate from both the agent under test
    and the judge model, so generation doesn't contaminate either.

    `dq_hints` (view_name -> list of known DQ quirk descriptions, typically from
    dataquality.known_issue_hints) biases some generated questions toward real, messy
    edge cases instead of clean demo-shaped ones -- generic synthetic generation
    otherwise clusters around what's easy to phrase and misses exactly the hard cases
    that matter most.

    Every golden comes back tagged source="synthetic", split="train": per the "From
    Vibes to Evals" playbook, synthetic generation should never auto-assign held_out or
    reference_values -- both stay human-curated only. Treat the output as a starting
    point requiring human review before committing, same discipline weiser's own
    production-seed extraction would use.
    """
    agent = Agent(model=model, output_type=SynthesizerOutput, system_prompt=SYSTEM_PROMPT)
    goldens: List[EvalGolden] = []
    target_views = views if views is not None else list(schema_catalog.views.keys())

    for view_name in target_views:
        view = schema_catalog.views.get(view_name)
        if view is None:
            continue
        prompt = f"View: {view_name}\nColumns/measures/dimensions: {sorted(view.members)}\n"
        hints = (dq_hints or {}).get(view_name)
        if hints:
            prompt += (
                "Known data-quality quirks on this view -- bias roughly one of the "
                f"generated questions toward probing one of these: {hints}\n"
            )
        prompt += f"Generate {n_per_view} distinct questions."

        result = await agent.run(prompt)
        for i, synthesized in enumerate(result.output.goldens):
            goldens.append(
                EvalGolden(
                    id=f"synthetic-{view_name}-{i}",
                    input=synthesized.input,
                    level=synthesized.level,
                    source="synthetic",
                    split="train",
                )
            )
    return goldens
