import asyncio
import hashlib
from typing import List, Optional

from pydantic import BaseModel
from pydantic_ai import Agent

from weiser.evals.metrics.base import BaseEvalMetric
from weiser.evals.models import CriterionScore, EvalTestCase
from weiser.loader.models import MetricConfig

DEFAULT_JUDGE_MODEL = "anthropic:claude-sonnet-5"

EVALUATION_PARAM_EXTRACTORS = {
    "input": lambda tc: tc.golden.input,
    "final_answer": lambda tc: tc.trace.final_answer,
    "predicted_sqls": lambda tc: tc.trace.predicted_sqls,
    "query_results": lambda tc: tc.trace.query_results,
    "reference_answer_text": lambda tc: tc.golden.reference_answer_text,
    "tool_calls": lambda tc: [f"{c.tool_name}({c.args})" for c in tc.trace.tool_calls],
    "schema_catalog": lambda tc: (
        sorted(tc.schema_catalog.views.keys()) if tc.schema_catalog else None
    ),
}


class JudgeVerdict(BaseModel):
    score: float
    reason: str


class LLMJudgeMetric(BaseEvalMetric):
    """Generic declarative LLM-as-judge metric -- the YAML-native generalization of
    DeepEval's GEval. Configure entirely through MetricConfig.params:

        type: llm_judge
        name: sql_soundness           # required when a suite uses llm_judge more than once
        threshold: 0.7
        params:
          criteria: "Is the SQL structurally sound given the schema?"
          # OR: evaluation_steps: ["step 1...", "step 2..."]  (not both)
          evaluation_params: [predicted_sqls, schema_catalog]
          judge_model: anthropic:claude-sonnet-5   # optional
          applicable_when: reference_answer_text    # optional: skip (and skip the LLM
                                                       # call) unless this field is truthy

    Simplification vs. DeepEval's GEval: the judge is asked directly for a 0.0-1.0 score
    via structured output rather than a 1-5 rubric normalized by token log-probabilities.
    That token-probability trick needs provider-level logprob access DeepEval's default
    models expose; a direct score is simpler and provider-agnostic, at the cost of some
    of GEval's score smoothing.

    Judge-prompt versioning is built in from the start (not retrofitted later, per
    eval_harness_improvement_spec.md's FIX-3 lesson): every CriterionScore this metric
    produces is stamped with a hash of the rendered system prompt, so a rubric edit is
    never silently compared against results scored under the old rubric.
    """

    def __init__(self, config: MetricConfig) -> None:
        super().__init__(config)
        params = config.params or {}

        self.criteria_text: Optional[str] = params.get("criteria")
        self.evaluation_steps: Optional[List[str]] = params.get("evaluation_steps")
        if not self.criteria_text and not self.evaluation_steps:
            raise ValueError(
                f"llm_judge metric '{self.name}': requires 'criteria' or "
                "'evaluation_steps' in params"
            )
        if self.criteria_text and self.evaluation_steps:
            raise ValueError(
                f"llm_judge metric '{self.name}': provide only one of 'criteria' or "
                "'evaluation_steps', not both"
            )

        self.evaluation_params: List[str] = params.get(
            "evaluation_params", ["input", "final_answer"]
        )
        unknown = set(self.evaluation_params) - set(EVALUATION_PARAM_EXTRACTORS)
        if unknown:
            raise ValueError(
                f"llm_judge metric '{self.name}': unknown evaluation_params "
                f"{sorted(unknown)}; choose from {sorted(EVALUATION_PARAM_EXTRACTORS)}"
            )

        self.applicable_when: Optional[str] = params.get("applicable_when")
        self.judge_model: str = params.get("judge_model", DEFAULT_JUDGE_MODEL)
        self._judge_agent = Agent(
            model=self.judge_model,
            output_type=JudgeVerdict,
            system_prompt=self._system_prompt(),
        )

    def _system_prompt(self) -> str:
        if self.evaluation_steps:
            steps = "\n".join(f"{i + 1}. {s}" for i, s in enumerate(self.evaluation_steps))
            body = f"Evaluate strictly by following these steps, in order:\n{steps}"
        else:
            body = f"Evaluate the input below against this criteria: {self.criteria_text}"
        return (
            "You are a strict evaluation judge for a natural-language-to-SQL BI agent. "
            f"{body}\n\n"
            "Score from 0.0 (completely fails the criteria) to 1.0 (fully satisfies it). "
            "Respond with a numeric score and a short reason for that score."
        )

    @property
    def prompt_version(self) -> str:
        return hashlib.sha256(self._system_prompt().encode()).hexdigest()[:12]

    def _render_context(self, test_case: EvalTestCase) -> str:
        lines = []
        for key in self.evaluation_params:
            lines.append(f"{key}: {EVALUATION_PARAM_EXTRACTORS[key](test_case)}")
        return "\n".join(lines)

    def measure(self, test_case: EvalTestCase) -> CriterionScore:
        return asyncio.run(self.a_measure(test_case))

    async def a_measure(self, test_case: EvalTestCase) -> CriterionScore:
        if self.applicable_when and not EVALUATION_PARAM_EXTRACTORS.get(
            self.applicable_when, lambda _: None
        )(test_case):
            self.score = None
            return CriterionScore(criterion=self.name, applicable=False)

        context = self._render_context(test_case)
        result = await self._judge_agent.run(context)
        verdict: JudgeVerdict = result.output
        self.score = max(0.0, min(1.0, verdict.score))
        self.reason = verdict.reason
        return CriterionScore(
            criterion=self.name,
            score=self.score,
            rationale=verdict.reason,
            judge_prompt_version=self.prompt_version,
        )
