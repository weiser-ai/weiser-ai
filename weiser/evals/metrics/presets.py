"""Pre-built LLM-judge presets, expressed as MetricConfig builders rather than new
metric classes -- consistent with the Evals-As-YAML philosophy, a "preset" here is just
a documented, reusable shape for `type: llm_judge`'s `params`. Each function's output is
exactly what you'd write by hand in a suite's `metrics:` list; use them from Python
config, or copy the YAML shown in each docstring.

Mirrors the three judge criteria from eval_harness_improvement_spec.md's judge.py, with
the mechanical parts (schema membership, expected-view recall) already carried by the
Phase-1 deterministic metrics so these three stay scoped to what's genuinely subjective.
"""

from weiser.loader.models import MetricConfig


def answer_correctness_metric_config(
    threshold: float = 0.7, judge_model: str = None
) -> MetricConfig:
    """
    metrics:
      - type: llm_judge
        name: answer_correctness
        threshold: 0.7
        params:
          applicable_when: reference_answer_text
          evaluation_params: [input, final_answer, reference_answer_text]
          criteria: >
            Compare the agent's substantive claims (numbers, trends, named entities)
            against the reference answer. Score 0.0 for a material factual or numeric
            contradiction, 1.0 when the substance matches even if phrasing differs. Do
            not penalize stylistic differences, extra correct detail, or a different but
            equally valid framing of the same facts.
    """
    params = {
        "applicable_when": "reference_answer_text",
        "evaluation_params": ["input", "final_answer", "reference_answer_text"],
        "criteria": (
            "Compare the agent's substantive claims (numbers, trends, named entities) "
            "against the reference answer. Score 0.0 for a material factual or numeric "
            "contradiction, 1.0 when the substance matches even if phrasing differs. Do "
            "not penalize stylistic differences, extra correct detail, or a different "
            "but equally valid framing of the same facts."
        ),
    }
    if judge_model:
        params["judge_model"] = judge_model
    return MetricConfig(type="llm_judge", name="answer_correctness", threshold=threshold, params=params)


def sql_soundness_metric_config(
    threshold: float = 0.5, judge_model: str = None
) -> MetricConfig:
    """
    metrics:
      - type: llm_judge
        name: sql_soundness
        threshold: 0.5
        params:
          evaluation_params: [input, predicted_sqls, schema_catalog]
          criteria: >
            Given the question and the known schema, is the submitted SQL structurally
            sound and a plausible way to answer the question? Judge plausibility only --
            you do not have live execution results here (that is covered separately by
            reference_value_match and groundedness). Do not penalize style choices
            (aliasing, formatting, CTE vs. subquery).
    """
    params = {
        "evaluation_params": ["input", "predicted_sqls", "schema_catalog"],
        "criteria": (
            "Given the question and the known schema, is the submitted SQL "
            "structurally sound and a plausible way to answer the question? Judge "
            "plausibility only -- you do not have live execution results here (that is "
            "covered separately by reference_value_match and groundedness). Do not "
            "penalize style choices (aliasing, formatting, CTE vs. subquery)."
        ),
    }
    if judge_model:
        params["judge_model"] = judge_model
    return MetricConfig(type="llm_judge", name="sql_soundness", threshold=threshold, params=params)


def groundedness_metric_config(
    threshold: float = 0.7, judge_model: str = None
) -> MetricConfig:
    """
    metrics:
      - type: llm_judge
        name: groundedness
        threshold: 0.7
        params:
          evaluation_params: [final_answer, query_results]
          criteria: >
            Does the final natural-language answer accurately reflect the query_results
            returned, without fabricating or omitting material facts? This checks
            internal consistency only -- it does not check whether query_results are
            themselves correct (that is a data-quality/reference-value question).
    """
    params = {
        "evaluation_params": ["final_answer", "query_results"],
        "criteria": (
            "Does the final natural-language answer accurately reflect the "
            "query_results returned, without fabricating or omitting material facts? "
            "This checks internal consistency only -- it does not check whether "
            "query_results are themselves correct (that is a data-quality/"
            "reference-value question)."
        ),
    }
    if judge_model:
        params["judge_model"] = judge_model
    return MetricConfig(type="llm_judge", name="groundedness", threshold=threshold, params=params)
