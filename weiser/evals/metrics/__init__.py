from typing import Dict, Type

from weiser.evals.metrics.base import BaseEvalMetric
from weiser.evals.metrics.deterministic import (
    ExpectedViewRecallMetric,
    HitLimitMetric,
    ReferenceValueMatchMetric,
    SchemaMembershipMetric,
    StepEfficiencyMetric,
)
from weiser.loader.models import MetricConfig

METRIC_TYPE_MAP: Dict[str, Type[BaseEvalMetric]] = {
    "schema_membership": SchemaMembershipMetric,
    "expected_view_recall": ExpectedViewRecallMetric,
    "reference_value_match": ReferenceValueMatchMetric,
    "step_efficiency": StepEfficiencyMetric,
    "hit_limit": HitLimitMetric,
    # "llm_judge": LLMJudgeMetric,  # Phase 2
}


class MetricFactory:
    @staticmethod
    def create(config: MetricConfig) -> BaseEvalMetric:
        metric_class = METRIC_TYPE_MAP.get(config.type, None)
        if not metric_class:
            raise Exception(f"Metric type {config.type} not implemented yet")
        return metric_class(config)
