from abc import ABC, abstractmethod
from typing import Optional

from weiser.evals.models import CriterionScore, EvalTestCase
from weiser.loader.models import MetricConfig


class BaseEvalMetric(ABC):
    """Mirrors DeepEval's BaseMetric contract: measure()/a_measure() set score/reason,
    is_successful() compares score against threshold. Deterministic metrics only need
    to implement measure(); the async default just calls it synchronously."""

    def __init__(self, config: MetricConfig) -> None:
        self.config = config
        self.threshold = config.threshold
        self.score: Optional[float] = None
        self.success: Optional[bool] = None
        self.reason: Optional[str] = None

    @property
    def name(self) -> str:
        return self.config.name or self.config.type

    @abstractmethod
    def measure(self, test_case: EvalTestCase) -> CriterionScore: ...

    async def a_measure(self, test_case: EvalTestCase) -> CriterionScore:
        return self.measure(test_case)

    def is_successful(self) -> bool:
        if self.score is None:
            self.success = False
        else:
            self.success = self.score >= self.threshold
        return self.success
