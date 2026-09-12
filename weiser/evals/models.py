from datetime import datetime
from typing import Any, List, Literal, Optional

from pydantic import BaseModel

from weiser.loader.models import EvalGolden
from weiser.evals.semantic_layer.base import SchemaCatalog


class ToolCall(BaseModel):
    tool_name: str
    args: dict
    result: Optional[Any] = None
    turn: int


class WidgetSummary(BaseModel):
    """Chart/visualization metadata for one widget a BI agent produced, for
    chart-selection-quality judging (`chart_type_appropriateness`). Optional: agents
    that only answer in text (no charting step) leave `AgentTrace.widgets` empty."""

    chart_type: str
    columns: List[str] = []
    row_count: int = 0
    title: Optional[str] = None


class AgentTrace(BaseModel):
    question: str
    tool_calls: List[ToolCall] = []
    predicted_sqls: List[str] = []
    final_answer: Optional[str] = None
    query_results: Optional[List[dict]] = None
    widgets: List[WidgetSummary] = []
    is_dashboard_turn: bool = False
    hit_limit: bool = False
    elapsed_s: float = 0.0
    cost_usd: float = 0.0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    error: Optional[str] = None


class CriterionScore(BaseModel):
    criterion: str
    score: Optional[float] = None
    applicable: bool = True
    rationale: Optional[str] = None
    judge_prompt_version: Optional[str] = None


class DQContext(BaseModel):
    touched_datasets: List[str] = []
    dq_results: List[dict] = []
    has_failing_dq: bool = False
    has_stale_dq: bool = False


class EvalTestCase(BaseModel):
    golden: EvalGolden
    arm: str
    trace: AgentTrace
    schema_catalog: Optional[SchemaCatalog] = None


FailureAttribution = Literal["clean", "agent", "data_quality", "judge_uncertain"]


class EvalResultRow(BaseModel):
    golden_id: str
    suite: str
    arm: str
    rep: int = 0
    level: Literal["easy", "hard"] = "easy"
    split: Literal["train", "held_out"] = "train"
    criteria: List[CriterionScore] = []
    dq_context: DQContext
    failure_attribution: FailureAttribution
    overall_score: float
    run_id: str
    run_time: datetime
