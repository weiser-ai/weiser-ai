from typing import Optional

from pydantic import BaseModel


class CrossSourceRowCountResult(BaseModel):
    success: bool
    postgres_row_count: Optional[int] = None
    snowflake_row_count: Optional[int] = None
    relative_diff: float
    weiser_run_id: str
    weiser_check_id: str
