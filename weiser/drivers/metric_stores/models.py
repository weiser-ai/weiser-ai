from datetime import datetime
from typing import List, Optional
from sqlmodel import SQLModel, Field, Column, ARRAY, Float


class MetricRecord(SQLModel, table=True):
    __tablename__ = "metrics"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    actual_value: Optional[float] = Field(default=None)
    check_id: Optional[str] = Field(default=None)
    condition: Optional[str] = Field(default=None)
    dataset: Optional[str] = Field(default=None)
    datasource: Optional[str] = Field(default=None)
    fail: Optional[bool] = Field(default=None)
    name: Optional[str] = Field(default=None)
    run_id: Optional[str] = Field(default=None)
    run_time: Optional[datetime] = Field(default=None)
    sql: Optional[str] = Field(default=None)
    success: Optional[bool] = Field(default=None)
    threshold: Optional[str] = Field(default=None)
    threshold_list: Optional[List[float]] = Field(default=None, sa_column=Column(ARRAY(Float)))
    type: Optional[str] = Field(default=None)
    tenant_id: Optional[int] = Field(default=1)