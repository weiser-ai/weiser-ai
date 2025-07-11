"""
DuckDB-compatible SQLModel models for metric storage.

This version addresses DuckDB-specific constraints:
- No SERIAL type support (uses INTEGER with manual ID management)
- Array types handled differently
- Simplified schema for better compatibility
"""

from datetime import datetime
from typing import List, Optional
from sqlmodel import SQLModel, Field


class MetricRecordDuckDB(SQLModel, table=True):
    """
    DuckDB-compatible metric record model.
    
    Key differences from PostgreSQL version:
    - No auto-incrementing ID (DuckDB doesn't support SERIAL)
    - Simplified array handling for threshold_list
    - All fields optional for flexibility
    """
    
    __tablename__ = "metrics_duckdb"
    
    # Manual ID management since DuckDB doesn't support SERIAL
    id: Optional[int] = Field(
        default=None, 
        primary_key=True,
        sa_column_kwargs={"autoincrement": False}
    )
    
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
    
    # DuckDB supports arrays, but we'll keep it simple for now
    # Could be enhanced later with proper array column support
    threshold_list: Optional[str] = Field(default=None)  # JSON string representation
    
    type: Optional[str] = Field(default=None)
    
    @classmethod
    def from_dict(cls, record: dict) -> "MetricRecordDuckDB":
        """Create a MetricRecordDuckDB from a dictionary."""
        import json
        import hashlib
        
        # Handle threshold list conversion
        threshold_list = None
        threshold = record.get("threshold")
        
        if isinstance(threshold, (list, tuple)):
            threshold_list = json.dumps(threshold)
            threshold = None
        else:
            threshold_list = record.get("threshold_list")
            if isinstance(threshold_list, (list, tuple)):
                threshold_list = json.dumps(threshold_list)
        
        # Generate a simple hash-based ID for DuckDB
        # This ensures uniqueness without relying on auto-increment
        id_string = f"{record.get('check_id', '')}{record.get('run_id', '')}{record.get('run_time', '')}"
        record_id = abs(hash(id_string)) % (2**31)  # Keep it within integer range
        
        return cls(
            id=record_id,
            actual_value=record.get("actual_value"),
            check_id=record.get("check_id"),
            condition=record.get("condition"),
            dataset=record.get("dataset"),
            datasource=record.get("datasource"),
            fail=record.get("fail"),
            name=record.get("name"),
            run_id=record.get("run_id"),
            run_time=record.get("run_time"),
            sql=record.get("measure"),  # Note: original uses 'measure' key for SQL
            success=record.get("success"),
            threshold=threshold,
            threshold_list=threshold_list,
            type=record.get("type"),
        )
    
    def get_threshold_list(self) -> Optional[List[float]]:
        """Get threshold_list as a Python list."""
        import json
        
        if self.threshold_list:
            try:
                return json.loads(self.threshold_list)
            except json.JSONDecodeError:
                return None
        return None


class MetricRecordDuckDBV2(SQLModel, table=True):
    """
    Alternative DuckDB model with proper array support.
    
    This version attempts to use DuckDB's native array support
    but may require additional configuration.
    """
    
    __tablename__ = "metrics_v2"
    
    id: Optional[int] = Field(
        default=None, 
        primary_key=True,
        sa_column_kwargs={"autoincrement": False}
    )
    
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
    
    # Attempt to use DuckDB's native array support
    # This might require custom column definition
    threshold_list: Optional[str] = Field(default=None)  # Could be enhanced with ARRAY type
    
    type: Optional[str] = Field(default=None)