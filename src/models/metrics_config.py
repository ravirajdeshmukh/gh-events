from pydantic import BaseModel, field_validator
from typing import Literal, List, Optional

class Metric(BaseModel):
    id: str
    name: str
    sql: str
    visualisation: Optional[Literal["bar", "line", "value", "table"]] = None

    @field_validator('sql')
    def sql_must_not_be_empty(cls, v):
        if not v.strip():
            raise ValueError("SQL query must not be empty")
        return v

class MetricConfig(BaseModel):
    kpis: List[Metric]