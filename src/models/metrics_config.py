from pydantic import BaseModel, field_validator
from typing import Literal, List, Optional

class Metric(BaseModel):
    """
    Represents a single KPI metric definition.
    
    Attributes:
        id (str): Unique identifier for the metric.
        name (str): Human-readable name for the metric.
        sql (str): SQL query that defines how the metric is calculated.
        visualisation (Optional[str]): Type of visualization to be used
            (e.g., bar, line, value, or table).
    """
    id: str
    name: str
    sql: str
    visualisation: Optional[Literal["bar", "line", "value", "table"]] = None

    @field_validator('sql')
    def sql_must_not_be_empty(cls, v: str) -> str:
        """
        Validates that the SQL query string is not empty or just whitespace.

        Args:
            v (str): The SQL string to validate.

        Returns:
            str: The original SQL string if valid.

        Raises:
            ValueError: If the SQL string is empty or only whitespace.
        """
        if not v.strip():
            raise ValueError("SQL query must not be empty")
        return v

class MetricConfig(BaseModel):
    """
    Configuration wrapper for multiple KPI metrics.

    Attributes:
        kpis (List[Metric]): A list of defined Metric objects.
    """
    kpis: List[Metric]
