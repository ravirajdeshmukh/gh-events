# src/db/metrics.py

import yaml
import os
from pathlib import Path
from pydantic import ValidationError
from utils.db_utils import run_query
from models.metrics_config import MetricConfig
from utils.defaults import *

# Define the path to the metrics configuration YAML file
CONFIG_PATH = Path(__file__).parent.parent / "config" / "metrics.yaml"

def load_kpi_config():
    """
    Loads and validates the KPI configuration from the metrics.yaml file.

    Returns:
        List[Metric]: A list of validated Metric objects.

    Raises:
        ValidationError: If the config does not conform to the MetricConfig schema.
    """
    with open(CONFIG_PATH, "r") as f:
        raw_config = yaml.safe_load(f)

    try:
        config = MetricConfig(**raw_config)
        return config.kpis
    except ValidationError as e:
        print("[-] KPI config validation failed:")
        print(e)
        raise e


def get_kpi_by_id(kpi_id):
    """
    Fetches a KPI definition by its ID.

    Args:
        kpi_id (str): The ID of the KPI to retrieve.

    Returns:
        Metric or None: The corresponding Metric object or None if not found.
    """
    all_kpis = load_kpi_config()
    return next((kpi for kpi in all_kpis if kpi.id == kpi_id), None)


def evaluate_kpi(kpi_id, params: dict = None):
    """
    Evaluates a KPI by substituting parameters into its SQL query and executing it.

    Args:
        kpi_id (str): The ID of the KPI to evaluate.
        params (dict, optional): Dictionary of parameter values to inject into the SQL.

    Returns:
        dict: A dictionary containing the KPI metadata and query results.

    Raises:
        ValueError: If the KPI is not found or SQL execution fails.
    """
    params = params or {}
    kpi = get_kpi_by_id(kpi_id)
    
    if not kpi:
        raise ValueError(f"KPI '{kpi_id}' not found.")

    # Replace parameter placeholders in the SQL query
    sql = kpi.sql
    for key, val in params.items():
        sql = sql.replace(f"${{{key}}}", str(val))

    # Run the query against DuckDB
    df = run_query(sql)

    return {
        "id": kpi.id,
        "name": kpi.name,
        "visualisation": kpi.visualisation,
        "data": df.to_dict(orient="records")
    }
