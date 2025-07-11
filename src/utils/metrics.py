# src/db/metrics.py

import yaml
import os
from pathlib import Path
from pydantic import ValidationError
from utils.db_utils import run_query
from models.metrics_config import MetricConfig
from utils.defaults import *

CONFIG_PATH = Path(__file__).parent.parent / "config" / "metrics.yaml"

def load_kpi_config():
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
    all_kpis = load_kpi_config()
    return next((kpi for kpi in all_kpis if kpi.id == kpi_id), None)

def evaluate_kpi(kpi_id, params:dict=None):
    params = params or {}
    kpi = get_kpi_by_id(kpi_id)
    if not kpi:
        raise ValueError(f"KPI '{kpi_id}' not found.")

    sql = kpi.sql
    for key, val in params.items():
        sql = sql.replace(f"${{{key}}}", str(val))

    df = run_query(sql)
    return {
        "id": kpi.id,
        "name": kpi.name,
        "visualisation": kpi.visualisation,
        "data": df.to_dict(orient="records")
    }
