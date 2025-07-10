
from fastapi import FastAPI, Query, Request
from utils.metrics import evaluate_kpi

app = FastAPI()

@app.get("/kpi/{kpi_id}")
def fetch_kpi(kpi_id: str, request: Request):
    query_params=dict(request.query_params)
    try:
        result = evaluate_kpi(kpi_id, query_params)
        return result
    except Exception as e:
        return {"error": str(e)}
