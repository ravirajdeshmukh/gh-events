from fastapi import FastAPI, Query, Request
from utils.metrics import evaluate_kpi

# Initialize FastAPI app
app = FastAPI()

@app.get("/kpi/{kpi_id}")
def fetch_kpi(kpi_id: str, request: Request):
    """
    API endpoint to fetch a KPI value based on a configured metric.

    This endpoint dynamically serves KPIs defined in a YAML configuration file.
    It uses the provided KPI ID and any optional query parameters (like filters or time offsets)
    to compute and return the result.


    KPI: Average Time Between Pull Requests (kpi_id=avg_pr_time)
    URL: http://0.0.0.0:9000/kpi/avg_pr_time
    Calculates the average time (in minutes) between successive pull requests for each repository.


    KPI: Event Counts by Offset (kpi_id=event_count_offset)
    URL: http://0.0.0.0:9000/kpi/event_count_offset?offset=500
    Returns the total number of GitHub events (Watch, Issues, PR) that occurred in the last N minutes.
    Accepts a query parameter `offset` (in minutes).


    Args:
        kpi_id (str): The unique ID of the KPI defined in metrics.yaml.
        request (Request): FastAPI's Request object, used to capture query params.

    Returns:
        dict: The result of the evaluated KPI, or an error message if evaluation fails.
    """
    query_params = dict(request.query_params)  # Capture URL query parameters

    try:
        # Delegate KPI evaluation to a utility function
        result = evaluate_kpi(kpi_id, query_params)
        return result
    except Exception as e:
        # Handle and return any errors gracefully
        return {"error": str(e)}