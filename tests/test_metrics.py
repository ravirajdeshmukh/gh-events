from utils.metrics import evaluate_kpi

def test_evaluate_event_count_offset():
    result = evaluate_kpi("event_count_offset", params={"offset": 15})
    assert "data" in result
    assert "visualisation" in result
    assert result["visualisation"] != "bar"

    assert isinstance(result["data"], list)
    if result["data"]:
        assert "event_type" in result["data"][0]
        assert "count" in result["data"][0]