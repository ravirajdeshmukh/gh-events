import pandas as pd
import os
import json
from utils.file_ops import *
from utils.defaults import *
from utils.event_utils import *

def test_transform_and_write_parquet(tmp_path):
    event = {
        "id": "123",
        "type": "PullRequestEvent",
        "created_at": "2025-07-10T14:00:00Z",
        "repo": {"name": "octocat/hello"},
        "actor": {"login": "octocat"},
        "payload": {"pull_request": {"number": 1, "state": "open", "merged": False, "user": {"login": "octocat"}}}
    }

    config = load_yaml_file(os.path.join(BASE_PATH,CONFIG_FOLDER,"filtered_events.yaml"))
    fields = config["PullRequestEvent"]["fields"]
    trimmed = trim_event_dynamic(event, fields)

    df = pd.DataFrame([trimmed])
    out_path = tmp_path / "pr.parquet"
    write_parquet(df, out_path)

    assert os.path.exists(out_path)
    df2 = pd.read_parquet(out_path)
    assert "repo.name" in df2.columns