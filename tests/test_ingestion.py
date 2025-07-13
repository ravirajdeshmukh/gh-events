import os
import json
from collections import deque
from utils.file_ops import write_json_to_file

def test_write_json_creates_file(tmp_path):
    buffer = deque([
        {"id": "001", "type": "WatchEvent", "created_at": "2025-07-10T14:00:00Z"},
        {"id": "200", "type": "WatchEvent", "created_at": "2025-07-12T17:00:00Z"},
    ])
    
    filepath = write_json_to_file(list(buffer), filepath=os.path.join(tmp_path, "bronze","WatchEvent", "events.json"))
    assert os.path.exists(filepath)

    with open(filepath) as f:
        data = json.load(f)
        assert len(data) == 2
        assert data[0]["id"] == "001"
        assert data[1]["id"] == "200"