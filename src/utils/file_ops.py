import os
import json
import pandas as pd
from datetime import datetime
import yaml
import pyarrow

def list_dir(directory):
    return [f for f in os.listdir(directory)]

def load_json_file(filepath):
    with open(filepath, "r") as f:
        return json.load(f)

def write_parquet(df, filepath):
    df.to_parquet(filepath, index=False, engine='pyarrow')

def parquet_exists(filepath):
    return os.path.exists(filepath)

def ensure_directory_exists(directory):
    os.makedirs(directory, exist_ok=True)

def trim_event(event):
    """Keep only necessary fields from the raw event."""
    return {
        "id": event.get("id"),
        "type": event.get("type"),
        "repo": event.get("repo", {}).get("name"),
        "created_at": event.get("created_at")
    }

def write_json_to_file(events, filepath):
    """Append events to a timestamped JSON file in the bronze layer."""
    ensure_directory_exists("/".join(str(filepath).split("/")[:-1]))

    with open(filepath, "w") as f:
        json.dump(events, f, indent=2)

    print(f"[INFO] Wrote {len(events)} events to {filepath}")
    return filepath

def load_yaml_file(filepath):
    with open(filepath, "r") as f:
        return yaml.safe_load(f)