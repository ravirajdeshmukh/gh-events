import os
import json
import pandas as pd
from datetime import datetime
import yaml
import pyarrow  # Required for Parquet support

def list_dir(directory):
    """
    Lists non-hidden files in a directory.

    Args:
        directory (str): Path to the directory.

    Returns:
        List[str]: List of visible file names.
    """
    return [f for f in os.listdir(directory) if not f.startswith('.')]


def load_json_file(filepath):
    """
    Loads and parses a JSON file.

    Args:
        filepath (str): Path to the JSON file.

    Returns:
        dict or list: Parsed JSON content.
    """
    with open(filepath, "r") as f:
        return json.load(f)


def write_parquet(df, filepath):
    """
    Writes a Pandas DataFrame to a Parquet file using PyArrow engine.

    Args:
        df (pd.DataFrame): Data to write.
        filepath (str): Destination Parquet file path.
    """
    df.to_parquet(filepath, index=False, engine='pyarrow')


def parquet_exists(filepath):
    """
    Checks if a Parquet file exists.

    Args:
        filepath (str): Path to check.

    Returns:
        bool: True if file exists, False otherwise.
    """
    return os.path.exists(filepath)


def ensure_directory_exists(directory):
    """
    Creates a directory if it doesn't exist.

    Args:
        directory (str): Directory path.
    """
    os.makedirs(directory, exist_ok=True)


def trim_event(event):
    """
    Extracts a minimal set of fields from a raw GitHub event.

    Args:
        event (dict): Full GitHub event.

    Returns:
        dict: Trimmed event with selected fields only.
    """
    return {
        "id": event.get("id"),
        "type": event.get("type"),
        "repo": event.get("repo", {}).get("name"),
        "created_at": event.get("created_at")
    }


def write_json_to_file(events, filepath):
    """
    Writes a list of event dictionaries to a JSON file.

    Args:
        events (list): List of event dictionaries.
        filepath (str): Destination file path.

    Returns:
        str: Path of the written file.
    """
    # Ensure the target directory exists
    ensure_directory_exists("/".join(str(filepath).split("/")[:-1]))

    # Write events to file
    with open(filepath, "w") as f:
        json.dump(events, f, indent=2)

    print(f"[INFO] Wrote {len(events)} events to {filepath}")
    return filepath


def load_yaml_file(filepath):
    """
    Loads a YAML file safely.

    Args:
        filepath (str): Path to YAML file.

    Returns:
        dict: Parsed YAML content.
    """
    with open(filepath, "r") as f:
        return yaml.safe_load(f)
