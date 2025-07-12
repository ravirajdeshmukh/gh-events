import os
import json
import time
import argparse
import pandas as pd
from datetime import datetime
from utils.file_ops import *
from utils.event_utils import *
from utils.defaults import *

# Define data layer folder names
BRONZE_DIR = "bronze"
SILVER_DIR = "silver"

# Ensure silver directories for each event type exist
for event in INTERESTED_TYPES:
    ensure_directory_exists(os.path.join(BASE_STORAGE_PATH, SILVER_DIR, event))

# Load YAML config that specifies which fields to extract from each event type
event_field_config = load_yaml_file(os.path.join(BASE_PATH, CONFIG_FOLDER, "filtered_events.yaml"))

def convert_json_to_parquet():
    """
    Converts raw JSON files from the bronze layer into trimmed Parquet files in the silver layer.

    - Checks each event type's bronze folder
    - Avoids reprocessing files that already exist in silver
    - Applies a YAML-driven schema transformation
    """
    bronze_dir_path = os.path.join(BASE_STORAGE_PATH, BRONZE_DIR)
    silver_dir_path = os.path.join(BASE_STORAGE_PATH, SILVER_DIR)

    for eventtype_folder in list_dir(bronze_dir_path):
        for filename in list_dir(os.path.join(bronze_dir_path, eventtype_folder)):
            json_path = os.path.join(bronze_dir_path, eventtype_folder, filename)
            parquet_path = os.path.join(silver_dir_path, eventtype_folder, filename.replace(".json", ".parquet"))

            # Skip if this file has already been transformed
            if parquet_exists(parquet_path):
                # print(f"[SKIP] {filename} already processed.")
                continue

            try:
                # Load raw events
                raw_events = load_json_file(json_path)
                
                # Transform using dynamic schema
                transformed_events_df = transform_events(raw_events)
                
                # Write to Parquet
                write_parquet(transformed_events_df, parquet_path)
                print(f"[SUCCESS] Wrote: {parquet_path}")
            except Exception as e:
                print(f"[ERROR] Failed to process {filename}: {e}")

def transform_events(raw_events):
    """
    Applies schema-based field extraction to a list of raw GitHub events.

    Args:
        raw_events (List[dict]): Raw GitHub events loaded from a JSON file.

    Returns:
        pd.DataFrame: A DataFrame of trimmed and typed events.
    """
    trimmed_events = []

    for event in raw_events:
        event_type = event.get("type")
        
        # Skip events not configured
        if not event_type or event_type not in event_field_config:
            continue

        try:
            # Get field paths from YAML config
            fields = event_field_config[event_type]["fields"]
            
            # Dynamically extract only configured fields
            trimmed = trim_event_dynamic(event, fields)
            trimmed_events.append(trimmed)
        except Exception as e:
            print(f"[WARN] Skipping event due to config error: {e}")
    
    # Create DataFrame and ensure datetime type
    df = pd.DataFrame(trimmed_events)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    return df


if __name__ == "__main__":
    # CLI: Allow batch or continuous transformation mode
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--live",
        action="store_true",
        help="Live mode to keep transforming the data in the bronze layer."
    )
    args = parser.parse_args()

    if args.live:
        while True:
            convert_json_to_parquet()
            time.sleep(10)
    else:
        convert_json_to_parquet()