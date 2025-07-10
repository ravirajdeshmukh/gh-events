import os
import json
import time
import pandas as pd
from datetime import datetime
from utils.file_ops import *
from utils.event_utils import *
from utils.defaults import *


BRONZE_DIR = "bronze"
SILVER_DIR = "silver"
for event in INTERESTED_TYPES:
    ensure_directory_exists(os.path.join(BASE_STORAGE_PATH, SILVER_DIR, event))

event_field_config = load_yaml_file(os.path.join(BASE_PATH ,CONFIG_FOLDER,"filtered_events.yaml"))


def convert_json_to_parquet():
    bronze_dir_path= os.path.join(BASE_STORAGE_PATH, BRONZE_DIR)
    silver_dir_path= os.path.join(BASE_STORAGE_PATH, SILVER_DIR)
    for eventtype_folder in list_dir(bronze_dir_path):
        for filename in list_dir(os.path.join(bronze_dir_path,eventtype_folder)):
            json_path = os.path.join(bronze_dir_path,eventtype_folder,filename)
            parquet_path = os.path.join(silver_dir_path,eventtype_folder,filename.replace(".json", ".parquet"))

            if parquet_exists(parquet_path):
                print(f"[SKIP] {filename} already processed.")
                continue

            try:
                raw_events = load_json_file(json_path)        
                transformed_events_df = transform_events(raw_events)
                write_parquet(transformed_events_df, parquet_path)
                print(f"[SUCCESS] Wrote: {parquet_path}")
            except Exception as e:
                print(f"[ERROR] Failed to process {filename}: {e}")

def transform_events(raw_events):
    """Transform raw events into a DataFrame with selected fields."""
    trimmed_events = []
    for event in raw_events:
        event_type = event.get("type")
        if not event_type or event_type not in event_field_config:
            continue
        try:
            fields = event_field_config[event_type]["fields"]
            trimmed = trim_event_dynamic(event, fields)
            trimmed_events.append(trimmed)
        except Exception as e:
            print(f"[WARN] Skipping event due to config error: {e}")

    df = pd.DataFrame(trimmed_events)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    return df

if __name__ == "__main__":
    while True:
        convert_json_to_parquet()

        time.sleep(10)
