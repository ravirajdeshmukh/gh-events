import duckdb
import os
import argparse
import time
from utils.defaults import *

# Constants
SILVER_DIR = "silver"
DUCKDB_PATH = "data/db/github_events.duckdb"

def create_duckdb_database():
    """
    Materializes cleaned Parquet files (Silver layer) into DuckDB tables.

    - Iterates over event type folders inside the Silver directory.
    - For each event type, reads matching Parquet files.
    - Creates or replaces a table in DuckDB using the contents.
    """
    con = duckdb.connect(DUCKDB_PATH)

    silver_dir_path = os.path.join(BASE_STORAGE_PATH, SILVER_DIR)

    for event_type in os.listdir(silver_dir_path):
        event_path = os.path.join(silver_dir_path, event_type)

        if os.path.isdir(event_path):
            # Match all Parquet files for the event type
            parquet_glob = os.path.join(event_path, f"{event_type}*.parquet")
            table_name = event_type.lower()

            # Replace or create a table in DuckDB
            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_parquet('{parquet_glob}')
            """)
            print(f"[INFO] Table created/updated: {table_name}")

    con.close()

# CLI entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--live", action="store_true", help="Live mode to continuously update DuckDB from Silver layer"
    )
    args = parser.parse_args()

    if args.live:
        # Keep updating the database every 10 seconds
        while True:
            create_duckdb_database()
            time.sleep(10)
    else:
        # Run once and exit
        create_duckdb_database()

    print("[INFO] DuckDB database materialization complete.")
