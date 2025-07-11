import duckdb
import os
import time
from utils.defaults import *

SILVER_DIR = "silver"
DUCKDB_PATH = "data/db/github_events.duckdb"

def create_duckdb_database():
    con = duckdb.connect(DUCKDB_PATH)
    silver_dir_path = os.path.join(BASE_STORAGE_PATH, SILVER_DIR)
    for event_type in os.listdir( silver_dir_path):
        event_path = os.path.join(silver_dir_path, event_type)
        if os.path.isdir(event_path):
            parquet_glob = os.path.join(event_path, f"{event_type}*.parquet")
            table_name = event_type.lower()

            # Register all Parquet files into DuckDB
            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_parquet('{parquet_glob}')
            """)
            print(f"[-] Table created: {table_name}")

    con.close()

if __name__ == "__main__":
    # while True:
        create_duckdb_database()
        # time.sleep(60)
        print("[INFO] DuckDB database created successfully.")