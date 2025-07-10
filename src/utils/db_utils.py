
import duckdb

DUCKDB_PATH = "data/db/github_events.duckdb"

def run_query(sql: str):
    with duckdb.connect(DUCKDB_PATH) as con:
        result = con.execute(sql).fetchdf()
    return result