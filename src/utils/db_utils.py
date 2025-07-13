import duckdb

# Path to the DuckDB database file
DUCKDB_PATH = "data/db/github_events.duckdb"

def run_query(sql: str):
    """
    Executes a given SQL query against the DuckDB database and returns the result as a DataFrame.

    Args:
        sql (str): The SQL query to execute.

    Returns:
        pandas.DataFrame: Query results returned as a DataFrame.
    """
    # Establish connection and execute query
    with duckdb.connect(DUCKDB_PATH) as con:
        result = con.execute(sql).fetchdf()
    return result
