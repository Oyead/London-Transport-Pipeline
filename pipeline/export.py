from google.cloud import bigquery
from pathlib import Path
import duckdb
import os

BASE_DIR = Path(__file__).resolve().parent.parent

def export_to_bigquery(con: duckdb.DuckDBPyConnection) -> None:    
    client = bigquery.Client(project="london-transport-494015")

    tables = {
        "tfl_pipeline.stg_journeys":          "SELECT * FROM stg_journeys",
        "tfl_pipeline.int_journeys":          "SELECT * FROM int_journeys",
        "tfl_pipeline.mart_journeys_yearly":  "SELECT * FROM mart_journeys_yearly",
    }

    for table_id, query in tables.items():
        df = con.execute(query).df()
        job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        )
        job.result()
        print(f"[export] {table_id} — {len(df)} rows loaded")