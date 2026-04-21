import duckdb
from pipeline.ingest import ingest
from pipeline.staging import stage
from pipeline.transform import transform
from pipeline.mart import mart
from pipeline.export import export_to_bigquery
def run_pipeline():
    con = duckdb.connect("transport_data.db")
    df = ingest()
    stage(df, con)
    transform(con)
    mart(con)
    export_to_bigquery(con)
    print("Pipeline complete")

if __name__ == "__main__":
    run_pipeline()