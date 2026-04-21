# London Transport Analysis Pipeline

An ELT pipeline built on TfL journey data covering 2010–2024.

## Stack
- Python, Polars, DuckDB, Google BigQuery

## Pipeline architecture
CSV → ingest → staging → transform → mart → BigQuery

## Layers
- `stg_journeys` — raw typed data
- `int_journeys` — cleaned with derived columns (total_m, modal share %)
- `mart_journeys_yearly` — yearly aggregations ready for analysis

## Key decisions
- Nulls kept for modes that didn't exist yet (Cable Car 2012, TfL Rail 2017)
- COVID anomaly periods flagged (March 2020 – May 2021)
- Data quality assertions run before each layer write

## Setup
```bash
pip install polars duckdb google-cloud-bigquery
gcloud auth application-default login
python run_pipeline.py
```

## Project structure
```
├── run_pipeline.py
├── pipeline/
│   ├── ingest.py
│   ├── staging.py
│   ├── transform.py
│   ├── mart.py
│   └── export.py
└── data/
    └── tfl-journeys-type.csv
```
