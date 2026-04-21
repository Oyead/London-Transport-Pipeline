import polars as pl
import duckdb
from pipeline.ingest import ingest


def stage(df: pl.DataFrame, con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """
    Parse dates, cast types, flag anomalies, deduplicate.
    Write raw-but-typed data to stg_journeys.
    No business logic or aggregations here.
    """
    df = (
        df.with_columns([
            pl.col("period_start").str.to_date(format="%d-%b-%y"),
            pl.col("period_end").str.to_date(format="%d-%b-%y"),
        ])
        .filter(pl.col("days_in_period") >= 28)
        .filter(pl.col("bus_m") > 0)
        .with_columns(
            pl.col("period_yr")
            .str.extract(r"(\d{2})/", 1)
            .cast(pl.Int32)
            .add(2000)
            .alias("period_yr")
        )
        .with_columns(
            is_anomaly=pl.when(
                (pl.col("period_start") >= pl.date(2020, 3, 1)) &
                (pl.col("period_start") <= pl.date(2021, 5, 1))
            ).then(True).otherwise(False)
        )
        .unique(subset=["period_start"], keep="first")
    )

    # Quality checks before writing
    assert df["period_yr"].min() >= 2000, "Invalid year — century assumption broken"
    assert df["period_yr"].max() <= 2026, "Future year detected"
    assert df.filter(pl.col("period_start").is_duplicated()).is_empty(), \
        "Duplicate period_start rows remain after dedup"

    con.execute("CREATE OR REPLACE TABLE stg_journeys AS SELECT * FROM df")
    print(f"[staging] stg_journeys written — {df.shape[0]} rows")
    return df


if __name__ == "__main__":
    import duckdb
    con = duckdb.connect("transport_data.db")
    df = ingest()
    stage(df, con)