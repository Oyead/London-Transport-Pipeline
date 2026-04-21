import duckdb
import os


def mart(con: duckdb.DuckDBPyConnection, export_path: str = "outputs") -> None:
    """
    Build mart_journeys_yearly from int_journeys.
    Final aggregated layer — ready for analysis or cloud upload.
    Exports to Parquet.
    """
    os.makedirs(export_path, exist_ok=True)

    con.execute("""
        CREATE OR REPLACE TABLE mart_journeys_yearly AS
        SELECT
            period_yr,
            SUM(bus_m)         AS total_bus_m,
            SUM(tube_m)        AS total_tube_m,
            SUM(dlr_m)         AS total_dlr_m,
            SUM(tram_m)        AS total_tram_m,
            SUM(overground_m)  AS total_overground_m,
            SUM(cable_car_m)   AS total_cable_car_m,
            SUM(tfl_rail_m)    AS total_tfl_rail_m,
            SUM(total_m)       AS total_all_m,

            ROUND(AVG(bus_share_pct), 2)  AS avg_bus_share_pct,
            ROUND(AVG(tube_share_pct), 2) AS avg_tube_share_pct,

            MAX(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS has_anomaly_period

        FROM int_journeys
        GROUP BY period_yr
        ORDER BY period_yr ASC
    """)

    # Export mart to Parquet
    con.execute(f"""
        COPY mart_journeys_yearly 
        TO '{export_path}/mart_journeys_yearly.parquet' 
        (FORMAT PARQUET)
    """)

    # Export intermediate layer too
    con.execute(f"""
        COPY int_journeys 
        TO '{export_path}/int_journeys.parquet' 
        (FORMAT PARQUET)
    """)

    result = con.execute("SELECT * FROM mart_journeys_yearly").df()
    print(f"[mart] mart_journeys_yearly written — {len(result)} rows")
    print(result)


if __name__ == "__main__":
    con = duckdb.connect("transport_data.db")
    mart(con)