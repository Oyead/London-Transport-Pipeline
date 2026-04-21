import duckdb


def transform(con: duckdb.DuckDBPyConnection) -> None:
    """
    Build int_journeys from stg_journeys.
    Adds derived columns: total_m, bus_share, tube_share.
    Keeps nulls for modes that didn't exist yet — no fill_null(0) here.
    """
    con.execute("""
        CREATE OR REPLACE TABLE int_journeys AS
        SELECT
            period_yr,
            reporting_period,
            days_in_period,
            period_start,
            period_end,
            is_anomaly,

            bus_m,
            tube_m,
            dlr_m,
            tram_m,
            overground_m,
            cable_car_m,
            tfl_rail_m,

            COALESCE(bus_m, 0)
                + COALESCE(tube_m, 0)
                + COALESCE(dlr_m, 0)
                + COALESCE(tram_m, 0)
                + COALESCE(overground_m, 0)
                + COALESCE(cable_car_m, 0)
                + COALESCE(tfl_rail_m, 0)
            AS total_m,

            ROUND(100.0 * bus_m  / NULLIF(total_m, 0), 2) AS bus_share_pct,
            ROUND(100.0 * tube_m / NULLIF(total_m, 0), 2) AS tube_share_pct

        FROM stg_journeys
    """)

    count = con.execute("SELECT COUNT(*) FROM int_journeys").fetchone()[0]
    print(f"[transform] int_journeys written — {count} rows")


if __name__ == "__main__":
    con = duckdb.connect("transport_data.db")
    transform(con)
    print(con.execute("SELECT * FROM int_journeys LIMIT 3").df())