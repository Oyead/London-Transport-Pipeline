import polars as pl
from pathlib import Path
clean_map = {
    "Period and Financial year":    "period_yr",
    "Reporting Period":             "reporting_period",
    "Days in period":               "days_in_period",
    "Period beginning":             "period_start",
    "Period ending":               "period_end",
    "Bus journeys (m)":            "bus_m",
    "Underground journeys (m)":     "tube_m",
    "DLR Journeys (m)":            "dlr_m",
    "Tram Journeys (m)":           "tram_m",
    "Overground Journeys (m)":      "overground_m",
    "London Cable Car Journeys (m)": "cable_car_m",
    "TfL Rail Journeys (m)":       "tfl_rail_m",
}
BASE_DIR = Path(__file__).resolve().parent.parent
def ingest(path: str = BASE_DIR /"data"/ "tfl-journeys-type.csv") -> pl.DataFrame:
    """Load raw CSV and rename columns. No transformations here."""
    df = pl.read_csv(path)
    df = df.rename(clean_map)
    print(f"[ingest] Loaded {df.shape[0]} rows, {df.shape[1]} columns")
    return df


if __name__ == "__main__":
    df = ingest()
    print(df.head())