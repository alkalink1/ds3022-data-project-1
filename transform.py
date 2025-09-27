# transform.py
import duckdb
import logging
from textwrap import dedent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="transform.log"
)
logger = logging.getLogger(__name__)

DB_PATH = "emissions.duckdb"
SECONDS_PER_HOUR = 3600.0

PAIRS = [
    ("yellow_trips_2024_clean", "yellow_trips_2024_transformed", "yellow"),
    ("green_trips_2024_clean",  "green_trips_2024_transformed",  "green"),
]

def table_exists(con, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?;",
        [name]
    ).fetchone()
    return row is not None

def get_emissions_cols(con):
    return {
        r[0].lower()
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'vehicle_emissions';"
        ).fetchall()
    }

def build_emissions_cte(con, taxi_type: str) -> str:
    cols = get_emissions_cols(con)
    if "co2_grams_per_mile" not in cols:
        raise RuntimeError("vehicle_emissions must have column 'co2_grams_per_mile'.")

    if "taxi_type" in cols:
        return dedent(f"""
            ve AS (
                SELECT co2_grams_per_mile
                FROM vehicle_emissions
                WHERE lower(taxi_type) = '{taxi_type}'
                LIMIT 1
            )
        """).strip()
    else:
        # Single-row or default factor table
        return dedent("""
            ve AS (
                SELECT co2_grams_per_mile
                FROM vehicle_emissions
                LIMIT 1
            )
        """).strip()

def transform_one(con, src: str, dst: str, taxi_type: str):
    logger.info(f"Transforming {src} -> {dst} (taxi_type={taxi_type})")

    # Ensure prerequisites
    if not table_exists(con, "vehicle_emissions"):
        raise RuntimeError("vehicle_emissions lookup table not found. Run load.py first.")
    if not table_exists(con, src):
        raise RuntimeError(f"Source cleaned table not found: {src}. Run clean.py first.")

    con.execute(f"DROP TABLE IF EXISTS {dst};")

    emissions_cte = build_emissions_cte(con, taxi_type)

    # Compose the transform query
    # Note: we project each column explicitly to avoid carrying helper columns forward.
    sql = f"""
        WITH
        base AS (
            SELECT
                cab_type,
                vendor_id,
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                trip_distance,
                date_diff('second', pickup_datetime, dropoff_datetime)::DOUBLE AS duration_seconds
            FROM {src}
        ),
        {emissions_cte}
        SELECT
            b.cab_type,
            b.vendor_id,
            b.pickup_datetime,
            b.dropoff_datetime,
            b.passenger_count,
            b.trip_distance,
            (b.trip_distance * ve.co2_grams_per_mile) / 1000.0 AS trip_co2_kgs,
            CASE
                WHEN b.duration_seconds > 0
                THEN b.trip_distance / (b.duration_seconds / {SECONDS_PER_HOUR})
                ELSE NULL
            END AS avg_mph,
            EXTRACT('hour' FROM b.pickup_datetime)::INTEGER  AS hour_of_day,
            EXTRACT('dow'  FROM b.pickup_datetime)::INTEGER  AS day_of_week,  -- Sun=0 .. Sat=6
            EXTRACT('week' FROM b.pickup_datetime)::INTEGER  AS week_of_year,
            EXTRACT('month'FROM b.pickup_datetime)::INTEGER  AS month_of_year
        FROM base b
        CROSS JOIN ve
    """
    con.execute(f"CREATE TABLE {dst} AS {sql};")

    # Verify columns exist
    cols = {
        r[0].lower()
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?;",
            [dst]
        ).fetchall()
    }
    expected = {
        "cab_type","vendor_id","pickup_datetime","dropoff_datetime",
        "passenger_count","trip_distance",
        "trip_co2_kgs","avg_mph","hour_of_day","day_of_week","week_of_year","month_of_year"
    }
    missing = expected - cols
    if missing:
        raise RuntimeError(f"{dst} missing expected columns: {missing}")

    # Print a small summary and a few sample rows
    count = con.execute(f"SELECT COUNT(*) FROM {dst};").fetchone()[0]
    ex = con.execute(
        f"""
        SELECT cab_type, trip_distance, trip_co2_kgs, avg_mph,
               hour_of_day, day_of_week, week_of_year, month_of_year
        FROM {dst} LIMIT 3;
        """
    ).fetchall()
    print(dedent(f"""
        [{dst}] rows: {count:,}
        Sample rows:
          {ex}
    """).rstrip())

def main():
    try:
        con = duckdb.connect(DB_PATH, read_only=False)
        logger.info("Connected to DuckDB")

        any_done = False
        for src, dst, taxi in PAIRS:
            if table_exists(con, src):
                transform_one(con, src, dst, taxi)
                any_done = True
            else:
                print(f"[WARN] '{src}' not found. Skipping.")

        if not any_done:
            print("No cleaned tables found. Run clean.py first.")
            return

        print("\n=== TRANSFORM COMPLETE ===")
        print("Created tables (if inputs existed): yellow_trips_2024_transformed, green_trips_2024_transformed")

    except Exception as e:
        logger.exception("Transform error")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
