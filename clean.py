import duckdb
import logging
from textwrap import dedent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="clean.log"
)
logger = logging.getLogger(__name__)

DB_PATH = "emissions.duckdb"
MAX_TRIP_SECONDS = 86_400   # 1 day
MAX_TRIP_MILES = 100.0

SRC_TABLES = [
    ("yellow_trips_2024", "yellow_trips_2024_clean"),
    ("green_trips_2024",  "green_trips_2024_clean"),
]

def table_exists(con, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?;",
        [name]
    ).fetchone()
    return row is not None

def clean_one(con, src: str, dst: str):
    logger.info(f"Cleaning {src} -> {dst}")

    # Drop any previous cleaned table
    con.execute(f"DROP TABLE IF EXISTS {dst};")

    # Build cleaned table:
    #  - Compute duration_seconds
    #  - Remove duplicates (DISTINCT over selected columns)
    #  - Apply filters per assignment
    sql = f"""
    WITH base AS (
        SELECT
            cab_type,
            vendor_id,
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            date_diff('second', pickup_datetime, dropoff_datetime) AS duration_seconds
        FROM {src}
    ),
    filtered AS (
        SELECT
            cab_type,
            vendor_id,
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance
        FROM base
        WHERE
            (passenger_count IS NULL OR passenger_count <> 0)
            AND trip_distance > 0
            AND trip_distance <= {MAX_TRIP_MILES}
            AND duration_seconds <= {MAX_TRIP_SECONDS}
    )
    SELECT DISTINCT
        cab_type,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance
    FROM filtered
"""

    con.execute(f"CREATE TABLE {dst} AS {sql};")

def verify_clean(con, table: str):
    """Verify that the five conditions no longer exist."""
    total = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]

    # duplicates: count difference between total rows and DISTINCT rows over all columns
    distinct = con.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {table});").fetchone()[0]
    dupes = total - distinct

    zero_pass = con.execute(
        f"SELECT COUNT(*) FROM {table} WHERE passenger_count = 0;"
    ).fetchone()[0]

    zero_miles = con.execute(
        f"SELECT COUNT(*) FROM {table} WHERE trip_distance = 0;"
    ).fetchone()[0]

    over_100 = con.execute(
        f"SELECT COUNT(*) FROM {table} WHERE trip_distance > {MAX_TRIP_MILES};"
    ).fetchone()[0]

    over_day = con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT date_diff('second', pickup_datetime, dropoff_datetime) AS s
            FROM {table}
        ) WHERE s > {MAX_TRIP_SECONDS};
        """
    ).fetchone()[0]

    print(dedent(f"""
        === VERIFY: {table} ===
          Rows:                 {total:,}
          Duplicates:           {dupes}
          0 passengers:         {zero_pass}
          0 miles:              {zero_miles}
          >100 miles:           {over_100}
          >1 day duration:      {over_day}
    """).rstrip())

def summarize_before_after(con, src: str, dst: str):
    raw = con.execute(f"SELECT COUNT(*) FROM {src};").fetchone()[0]
    clean = con.execute(f"SELECT COUNT(*) FROM {dst};").fetchone()[0]
    print(f"[{src} -> {dst}] Raw: {raw:,}  |  Clean: {clean:,}  |  Removed: {raw - clean:,}")

def main():
    try:
        con = duckdb.connect(DB_PATH, read_only=False)
        logger.info("Connected to DuckDB")

        any_found = False
        for src, dst in SRC_TABLES:
            if table_exists(con, src):
                any_found = True
                clean_one(con, src, dst)
                summarize_before_after(con, src, dst)
                verify_clean(con, dst)
            else:
                print(f"[WARN] Source table '{src}' not found. Skipping.")

        if not any_found:
            print("No source tables found. Run load.py first.")
            return

        print("\nCleaning complete. Cleaned tables: "
              + ", ".join([dst for src, dst in SRC_TABLES if table_exists(con, dst)]))

    except Exception as e:
        logger.exception("Error during cleaning")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
