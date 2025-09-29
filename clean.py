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

# Cleaning thresholds
MAX_TRIP_SECONDS = 86_400   # 1 day
MAX_TRIP_MILES = 100.0

# Years & cabs to look for (matches what load.py creates)
YEARS = range(2015, 2025)  # 2015..2024 inclusive
CABS = ("yellow", "green")


# Check if a table exists in DuckDB
def table_exists(con, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?;",
        [name]
    ).fetchone()
    return row is not None


# Return (src,dst) pairs for all existing per-year cab tables
def discover_src_tables(con):
    pairs = []
    for cab in CABS:
        for y in YEARS:
            src = f"{cab}_trips_{y}"
            if table_exists(con, src):
                dst = f"{src}_clean"
                pairs.append((src, dst))
            else:
                logger.info("Source table not found, skipping: %s", src)
    return pairs


# Build a cleaned table from src into dst
def clean_one(con, src: str, dst: str):
    logger.info("Cleaning %s -> %s", src, dst)
    con.execute(f"DROP TABLE IF EXISTS {dst};")

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


# Verify the cleaned table meets constraints
def verify_clean(con, table: str):
    total = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
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


# Print row counts before/after cleaning
def summarize_before_after(con, src: str, dst: str):
    raw = con.execute(f"SELECT COUNT(*) FROM {src};").fetchone()[0]
    clean = con.execute(f"SELECT COUNT(*) FROM {dst};").fetchone()[0]
    print(f"[{src} -> {dst}] Raw: {raw:,}  |  Clean: {clean:,}  |  Removed: {raw - clean:,}")


# Build union tables across all cleaned yearly tables
def build_unions(con, cleaned_pairs):
    # Per-cab collections
    yellow_clean = [dst for (src, dst) in cleaned_pairs if src.startswith("yellow_")]
    green_clean  = [dst for (src, dst) in cleaned_pairs if src.startswith("green_")]

    # Helper to create a union table
    def make_union_table(name: str, tables: list[str]):
        con.execute(f"DROP TABLE IF EXISTS {name};")
        if not tables:
            logger.info("No tables to union for %s", name)
            return
        union_sql = " UNION ALL ".join([f"SELECT * FROM {t}" for t in tables])
        con.execute(f"CREATE TABLE {name} AS {union_sql};")
        cnt = con.execute(f"SELECT COUNT(*) FROM {name};").fetchone()[0]
        # pre-format the count with commas for logging
        logger.info("Created %s with %s rows", name, f"{cnt:,}")
        print(f"[UNION] {name}: {cnt:,} rows")

    make_union_table("yellow_trips_clean_all", yellow_clean)
    make_union_table("green_trips_clean_all", green_clean)
    make_union_table("all_trips_clean_2015_2024", yellow_clean + green_clean)


# Orchestrate cleaning and union building
def main():
    try:
        con = duckdb.connect(DB_PATH, read_only=False)
        logger.info("Connected to DuckDB")

        # Discover all source tables that actually exist
        cleaned_pairs = []
        for src, dst in discover_src_tables(con):
            clean_one(con, src, dst)
            summarize_before_after(con, src, dst)
            verify_clean(con, dst)
            cleaned_pairs.append((src, dst))

        if not cleaned_pairs:
            print("No source tables found. Run load.py first.")
            return

        # Build consolidated union tables for convenience
        build_unions(con, cleaned_pairs)

        # Final summary
        made = ", ".join(dst for _, dst in cleaned_pairs)
        print("\nCleaning complete.")
        print(f"Created cleaned tables: {made}")
        if any(s.startswith("yellow_") for s, _ in cleaned_pairs):
            print("Created union: yellow_trips_clean_all")
        if any(s.startswith("green_") for s, _ in cleaned_pairs):
            print("Created union: green_trips_clean_all")
        print("Created union: all_trips_clean_2015_2024")

    except Exception as e:
        logger.exception("Error during cleaning")
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
