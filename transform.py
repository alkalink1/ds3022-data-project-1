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

# Years & cabs to look for (matches load/clean)
YEARS = range(2015, 2025)  # 2015..2024 inclusive
CABS = ("yellow", "green")


# Check if a table exists in DuckDB
def table_exists(con, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?;",
        [name]
    ).fetchone()
    return row is not None


# Get lowercase column names from vehicle_emissions
def get_emissions_cols(con):
    return {
        r[0].lower()
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'vehicle_emissions';"
        ).fetchall()
    }


# Build CTE to fetch emissions factor
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


# Transform one cleaned table into enriched metrics
def transform_one(con, src_clean: str, dst_transformed: str, taxi_type: str):
    logger.info("Transforming %s -> %s (taxi_type=%s)", src_clean, dst_transformed, taxi_type)

    # Ensure prerequisites
    if not table_exists(con, "vehicle_emissions"):
        raise RuntimeError("vehicle_emissions lookup table not found. Run load.py first.")
    if not table_exists(con, src_clean):
        raise RuntimeError(f"Source cleaned table not found: {src_clean}. Run clean.py first.")

    con.execute(f"DROP TABLE IF EXISTS {dst_transformed};")

    emissions_cte = build_emissions_cte(con, taxi_type)

    # Compose the transform query
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
            FROM {src_clean}
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
    con.execute(f"CREATE TABLE {dst_transformed} AS {sql};")

    # Verify columns exist
    cols = {
        r[0].lower()
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?;",
            [dst_transformed]
        ).fetchall()
    }
    expected = {
        "cab_type","vendor_id","pickup_datetime","dropoff_datetime",
        "passenger_count","trip_distance",
        "trip_co2_kgs","avg_mph","hour_of_day","day_of_week","week_of_year","month_of_year"
    }
    missing = expected - cols
    if missing:
        raise RuntimeError(f"{dst_transformed} missing expected columns: {missing}")

    # Print a small summary and a few sample rows
    count = con.execute(f"SELECT COUNT(*) FROM {dst_transformed};").fetchone()[0]
    ex = con.execute(
        f"""
        SELECT cab_type, trip_distance, trip_co2_kgs, avg_mph,
               hour_of_day, day_of_week, week_of_year, month_of_year
        FROM {dst_transformed} LIMIT 3;
        """
    ).fetchall()
    print(dedent(f"""
        [{dst_transformed}] rows: {count:,}
        Sample rows:
          {ex}
    """).rstrip())


# List available cleaned tables to transform
def discover_cleaned_tables(con):
    """
    Return a list of tuples: (src_clean, dst_transformed, taxi_type)
    for all existing cleaned tables across years/cabs.
    """
    pairs = []
    for cab in CABS:
        for y in YEARS:
            src = f"{cab}_trips_{y}_clean"
            if table_exists(con, src):
                dst = f"{cab}_trips_{y}_transformed"
                pairs.append((src, dst, cab))
            else:
                logger.info("Cleaned table not found, skipping: %s", src)
    return pairs


# Create union tables across all transformed years
def build_unions(con, transformed_tables):
    """
    Build three consolidated union tables across all transformed tables:
      - yellow_trips_transformed_all
      - green_trips_transformed_all
      - all_trips_transformed_2015_2024
    """
    yellow_t = [t for t in transformed_tables if t.startswith("yellow_")]
    green_t  = [t for t in transformed_tables if t.startswith("green_")]

    # Helper to create a union table
    def make_union_table(name: str, tables: list[str]):
        con.execute(f"DROP TABLE IF EXISTS {name};")
        if not tables:
            logger.info("No tables to union for %s", name)
            return
        union_sql = " UNION ALL ".join([f"SELECT * FROM {t}" for t in tables])
        con.execute(f"CREATE TABLE {name} AS {union_sql};")
        cnt = con.execute(f"SELECT COUNT(*) FROM {name};").fetchone()[0]
        # Pre-format the count (avoid logging % ,d)
        logger.info("Created %s with %s rows", name, f"{cnt:,}")
        print(f"[UNION] {name}: {cnt:,} rows")

    make_union_table("yellow_trips_transformed_all", yellow_t)
    make_union_table("green_trips_transformed_all", green_t)
    make_union_table("all_trips_transformed_2015_2024", yellow_t + green_t)


# Orchestrate transforms and unions
def main():
    try:
        con = duckdb.connect(DB_PATH, read_only=False)
        logger.info("Connected to DuckDB")

        # Discover all cleaned tables that actually exist
        worklist = discover_cleaned_tables(con)
        if not worklist:
            print("No cleaned tables found. Run clean.py first.")
            return

        created = []
        for src_clean, dst_transformed, taxi in worklist:
            transform_one(con, src_clean, dst_transformed, taxi)
            created.append(dst_transformed)

        # Build consolidated unions
        build_unions(con, created)

        # Final summary
        made = ", ".join(created)
        print("\n=== TRANSFORM COMPLETE ===")
        print(f"Created transformed tables: {made}")
        if any(t.startswith("yellow_") for t in created):
            print("Created union: yellow_trips_transformed_all")
        if any(t.startswith("green_") for t in created):
            print("Created union: green_trips_transformed_all")
        print("Created union: all_trips_transformed_2015_2024")

    except Exception as e:
        logger.exception("Transform error")
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
