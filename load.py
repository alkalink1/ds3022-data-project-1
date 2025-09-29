import duckdb
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

DB_PATH = 'emissions.duckdb'
YEARS = range(2015, 2025)  # 2015..2024 inclusive
MONTHS = range(1, 13)
RATE_LIMIT_SECONDS = 15  # to try

def load_year_cab(con, year: int, cab: str):
    """
    Create (drop+create) a table for the given year & cab and load all 12 months.
    cab ∈ {'yellow','green'}
    """
    assert cab in ('yellow', 'green')
    table = f"{cab}_trips_{year}"
    logger.info("Loading %s", table)

    # Choose field names & URL patterns
    if cab == 'yellow':
        url_tpl = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
        vendor_col = "VendorID"
        pu_col = "tpep_pickup_datetime"
        do_col = "tpep_dropoff_datetime"
    else:
        url_tpl = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"
        vendor_col = "VendorID"
        pu_col = "lpep_pickup_datetime"
        do_col = "lpep_dropoff_datetime"

    # Drop then create from first month, append the rest
    con.execute(f"DROP TABLE IF EXISTS {table};")

    first_done = False
    for m in MONTHS:
        url = url_tpl.format(year=year, month=m)

        if not first_done:
            con.execute(f"""
                CREATE TABLE {table} AS
                SELECT
                    '{cab}' AS cab_type,
                    {vendor_col} AS vendor_id,
                    {pu_col}   AS pickup_datetime,
                    {do_col}   AS dropoff_datetime,
                    passenger_count,
                    trip_distance
                FROM read_parquet('{url}');
            """)
            first_done = True
        else:
            con.execute(f"""
                INSERT INTO {table}
                SELECT
                    '{cab}' AS cab_type,
                    {vendor_col} AS vendor_id,
                    {pu_col}   AS pickup_datetime,
                    {do_col}   AS dropoff_datetime,
                    passenger_count,
                    trip_distance
                FROM read_parquet('{url}');
            """)
        logger.info("Loaded %s %04d-%02d", cab.capitalize(), year, m)
        time.sleep(RATE_LIMIT_SECONDS)

    # Basic summary
    cnt, mindt, maxdt = con.execute(
        f"SELECT COUNT(*), MIN(pickup_datetime), MAX(pickup_datetime) FROM {table};"
    ).fetchone()
    print(f"{table}: {cnt} rows, {mindt} to {maxdt}")
    logger.info("%s summary: rows=%s, min=%s, max=%s", table, cnt, mindt, maxdt)
    return table, cnt

def load_parquet_files():
    con = None
    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database=DB_PATH, read_only=False)
        logger.info("Connected to DuckDB instance")

        # Enable remote file access
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

        total_yellow = 0
        total_green = 0

        # Load all years for Yellow
        for y in YEARS:
            _, cnt = load_year_cab(con, y, 'yellow')
            total_yellow += cnt

        # Load all years for Green
        for y in YEARS:
            _, cnt = load_year_cab(con, y, 'green')
            total_green += cnt

        logger.info("Finished loading all Yellow & Green tables 2015–2024")

        # Load vehicle_emissions.csv into its own table (overwrite)
        if not os.path.exists("data/vehicle_emissions.csv"):
            raise FileNotFoundError("Missing data/vehicle_emissions.csv")
        con.execute("DROP TABLE IF EXISTS vehicle_emissions;")
        con.execute("""
            CREATE TABLE vehicle_emissions AS
            SELECT * FROM read_csv_auto('data/vehicle_emissions.csv', header=True);
        """)
        logger.info("Loaded vehicle_emissions.csv")

        # Simple emissions preview
        emissions_count = con.execute("SELECT COUNT(*) FROM vehicle_emissions;").fetchone()[0]
        preview = con.execute("SELECT * FROM vehicle_emissions LIMIT 5;").fetchall()

        print("\n=== GRAND TOTALS (2015–2024) ===")
        print(f"Yellow Trips (all years): {total_yellow:,}")
        print(f"Green Trips  (all years): {total_green:,}")
        print(f"vehicle_emissions rows:   {emissions_count:,}\n")

        print("Preview of vehicle_emissions (first 5 rows):")
        for row in preview:
            print(row)

        logger.info("Grand totals — Yellow: %s, Green: %s, Emissions rows: %s",
                    total_yellow, total_green, emissions_count)
        logger.info("Preview vehicle_emissions: %s", preview)

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error("An error occurred: %s", e)
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass

if __name__ == "__main__":
    load_parquet_files()
