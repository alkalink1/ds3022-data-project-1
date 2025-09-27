import duckdb
import os
import logging
import time  # added for rate limiting

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Enable remote file access
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

        # Drop if exists, then create Yellow trips table
        con.execute("DROP TABLE IF EXISTS yellow_trips_2024;")
        for month in range(1, 13):
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{month:02d}.parquet"
            if month == 1:
                con.execute(f"""
                    CREATE TABLE yellow_trips_2024 AS
                    SELECT
                        'yellow' AS cab_type,
                        VendorID AS vendor_id,
                        tpep_pickup_datetime AS pickup_datetime,
                        tpep_dropoff_datetime AS dropoff_datetime,
                        passenger_count,
                        trip_distance
                    FROM read_parquet('{url}');
                """)
            else:
                con.execute(f"""
                    INSERT INTO yellow_trips_2024
                    SELECT
                        'yellow' AS cab_type,
                        VendorID AS vendor_id,
                        tpep_pickup_datetime AS pickup_datetime,
                        tpep_dropoff_datetime AS dropoff_datetime,
                        passenger_count,
                        trip_distance
                    FROM read_parquet('{url}');
                """)
            logger.info("Loaded Yellow 2024 month %02d", month)
            time.sleep(30)  # pause 30s between months

        logger.info("Loaded all Yellow 2024 files")

        # Drop if exists, then create Green trips table
        con.execute("DROP TABLE IF EXISTS green_trips_2024;")
        for month in range(1, 13):
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-{month:02d}.parquet"
            if month == 1:
                con.execute(f"""
                    CREATE TABLE green_trips_2024 AS
                    SELECT
                        'green' AS cab_type,
                        VendorID AS vendor_id,
                        lpep_pickup_datetime AS pickup_datetime,
                        lpep_dropoff_datetime AS dropoff_datetime,
                        passenger_count,
                        trip_distance
                    FROM read_parquet('{url}');
                """)
            else:
                con.execute(f"""
                    INSERT INTO green_trips_2024
                    SELECT
                        'green' AS cab_type,
                        VendorID AS vendor_id,
                        lpep_pickup_datetime AS pickup_datetime,
                        lpep_dropoff_datetime AS dropoff_datetime,
                        passenger_count,
                        trip_distance
                    FROM read_parquet('{url}');
                """)
            logger.info("Loaded Green 2024 month %02d", month)
            time.sleep(30)  # pause 30s between months

        logger.info("Loaded all Green 2024 files")

        # Load vehicle_emissions.csv into its own table
        con.execute("DROP TABLE IF EXISTS vehicle_emissions;")
        con.execute("""
            CREATE TABLE vehicle_emissions AS
            SELECT * FROM read_csv_auto('data/vehicle_emissions.csv', header=True);
        """)
        logger.info("Loaded vehicle_emissions.csv")

        # Summaries (basic data summarization)
        yellow_summary = con.execute("SELECT COUNT(*), MIN(pickup_datetime), MAX(pickup_datetime) FROM yellow_trips_2024;").fetchone()
        green_summary = con.execute("SELECT COUNT(*), MIN(pickup_datetime), MAX(pickup_datetime) FROM green_trips_2024;").fetchone()
        emissions_count = con.execute("SELECT COUNT(*) FROM vehicle_emissions;").fetchone()[0]

        print(f"Yellow Trips: {yellow_summary[0]} rows, {yellow_summary[1]} to {yellow_summary[2]}")
        print(f"Green Trips: {green_summary[0]} rows, {green_summary[1]} to {green_summary[2]}")
        print(f"Vehicle Emissions: {emissions_count} rows")

        logger.info("Yellow summary: %s", yellow_summary)
        logger.info("Green summary: %s", green_summary)
        logger.info("Emissions rows: %s", emissions_count)

        # Preview first few rows of emissions to confirm schema
        preview = con.execute("SELECT * FROM vehicle_emissions LIMIT 5;").fetchall()
        print("Preview of vehicle_emissions:")
        for row in preview:
            print(row)
        logger.info("Preview of vehicle_emissions: %s", preview)

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()