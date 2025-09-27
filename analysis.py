# analysis.py
import os
import duckdb
import pandas as pd
import matplotlib.pyplot as plt

DB_PATH = "emissions.duckdb"
OUT_DIR = "outputs"
PLOT_PATH = os.path.join(OUT_DIR, "monthly_co2_totals.png")

DAY_NAMES = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
MONTH_NAMES = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
}

def table_exists(con, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?;",
        [name]
    ).fetchone()
    return row is not None

def get_max_trip(con, table):
    q = f"""
        SELECT
            trip_co2_kgs,
            trip_distance,
            pickup_datetime,
            dropoff_datetime,
            cab_type,
            vendor_id
        FROM {table}
        ORDER BY trip_co2_kgs DESC
        LIMIT 1;
    """
    return con.execute(q).fetchdf()

def avg_by_bucket(con, table, bucket_col):
    q = f"""
        SELECT {bucket_col} AS bucket, AVG(trip_co2_kgs) AS avg_co2
        FROM {table}
        GROUP BY 1
        HAVING bucket IS NOT NULL
        ORDER BY bucket;
    """
    return con.execute(q).fetchdf()

def monthly_totals(con, table):
    q = f"""
        SELECT month_of_year AS month, SUM(trip_co2_kgs) AS total_co2
        FROM {table}
        GROUP BY 1
        HAVING month IS NOT NULL
        ORDER BY month;
    """
    df = con.execute(q).fetchdf()
    # Ensure every month 1..12 shows up (fill with 0 if absent)
    full = pd.DataFrame({"month": list(range(1, 13))})
    df = full.merge(df, on="month", how="left").fillna({"total_co2": 0.0})
    df["label"] = df["month"].map(MONTH_NAMES)
    return df

def label_header(title: str):
    print("\n" + "=" * len(title))
    print(title)
    print("=" * len(title))

def analyze_cab(con, table: str, label: str):
    if not table_exists(con, table):
        print(f"[WARN] Missing table '{table}'. Skipping {label}.")
        return None

    label_header(f"{label} — ANALYSIS")

    # 1) Largest carbon-producing trip (single)
    top = get_max_trip(con, table)
    if not top.empty:
        r = top.iloc[0]
        print(f"[{label}] Largest single-trip CO₂: {r['trip_co2_kgs']:.3f} kg")
        print(f"  pickup:        {r['pickup_datetime']}")
        print(f"  dropoff:       {r['dropoff_datetime']}")
        print(f"  distance:      {r['trip_distance']} miles")
        print(f"  vendor_id:     {r['vendor_id']}")

    # 2) Hour of day (report 1–24). We compute average CO₂ per trip by hour.
    hour_df = avg_by_bucket(con, table, "hour_of_day")
    if not hour_df.empty:
        # Convert 0–23 to 1–24 for reporting
        hour_df["report_hour"] = ((hour_df["bucket"] % 24) + 1).astype(int)
        light_h = hour_df.loc[hour_df["avg_co2"].idxmin()]
        heavy_h = hour_df.loc[hour_df["avg_co2"].idxmax()]
        print(f"[{label}] Lightest avg CO₂ hour (1–24): {int(light_h['report_hour'])} — {light_h['avg_co2']:.3f} kg/trip")
        print(f"[{label}] Heaviest avg CO₂ hour (1–24): {int(heavy_h['report_hour'])} — {heavy_h['avg_co2']:.3f} kg/trip")

    # 3) Day of week (Sun–Sat)
    dow_df = avg_by_bucket(con, table, "day_of_week")
    if not dow_df.empty:
        dow_df["name"] = dow_df["bucket"].astype(int).map(DAY_NAMES)
        light_d = dow_df.loc[dow_df["avg_co2"].idxmin()]
        heavy_d = dow_df.loc[dow_df["avg_co2"].idxmax()]
        print(f"[{label}] Lightest avg CO₂ day: {light_d['name']} — {light_d['avg_co2']:.3f} kg/trip")
        print(f"[{label}] Heaviest avg CO₂ day: {heavy_d['name']} — {heavy_d['avg_co2']:.3f} kg/trip")

    # 4) Week of year (1–52/53)
    wk_df = avg_by_bucket(con, table, "week_of_year")
    if not wk_df.empty:
        light_w = wk_df.loc[wk_df["avg_co2"].idxmin()]
        heavy_w = wk_df.loc[wk_df["avg_co2"].idxmax()]
        print(f"[{label}] Lightest avg CO₂ week: {int(light_w['bucket'])} — {light_w['avg_co2']:.3f} kg/trip")
        print(f"[{label}] Heaviest avg CO₂ week: {int(heavy_w['bucket'])} — {heavy_w['avg_co2']:.3f} kg/trip")

    # 5) Month of year (Jan–Dec)
    mo_df = avg_by_bucket(con, table, "month_of_year")
    if not mo_df.empty:
        mo_df["name"] = mo_df["bucket"].astype(int).map(MONTH_NAMES)
        light_m = mo_df.loc[mo_df["avg_co2"].idxmin()]
        heavy_m = mo_df.loc[mo_df["avg_co2"].idxmax()]
        print(f"[{label}] Lightest avg CO₂ month: {light_m['name']} — {light_m['avg_co2']:.3f} kg/trip")
        print(f"[{label}] Heaviest avg CO₂ month: {heavy_m['name']} — {heavy_m['avg_co2']:.3f} kg/trip")

    # Return monthly totals for plotting
    return monthly_totals(con, table)

def plot_monthly(yellow_df, green_df):
    os.makedirs(OUT_DIR, exist_ok=True)

    plt.figure(figsize=(12, 6))
    fig, ax2 = plt.subplots(figsize=(12, 6))

    if yellow_df is not None and green_df is not None:
        ax2_twin = ax2.twinx()
        
        line1 = ax2.plot(yellow_df["month"], yellow_df["total_co2"], marker="o", color="gold", label="Yellow", linewidth=2)
        line2 = ax2_twin.plot(green_df["month"], green_df["total_co2"], marker="s", color="green", label="Green", linewidth=2)
        
        ax2.set_xticks(list(range(1, 13)))
        ax2.set_xticklabels([MONTH_NAMES[m] for m in range(1, 13)])
        ax2.set_ylabel("Yellow CO₂ (kg)", color="gold")
        ax2_twin.set_ylabel("Green CO₂ (kg)", color="green")
        ax2.set_title("Monthly Taxi CO₂ Totals — 2024 (Dual Y-Axis)")
        
        # Combine legends
        lines = line1 + line2
        labels = [l.get_label() for l in lines]
        ax2.legend(lines, labels, loc='upper left')
        ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(PLOT_PATH, dpi=150)
    print(f"\n[Plot] Saved monthly CO₂ totals to: {PLOT_PATH}")

def main():
    con = duckdb.connect(DB_PATH, read_only=True)

    yellow_table = "yellow_trips_2024_transformed"
    green_table  = "green_trips_2024_transformed"

    y_df = analyze_cab(con, yellow_table, "YELLOW")
    g_df = analyze_cab(con, green_table,  "GREEN")

    if y_df is None and g_df is None:
        print("\nNo transformed tables found. Run transform.py first.")
        return

    plot_monthly(y_df, g_df)

if __name__ == "__main__":
    main()
