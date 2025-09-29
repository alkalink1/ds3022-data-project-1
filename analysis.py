# analysis.py
import os
import duckdb
import pandas as pd
import matplotlib.pyplot as plt

DB_PATH = "emissions.duckdb"
OUT_DIR = "outputs"
PLOT_PATH = os.path.join(OUT_DIR, "monthly_co2_totals_2015_2024.png")

DAY_NAMES = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
MONTH_NAMES = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
}

CAB_TABLES = {
    "YELLOW": "yellow_trips_transformed_all",
    "GREEN":  "green_trips_transformed_all",
}

# Check if a table exists in DuckDB
def table_exists(con, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?;",
        [name]
    ).fetchone()
    return row is not None

# Print a section header label
def label_header(title: str):
    print("\n" + "=" * len(title))
    print(title)
    print("=" * len(title))

# Get min/max pickup_datetime and row count
def get_date_range(con, table):
    q = f"""
        SELECT
            MIN(pickup_datetime) AS min_dt,
            MAX(pickup_datetime) AS max_dt,
            COUNT(*) AS rows
        FROM {table}
    """
    return con.execute(q).fetchdf().iloc[0]

# Get the highest CO₂ single trip
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
        WHERE trip_co2_kgs IS NOT NULL
        ORDER BY trip_co2_kgs DESC
        LIMIT 1;
    """
    return con.execute(q).fetchdf()

# Average CO₂ grouped by a bucket column
def avg_by_bucket(con, table, bucket_col):
    q = f"""
        SELECT {bucket_col} AS bucket, AVG(trip_co2_kgs) AS avg_co2
        FROM {table}
        WHERE trip_co2_kgs IS NOT NULL AND {bucket_col} IS NOT NULL
        GROUP BY 1
        ORDER BY 1;
    """
    return con.execute(q).fetchdf()

# Monthly totals across all 10 years
def monthly_totals_full(con, table):
    q = f"""
        SELECT
            date_trunc('month', pickup_datetime) AS ym,
            EXTRACT('year' FROM pickup_datetime)::INT   AS year,
            EXTRACT('month' FROM pickup_datetime)::INT  AS month,
            SUM(trip_co2_kgs) AS total_co2
        FROM {table}
        WHERE trip_co2_kgs IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY ym
    """
    df = con.execute(q).fetchdf()
    df["label"] = df["ym"].dt.strftime("%Y-%m")
    return df

# Find heaviest and lightest total-CO₂ months
def heaviest_lightest_month_totals(df):
    if df.empty:
        return None, None
    heavy = df.loc[df["total_co2"].idxmax()]
    light = df.loc[df["total_co2"].idxmin()]
    return heavy, light

# Print summary stats for a cab table
def analyze_cab(con, table: str, label: str):
    if not table_exists(con, table):
        print(f"[WARN] Missing table '{table}'. Skipping {label}.")
        return None

    label_header(f"{label} — ANALYSIS (2015–2024)")

    # Coverage info
    rng = get_date_range(con, table)
    print(f"[{label}] Coverage: {pd.to_datetime(rng['min_dt'])} → {pd.to_datetime(rng['max_dt'])}  ({int(rng['rows']):,} rows)")

    # 1) Largest carbon-producing trip (single across all years)
    top = get_max_trip(con, table)
    if not top.empty:
        r = top.iloc[0]
        print(f"[{label}] Largest single-trip CO₂: {r['trip_co2_kgs']:.3f} kg")
        print(f"  pickup:        {r['pickup_datetime']}")
        print(f"  dropoff:       {r['dropoff_datetime']}")
        print(f"  distance:      {r['trip_distance']} miles")
        print(f"  vendor_id:     {r['vendor_id']}")

    # 2) Hour of day (report 1–24). Average CO₂ per trip by hour across all years.
    hour_df = avg_by_bucket(con, table, "hour_of_day")
    if not hour_df.empty:
        hour_df["report_hour"] = ((hour_df["bucket"] % 24) + 1).astype(int)
        light_h = hour_df.loc[hour_df["avg_co2"].idxmin()]
        heavy_h = hour_df.loc[hour_df["avg_co2"].idxmax()]
        print(f"[{label}] Lightest avg CO₂ hour (1–24): {int(light_h['report_hour'])} — {light_h['avg_co2']:.3f} kg/trip")
        print(f"[{label}] Heaviest avg CO₂ hour (1–24): {int(heavy_h['report_hour'])} — {heavy_h['avg_co2']:.3f} kg/trip")

    # 3) Day of week (Sun–Sat) — average CO₂ per trip across all years
    dow_df = avg_by_bucket(con, table, "day_of_week")
    if not dow_df.empty:
        dow_df["name"] = dow_df["bucket"].astype(int).map(DAY_NAMES)
        light_d = dow_df.loc[dow_df["avg_co2"].idxmin()]
        heavy_d = dow_df.loc[dow_df["avg_co2"].idxmax()]
        print(f"[{label}] Lightest avg CO₂ day: {light_d['name']} — {light_d['avg_co2']:.3f} kg/trip")
        print(f"[{label}] Heaviest avg CO₂ day: {heavy_d['name']} — {heavy_d['avg_co2']:.3f} kg/trip")

    # 4) Week of year (1–52/53) — average CO₂ per trip across all years
    wk_df = avg_by_bucket(con, table, "week_of_year")
    if not wk_df.empty:
        # DuckDB ISO week can be 1–53
        light_w = wk_df.loc[wk_df["avg_co2"].idxmin()]
        heavy_w = wk_df.loc[wk_df["avg_co2"].idxmax()]
        print(f"[{label}] Lightest avg CO₂ week: {int(light_w['bucket'])} — {light_w['avg_co2']:.3f} kg/trip")
        print(f"[{label}] Heaviest avg CO₂ week: {int(heavy_w['bucket'])} — {heavy_w['avg_co2']:.3f} kg/trip")

    # 5) Month of year (Jan–Dec) — average CO₂ per trip across all years
    mo_df = avg_by_bucket(con, table, "month_of_year")
    if not mo_df.empty:
        mo_df["name"] = mo_df["bucket"].astype(int).map(MONTH_NAMES)
        light_m = mo_df.loc[mo_df["avg_co2"].idxmin()]
        heavy_m = mo_df.loc[mo_df["avg_co2"].idxmax()]
        print(f"[{label}] Lightest avg CO₂ month: {light_m['name']} — {light_m['avg_co2']:.3f} kg/trip")
        print(f"[{label}] Heaviest avg CO₂ month: {heavy_m['name']} — {heavy_m['avg_co2']:.3f} kg/trip")

    # 6) Most/least carbon-heavy MONTH (by TOTALS, not averages), across the full 10 years
    mt = monthly_totals_full(con, table)
    heavy, light = heaviest_lightest_month_totals(mt)
    if heavy is not None:
        print(f"[{label}] Heaviest month total: {heavy['label']} — {heavy['total_co2']:.3f} kg")
        print(f"[{label}] Lightest  month total: {light['label']} — {light['total_co2']:.3f} kg")

    # Return monthly totals for plotting
    return mt

# Plot monthly totals for 2015–2024
def plot_monthly_10yr(y_df: pd.DataFrame | None, g_df: pd.DataFrame | None):
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import os

    y_ok = (y_df is not None) and (not y_df.empty)
    g_ok = (g_df is not None) and (not g_df.empty)

    if not y_ok and not g_ok:
        print("[Plot] No data to plot.")
        return

    # Filter to 2015–2024 and sort by month-year
    def prep(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["ym", "total_co2"])  # empty
        sel = df[(df["year"] >= 2015) & (df["year"] <= 2024)].copy()
        sel = sel.sort_values("ym")
        return sel

    y_m = prep(y_df)
    g_m = prep(g_df)

    if y_m.empty and g_m.empty:
        print("[Plot] No monthly data in range 2015–2024 to plot.")
        return

    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = PLOT_PATH

    plt.figure(figsize=(14, 6))
    if not y_m.empty:
        plt.plot(y_m["ym"], y_m["total_co2"], marker="o", label="YELLOW", linewidth=1.8, color="yellow")
    if not g_m.empty:
        plt.plot(g_m["ym"], g_m["total_co2"], marker="s", label="GREEN", linewidth=1.8, color="green")

    # Format x-axis as monthly ticks across 10 years
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=(1,4,7,10)))
    plt.xlabel("Month")
    plt.ylabel("CO₂ (kg)")
    plt.title("Monthly Taxi CO₂ Totals — 2015–2024")
    plt.legend()
    plt.grid(True, which='both', axis='both', alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    print(f"[Plot] Saved monthly CO₂ totals to: {out_path}")


# Plot yearly totals for 2015–2024
def plot_yearly_10yr(y_df: pd.DataFrame | None, g_df: pd.DataFrame | None):
    import matplotlib.pyplot as plt
    import os

    y_ok = (y_df is not None) and (not y_df.empty)
    g_ok = (g_df is not None) and (not g_df.empty)

    if not y_ok and not g_ok:
        print("[Plot] No data to plot for yearly totals.")
        return

    # Aggregate to yearly totals and restrict to 2015–2024
    def to_yearly(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["year", "total_co2"])  # empty
        yr = (
            df[(df["year"] >= 2015) & (df["year"] <= 2024)]
            .groupby("year", as_index=False)["total_co2"].sum()
            .sort_values("year")
        )
        return yr

    y_year = to_yearly(y_df)
    g_year = to_yearly(g_df)

    if y_year.empty and g_year.empty:
        print("[Plot] No yearly data in range 2015–2024 to plot.")
        return

    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, "yearly_co2_totals_2015_2024.png")

    plt.figure(figsize=(12, 6))
    if not y_year.empty:
        plt.plot(y_year["year"], y_year["total_co2"], marker="o", label="YELLOW", linewidth=2, color="yellow")
    if not g_year.empty:
        plt.plot(g_year["year"], g_year["total_co2"], marker="s", label="GREEN", linewidth=2, color="green")

    # X-axis ticks as whole years in range if present
    all_years = sorted(set(y_year.get("year", pd.Series(dtype=int))) | set(g_year.get("year", pd.Series(dtype=int))))
    if all_years:
        plt.xticks(all_years, [str(y) for y in all_years])
    plt.xlabel("Year")
    plt.ylabel("CO₂ (kg)")
    plt.title("Yearly Taxi CO₂ Totals — 2015–2024")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    print(f"[Plot] Saved yearly CO₂ totals to: {out_path}")

# Orchestrate analysis and plotting
def main():
    con = duckdb.connect(DB_PATH, read_only=True)

    # Prefer union tables created by transform.py
    missing = [t for t in CAB_TABLES.values() if not table_exists(con, t)]
    if missing:
        print("[WARN] Some union tables are missing:", ", ".join(missing))
        print("       Re-run transform.py so it creates *_trips_transformed_all union tables.")
        # You could add a fallback to stitch per-year tables, but directions say to build off transform.py.

    y_df = analyze_cab(con, CAB_TABLES["YELLOW"], "YELLOW")
    g_df = analyze_cab(con, CAB_TABLES["GREEN"],  "GREEN")

    if (y_df is None or y_df.empty) and (g_df is None or g_df.empty):
        print("\nNo transformed union tables found. Run transform.py first.")
        return

    plot_monthly_10yr(y_df, g_df)
    plot_yearly_10yr(y_df, g_df)

if __name__ == "__main__":
    main()
