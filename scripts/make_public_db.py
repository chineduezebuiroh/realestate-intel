# scripts/make_public_db.py

from pathlib import Path
import os
import duckdb

# Source (full) DB and target (public) DB
FULL_DB = Path(os.getenv("FULL_DUCKDB_PATH", "data/market.duckdb"))
PUBLIC_DB = Path(os.getenv("PUBLIC_DUCKDB_PATH", "data/market_public.duckdb"))

# If you want, you can tighten this later (e.g. only post-2000 data)
MIN_DATE = os.getenv("PUBLIC_MIN_DATE")  # e.g. "2000-01-01" or empty

MAX_MB = 100.0  # GitHub hard limit

def main() -> None:
    if not FULL_DB.exists():
        raise SystemExit(f"[make_public_db] Source DB not found: {FULL_DB}")

    # Remove old snapshot
    if PUBLIC_DB.exists():
        print(f"[make_public_db] Removing existing {PUBLIC_DB}")
        PUBLIC_DB.unlink()

    print(f"[make_public_db] Building {PUBLIC_DB} from {FULL_DB}")

    # Connect to full DB
    con = duckdb.connect(str(FULL_DB))

    # Attach new, empty public DB
    con.execute(f"ATTACH DATABASE '{PUBLIC_DB.as_posix()}' AS public_db;")

    # 1) Copy tables you need for the app
    # You can add more here if needed.
    print("[make_public_db] Copying tables into public_db")

    # geo_manifest (dimension)
    print("[make_public_db] Creating geo_manifest in public_db from CSV")
    con.execute("""
        CREATE TABLE public_db.geo_manifest AS
        SELECT
            geo_id,
            level,
            geo_name,
            bls_ces_area_code,
            include_ces,
            bls_laus_area_code,
            include_laus,
            redfin_code,
            include_redfin,
            census_code,
            include_census,
            bea_geo_fips,
            include_bea_qgdp,
            fred_unemp_series_id,
            include_fred_unemp,
            fred_geo_code,
            include_fred
        FROM read_csv_auto('config/geo_manifest.csv', header=True);
    """)
    

    # fact_timeseries (facts) – optionally filter by date to keep size down
    if MIN_DATE:
        print(f"[make_public_db] Copying fact_timeseries (date >= {MIN_DATE})")
        con.execute("""
            CREATE TABLE public_db.fact_timeseries AS
            SELECT *
            FROM main.fact_timeseries
            WHERE date >= ?;
        """, [MIN_DATE])
    else:
        print("[make_public_db] Copying full fact_timeseries")
        con.execute("""
            CREATE TABLE public_db.fact_timeseries AS
            SELECT *
            FROM main.fact_timeseries;
        """)

    # 2) Recreate the views inside public_db
    print("[make_public_db] Creating views in public_db")
    con.execute("SET schema 'public_db'")

    # v_geo_manifest
    con.execute("""
        CREATE OR REPLACE VIEW v_geo_manifest AS
        SELECT
            geo_id,
            level,
            census_code,
            geo_name AS name
        FROM geo_manifest;
    """)

    # v_fact_timeseries_enriched
    con.execute("""
        CREATE OR REPLACE VIEW v_fact_timeseries_enriched AS
        SELECT
            f.geo_id,
            g.level,
            g.census_code,
            f.metric_id,
            f.date,
            f.value,
            f.source_id,
            f.property_type_id,
            f.property_type
        FROM fact_timeseries f
        LEFT JOIN v_geo_manifest g
        USING (geo_id);
    """)

    # Optional: quick sanity check from inside public_db
    print("[make_public_db] Sample from v_fact_timeseries_enriched:")
    print(con.execute("""
        SELECT
            geo_id,
            MIN(date) AS first,
            MAX(date) AS last,
            COUNT(*) AS rows
        FROM v_fact_timeseries_enriched
        GROUP BY 1
        ORDER BY 1
        LIMIT 10;
    """).fetchdf())

    # Reset schema & detach
    con.execute("SET schema 'main'")
    
    print("[make_public_db] Finalizing public DB")
    # Switch back to main DB so DuckDB allows detaching public_db
    con.execute("USE main;")
    con.execute("DETACH DATABASE public_db")
    print("[make_public_db] Detached public_db")

    con.close()

    # 3) Size check
    size_mb = PUBLIC_DB.stat().st_size / (1024 * 1024)
    print(f"[make_public_db] Done. {PUBLIC_DB} size: {size_mb:.1f} MB")

    if size_mb > MAX_MB:
        raise SystemExit(
            f"[make_public_db] ERROR: {PUBLIC_DB} is {size_mb:.1f} MB, "
            f"which exceeds GitHub's 100MB limit of {MAX_MB:.0f} MB. "
            "Trim data (date range, metrics, geos) before committing."
        )
    elif size_mb > 95:
        print(
            "[make_public_db] WARNING: file is close to GitHub's 100MB limit.\n"
            "Consider filtering dates/metrics in fact_timeseries for the public snapshot."
        )

if __name__ == "__main__":
    main()
