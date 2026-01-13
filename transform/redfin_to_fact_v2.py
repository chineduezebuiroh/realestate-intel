# transform/redfin_to_fact_v2.py
import os, duckdb, pandas as pd, yaml
from glob import glob
from pathlib import Path
from typing import Optional, List

# Redfin "PROPERTY_TYPE" → our canonical IDs
PTYPE_MAP = {
    "All Residential": "all",
    "Single Family Residential": "single_family_residential",
    "Condo/Co-op": "condo_co-op",
    "Townhouse": "townhouse",
    "Multi-Family (2-4 Unit)": "multi_family",
    "Manufactured": "manufactured",
    # fallback: slugify
}
def _ptype_id(raw: str) -> str:
    if raw in PTYPE_MAP: 
        return PTYPE_MAP[raw]
    return str(raw or "all").strip().lower().replace("/", "_").replace(" ", "_").replace("-", "_")



def _ptype_label_and_group(raw: str) -> tuple[str, str]:
    """
    Return (label, group) from the raw Redfin PROPERTY_TYPE.
    Label = human-readable, Group = coarse bucket for UI (optional).
    """
    raw = (raw or "").strip()
    if not raw:
        return ("All Home Types", "all")
    # Simple heuristics; tweak to your taste
    txt = raw.lower()
    if "single family" in txt:
        return ("Single-Family", "residential")
    if "condo" in txt or "co-op" in txt:
        return ("Condo/Co-op", "residential")
    if "townhouse" in txt or "townhome" in txt:
        return ("Townhouse", "residential")
    if "multi" in txt:
        return ("Multi-Family (2–4)", "residential")
    if "manufactured" in txt:
        return ("Manufactured", "residential")
    if "all residential" in txt:
        return ("All Home Types", "all")
    # Fallback: title-case the raw text
    return (raw.title(), "other")



ROOT = Path("data/raw/redfin")

SOURCE = (
    "redfin",
    "Redfin Data Center",
    "https://www.redfin.com/news/data-center/",
    "monthly",
    "public",
)

# Optional: load market metadata from config/markets.yml (if available)
MARKETS_YAML = Path("config/markets.yml")
MARKETS = {}
if MARKETS_YAML.exists():
    with open(MARKETS_YAML, "r") as f:
        try:
            MARKETS = yaml.safe_load(f) or {}
            print(f"[redfin] loaded {len(MARKETS)} market entries from config/markets.yml")
        except Exception as e:
            print("[redfin] warning: couldn't parse markets.yml:", e)


# canonical_metric_id -> (display_name, unit, category)
# canonical_metric_id must match what you write into fact_timeseries.metric_id
COL_MAP = {
    "median_sale_price":        ("Median Sale Price",        "usd",     "prices"),
    "homes_sold":               ("Homes Sold",               "homes",   "sales"),
    "inventory":                ("Active Inventory",         "homes",   "supply"),
    "new_listings":             ("New Listings",             "homes",   "supply"),
    "median_dom":               ("Median Days on Market",    "days",    "speed"),
    "months_of_supply":         ("Months of Supply",         "months",  "supply"),
    "avg_sale_to_list":         ("Sale-to-List Ratio",       "ratio",   "prices"),
    "off_market_in_two_weeks":  ("Off-Market in 2 Weeks %",  "percent", "speed"),
    "pending_sales":            ("Pending Sales",            "homes",   "sales"),
    # add any others you actually ingest (price_drops, etc.)
    "median_dom":               ("Median Days on Market",    "days",    "speed"),
    "avg_sale_to_list":         ("Sale-to-List Ratio",       "ratio",   "prices"),
    "median_list_price":        ("Median List Price",        "usd",     "prices"),
    "median_list_ppsf":         ("Median List Price per Sq Ft", "usd_per_sqft", "prices"),
    "median_ppsf":              ("Median Sale Price per Sq Ft", "usd_per_sqft", "prices"),
    "sold_above_list":          ("Sold Above List %",        "percent", "prices"),
    "price_drops":              ("Price Drops",              "homes",   "supply"),
}




def _read_tsv(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path): return None
    try:
        return pd.read_csv(path, sep="\t")
    except Exception as e:
        print(f"[redfin:{path}] read failed: {e}")
        return None



def ensure_dims(con: duckdb.DuckDBPyConnection, geo_df: pd.DataFrame):
    # source
    con.execute("""
        INSERT INTO dim_source(source_id, name, url, cadence, license)
        SELECT ?, ?, ?, ?, ?
        WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id = ?)
    """, [*SOURCE, SOURCE[0]])

    # metrics (monthly)
    for canonical_metric_id, (name, unit, cat) in COL_MAP.items():
        con.execute("""
            INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
            SELECT ?, ?, 'monthly', ?, ?
            WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id = ?)
        """, [canonical_metric_id, name, unit, cat, canonical_metric_id])

    
    derivatives = []
    for base_id, (name, unit, cat) in COL_MAP.items():
        derivatives.append((f"{base_id}_mom", f"{name} (MoM)", unit, cat))
        derivatives.append((f"{base_id}_yoy", f"{name} (YoY)", unit, cat))
    
    for mid, nm, un, cat in derivatives:
        con.execute("""
          INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
          SELECT ?, ?, 'monthly', ?, ?
          WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id = ?)
        """, [mid, nm, un, cat, mid])


    # markets (auto from geo_df)
    con.register("df_geo", geo_df)
    con.execute("DELETE FROM dim_market WHERE geo_id IN (SELECT geo_id FROM df_geo)")
    con.execute("""
        INSERT INTO dim_market (geo_id, name, type, fips)
        SELECT geo_id, name, type, fips FROM df_geo
    """)






def main():

    #from glob import glob
    #from pathlib import Path
    
    pieces = []
    geo_meta = []
    ptype_rows = set() 
    
    files = glob("data/raw/redfin/*/*_monthly_latest.tsv")
    if not files:
        print("[redfin] ❌ no per-geo Redfin slice files found under data/raw/redfin/*/")
        raise SystemExit(1)
    
    print(f"[redfin] discovered {len(files)} slice(s)")
    for path in files:
        path = Path(path)
        level = path.parent.name                     # city/county/state
        geo_id = path.stem.split("_monthly_")[0]     # e.g., dc_city
    
        try:
            df = pd.read_csv(path, sep="\t")
        except Exception as e:
            print(f"[redfin:{geo_id}] failed to read: {e}")
            continue
        if df.empty:
            continue


        # ---- canonicalize header lookup (case-insensitive) ----
        lc = {c.lower(): c for c in df.columns}

        # ---- choose a date column (prefer period_end) ----
        date_candidates = ["period_end", "period_end_date", "month", "date"]
        date_raw = None
        for key in date_candidates:
            if key in lc:
                date_raw = lc[key]
                break
        
        if date_raw is None:
            print(f"[redfin:{geo_id}] ❌ no usable date column in {path.name}; columns={list(df.columns)[:8]}...")
            continue  # skip this slice safely
        
        # ---- normalize to month-end ----
        df["date"] = (
            pd.to_datetime(df[date_raw], errors="coerce")
              .dt.to_period("M").dt.to_timestamp("M")
        )
        
        # drop rows where we still couldn't parse a date
        df = df.dropna(subset=["date"])
        if df.empty:
            print(f"[redfin:{geo_id}] ⚠️ all rows had invalid dates in {path.name}; skipping")
            continue

        

        # ---- property type extraction (robust) ----
        pid_col   = lc.get("property_type_id")
        pname_col = lc.get("property_type") or lc.get("propertytype") or lc.get("ptype")
        
        import re
        def slugify(s: str) -> str:
            return re.sub(r'[^a-z0-9]+', '_', str(s).lower()).strip('_')
        
        if pid_col is not None:
            df["__ptype_id__"] = df[pid_col].astype(str).str.lower().str.strip()
        elif pname_col is not None:
            df["__ptype_id__"] = df[pname_col].fillna("all").map(slugify)
        else:
            df["__ptype_id__"] = "all"

        # DEBUG: what property types are present in this slice?
        ptypes_here = sorted(df["__ptype_id__"].dropna().astype(str).unique().tolist())
        print(f"[redfin:{geo_id}] property_type_ids in slice:", ptypes_here[:20], ("..." if len(ptypes_here) > 20 else ""))
        
        # Also compute readable label & group for the dim table
        if pname_col is not None:
            labels_groups = df[pname_col].fillna("All Residential").apply(_ptype_label_and_group)
        else:
            # if we only had IDs, make a best-effort label from ID
            labels_groups = df["__ptype_id__"].fillna("all").apply(
                lambda s: _ptype_label_and_group(str(s).replace("_", " "))
            )
        df["__ptype_name__"]  = labels_groups.apply(lambda t: t[0])
        df["__ptype_group__"] = labels_groups.apply(lambda t: t[1])

        # Accumulate unique rows for dim_property_type
        for pid, pname, pgrp in zip(df["__ptype_id__"], df["__ptype_name__"], df["__ptype_group__"]):
            ptype_rows.add((str(pid), str(pname), str(pgrp)))



        
        #for source_col_lc, (metric_id, _name, _unit, _cat) in COL_MAP.items():
        for source_col_lc, (name, unit, cat) in COL_MAP.items():
            metric_id = source_col_lc  # canonical metric_id

            exact = next((c for c in df.columns if c.lower() == source_col_lc), None)
            if exact is None:
                continue
            
            # guard: make sure 'date' is there (it should be, from step 1)
            if "date" not in df.columns:
                print(f"[redfin:{geo_id}] ❌ internal: 'date' missing just before flatten; skipping metric {source_col_lc}")
                continue
            
            sub = (df[["date", exact, "__ptype_id__"]]
                   .dropna(subset=[exact])
                   .rename(columns={exact: "value"})
                   .copy())
            if sub.empty:
                continue
            
            sub["metric_id"]        = metric_id
            sub["geo_id"]           = geo_id
            sub["source_id"]        = SOURCE[0]
            sub["property_type_id"] = sub["__ptype_id__"]
            pieces.append(sub)



        

    
        # capture minimal market metadata (can be enriched from config later)
        geo_meta.append({
            "geo_id": geo_id,
            "name":   geo_id,     # fallback; we’ll improve via markets.yml if desired
            "type":   level,
            "fips":   None,
        })


    
    if not pieces:
        print("[redfin] no usable data after parsing.")
        raise SystemExit(0)
    
    tall = pd.concat(pieces, ignore_index=True)
    tall["value"] = pd.to_numeric(tall["value"], errors="coerce")
    tall = tall.dropna(subset=["date","value","metric_id","geo_id"])
    
    geo_df = pd.DataFrame(geo_meta).drop_duplicates(subset=["geo_id"])



    con = duckdb.connect("./data/market.duckdb")
    ensure_dims(con, geo_df)


    # Normalize and dedupe within the stage to one row per 4-key
    tall["property_type_id"] = tall["property_type_id"].fillna("all").astype(str)
    # If the same 4-key appears from multiple files, keep the last (usually _latest)
    tall = (
        tall.sort_values(["date"])
            .drop_duplicates(
                subset=["geo_id","metric_id","date","property_type_id"],
                keep="last"
            )
    )
   

    # Ensure dim_property_type exists
    con.execute("""
      CREATE TABLE IF NOT EXISTS dim_property_type(
        property_type_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        "group" VARCHAR
      );
    """)
    
    # Upsert labels/groups gathered during parsing
    if ptype_rows:
        df_ptypes = pd.DataFrame(list(ptype_rows),
                                 columns=["property_type_id","name","group"])
        con.register("df_ptypes", df_ptypes)
    
        # INSERT OR REPLACE is fine here because property_type_id is PK
        con.execute("""
          INSERT OR REPLACE INTO dim_property_type(property_type_id, name, "group")
          SELECT property_type_id, name, "group" FROM df_ptypes;
        """)
    
        # optional (safe): con.unregister("df_ptypes")

    
    
    # register with property_type_id included
    con.register("df_stage", tall[["geo_id","metric_id","date","property_type_id","value","source_id"]])



    # 4-key upsert
    con.execute("""
    DELETE FROM fact_timeseries AS f
    WHERE EXISTS (
      SELECT 1
      FROM df_stage AS s
      WHERE s.geo_id = f.geo_id
        AND s.metric_id = f.metric_id
        AND s.date = f.date
        AND s.property_type_id = f.property_type_id
    );
    """)


    
    # 2) Insert fresh rows
    con.execute("""
    INSERT INTO fact_timeseries(geo_id, metric_id, date, property_type_id, value, source_id)
    SELECT geo_id, metric_id, date, property_type_id, CAST(value AS DOUBLE), source_id
    FROM df_stage;
    """)





    
    print(con.execute("""
        SELECT geo_id, metric_id, COUNT(*) AS rows, MIN(date) AS first, MAX(date) AS last
        FROM fact_timeseries
        WHERE source_id='redfin'
        GROUP BY 1,2 ORDER BY 1,2
    """).fetchdf())
    con.close()

    bad = con.execute("""
      SELECT COUNT(*)
      FROM fact_timeseries
      WHERE source_id='redfin'
        AND property_type IS NULL
    """).fetchone()[0]
    
    if bad > 0:
        raise RuntimeError(f"[redfin] invariant violated: {bad} rows with NULL property_type")






if __name__ == "__main__":
    main()
