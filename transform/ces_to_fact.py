# transform/ces_to_fact.py
import os, sys, csv, duckdb
from collections import defaultdict
from datetime import date

DB_PATH    = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
GEN_PATH   = "config/ces_series.generated.csv"
STRICT     = os.getenv("CES_STRICT", "0") not in ("0","","false","False","no","No")

def sfx_from_sid(sid: str) -> str:
    s = (sid or "").strip().upper()
    if s.startswith("SMS"): return "sa"   # seasonally adjusted
    if s.startswith("SMU"): return "nsa"  # not seasonally adjusted
    # fallback to csv seasonal if needed elsewhere
    return "nsa"

def metric_id_for_seasonal(sfx: str) -> str:
    return "ces_total_nonfarm_sa" if (sfx or "").lower()=="sa" else "ces_total_nonfarm_nsa"

def load_expected_from_generated(gen_path: str):
    """
    Build expected metric_id set per geo_id from ces_series.generated.csv.
    This keeps expectations aligned with whatever you enabled in the generator.
    """
    expected = defaultdict(set)  # geo_id -> {metric_id,...}
    if not os.path.exists(gen_path):
        return expected

    with open(gen_path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            geo = (r.get("geo_id") or "").strip()
            sid = (r.get("series_id") or "").strip()
            if not geo or not sid: 
                continue
            expected[geo].add(metric_id_for_seasonal(sfx_from_sid(sid)))
    return expected

def main():
    con = duckdb.connect(DB_PATH)

    # quick existence check
    total = con.execute("""
        SELECT COUNT(*) n FROM fact_timeseries WHERE metric_id LIKE 'ces_%'
    """).fetchone()[0]
    if int(total) == 0:
        print("[ces:transform] No CES facts found yet — nothing to validate.")
        con.close()
        sys.exit(0)

    expected = load_expected_from_generated(GEN_PATH)

    # what's present?
    present = con.execute("""
        SELECT geo_id, metric_id, MIN(date) AS first, MAX(date) AS last, COUNT(*) AS n
        FROM fact_timeseries
        WHERE metric_id LIKE 'ces_%'
        GROUP BY 1,2
    """).fetchdf()

    # per-geo coverage
    missing = []
    for geo, exp_set in expected.items():
        have = set(present[present["geo_id"] == geo]["metric_id"].tolist())
        miss = sorted(exp_set - have)
        if miss:
            missing.append((geo, miss))

    # continuity sanity (optional): flag years that don’t have full 12 months except
    # the first/last year of each series (partial current year allowed).
    continuity_notes = []
    for _, row in present.iterrows():
        geo, metric, first, last, n = row["geo_id"], row["metric_id"], row["first"], row["last"], int(row["n"])
        # rough expected month count (inclusive month span):
        months = (last.year - first.year) * 12 + (last.month - first.month) + 1
        # allow some tolerance for endpoints; warn if it’s way off
        if months - n >= 3:  # more than a couple missing months
            continuity_notes.append((geo, metric, first, last, n, months))

    if not missing and not continuity_notes:
        print("[ces:transform] OK — CES facts look good. Summary:")
        print(con.execute("""
            SELECT metric_id,
                   MIN(date) AS first,
                   MAX(date) AS last,
                   COUNT(*)  AS rows
            FROM fact_timeseries
            WHERE metric_id LIKE 'ces_%'
            GROUP BY 1
            ORDER BY 1
        """).fetchdf())
        con.close()
        sys.exit(0)

    if missing:
        print("\n[ces:transform] WARNING — Missing expected CES series detected:")
        for geo, miss in missing:
            print(f"  - {geo}: missing {len(miss)} metric_id(s): {', '.join(miss)}")

    if continuity_notes:
        print("\n[ces:transform] NOTE — Possible continuity gaps (rows << expected months):")
        for geo, metric, first, last, n, months in continuity_notes:
            print(f"  - {geo} / {metric}: {n} rows vs ~{months} months between {first}–{last}")

    # helpful hint
    print("\nHint: ensure expected series are enabled in config/geo_manifest.csv, "
          "regenerate with ingest/ces_expand_spec.py, then re-run ces_api_bulk.py.")

    con.close()
    sys.exit(2 if STRICT else 0)

if __name__ == "__main__":
    main()
