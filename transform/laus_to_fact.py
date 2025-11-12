# transform/laus_to_fact.py
import os, sys, csv, duckdb
from collections import defaultdict

DB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")

CFG_PATHS = ("config/laus_series.generated.csv", "config/laus_series.csv")
def _pick_cfg_path():
    for p in CFG_PATHS:
        if os.path.exists(p):
            return p
    return None

STRICT = os.getenv("LAUS_STRICT", "0") not in ("0", "", "false", "False", "no", "No")

BASE_NAMES = {
    "003": "laus_unemployment_rate",
    "004": "laus_unemployment",
    "005": "laus_employment",
    "006": "laus_labor_force",
}


def _expect_sa_for_geo(geo_id: str) -> bool:
    # Treat *_state as states; everything else is sub-state (city/county/MSA/etc.)
    return str(geo_id or "").endswith("_state")



def tail(sid: str) -> str:
    sid = (sid or "").strip()
    return sid[-3:] if len(sid) >= 3 else ""

def sfx_from_sid(sid: str) -> str:
    sid = (sid or "").upper()
    if sid.startswith("LASST"): return "sa"
    if sid.startswith("LAUST"): return "nsa"
    return "nsa"



def _geos_from_cfg(cfg_path: str) -> list[str]:
    geos = set()
    with open(cfg_path, newline="") as f:
        for r in csv.DictReader(f):
            if not r:
                continue
            g = (r.get("geo_id") or "").strip()
            sid = (r.get("series_id") or "").strip()
            if g and sid and not g.startswith("#") and not sid.startswith("#"):
                geos.add(g)
    return sorted(geos)



def load_expected_metric_ids():
    """Read config/laus_series.csv and produce the set of expected metric_ids per geo."""
    expected = defaultdict(set)  # geo_id -> {metric_id,...}
    if not os.path.exists(CFG_PATH):
        return expected
    with open(CFG_PATH, newline="") as f:
        for r in csv.DictReader(f):
            if not r: continue
            geo = (r.get("geo_id") or "").strip()
            sid = (r.get("series_id") or "").strip()
            if not geo or not sid or geo.startswith("#") or sid.startswith("#"):
                continue
            base = BASE_NAMES.get(tail(sid), (r.get("metric_base") or "").strip())
            if base and not base.startswith("laus_"):
                base = "laus_" + base
            base = base or "laus_unemployment_rate"
            sfx  = sfx_from_sid(sid)
            expected[geo].add(f"{base}_{sfx}")
    return expected

def main():
    con = duckdb.connect(DB_PATH)

    # Quick existence check
    total = con.execute("""
        SELECT COUNT(*) n FROM fact_timeseries WHERE metric_id LIKE 'laus_%'
    """).fetchdf().loc[0, "n"]
    if int(total) == 0:
        print("[laus:transform] No LAUS facts found yet — nothing to transform.")
        con.close()
        sys.exit(0)

    # Build expectations per-geo, based on simple rule:
    #   - *_state => needs SA + NSA for all four bases
    #   - everything else => NSA only
    BASES = [
        "laus_employment",
        "laus_labor_force",
        "laus_unemployment",
        "laus_unemployment_rate",
    ]
    
    cfg_path = _pick_cfg_path()
    if not cfg_path:
        print("[laus:transform] No config CSV found (looked for generated and hand CSV).")
        con.close()
        sys.exit(0)
    
    geos = _geos_from_cfg(cfg_path)
    
    expected = {}  # dict[geo_id] -> set(metric_ids)
    for geo_id in geos:
        if _expect_sa_for_geo(geo_id):
            exp = [f"{b}_nsa" for b in BASES] + [f"{b}_sa" for b in BASES]
        else:
            exp = [f"{b}_nsa" for b in BASES]
        expected[geo_id] = set(exp)

    # What’s actually present?
    present = con.execute("""
        SELECT geo_id, metric_id, MIN(date) AS first, MAX(date) AS last, COUNT(*) AS n
        FROM fact_timeseries
        WHERE metric_id LIKE 'laus_%'
        GROUP BY 1,2
    """).fetchdf()

    # Validate per-geo coverage
    missing = []
    for geo, exp_set in expected.items():
        have = set(present[present["geo_id"] == geo]["metric_id"].tolist())
        miss = sorted(exp_set - have)
        if miss:
            missing.append((geo, miss))

    if not missing:
        print("[laus:transform] OK — LAUS facts already loaded. Summary:")
        print(con.execute("""
            SELECT metric_id,
                   MIN(date) AS first,
                   MAX(date) AS last,
                   COUNT(*)  AS rows
            FROM fact_timeseries
            WHERE metric_id LIKE 'laus_%'
            GROUP BY 1
            ORDER BY 1
        """).fetchdf())
        con.close()
        sys.exit(0)

    # Report gaps (don’t auto-reingest)
    print("\n[laus:transform] WARNING — Missing expected LAUS series detected:")
    for geo, miss in missing:
        print(f"  - {geo}: missing {len(miss)} metric_id(s): {', '.join(miss)}")

    print("\nHint: ensure corresponding series_id rows exist in config/laus_series.csv, then run:")
    print("    make ingest_bls && make transform_bls")

    con.close()
    if STRICT:
        # Fail the pipeline to make gaps visible in CI
        sys.exit(2)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()
