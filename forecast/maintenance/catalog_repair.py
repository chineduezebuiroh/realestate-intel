# forecast/catalog_repair.py
from __future__ import annotations

from typing import Dict, Tuple
from forecast.db_forecast import get_connection

PLACEHOLDER = "__UNCLASSIFIED__"

def load_redfin_catalog() -> Dict[str, Tuple[str, str, str]]:
    """
    Returns {metric_id: (name, unit, category)} for BASE metrics only.
    Pulls from transform.redfin_to_fact_v2.COL_MAP (authoritative).
    """
    # IMPORTANT: this assumes you updated COL_MAP to:
    #   canonical_metric_id -> (name, unit, category)
    from transform.redfin_to_fact_v2 import COL_MAP  # type: ignore
    out: Dict[str, Tuple[str, str, str]] = {}
    for metric_id, (name, unit, cat) in COL_MAP.items():
        out[str(metric_id)] = (str(name), str(unit), str(cat))
    return out

def upsert_dim_metric(metric_id: str, name: str, frequency: str, unit: str, category: str):
    con = get_connection()
    con.execute("""
        INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(metric_id) DO UPDATE SET
          name=excluded.name,
          frequency=excluded.frequency,
          unit=excluded.unit,
          category=excluded.category
    """, [metric_id, name, frequency, unit, category])
    con.close()

def repair_redfin_and_derivatives():
    base = load_redfin_catalog()

    # 1) Base metrics
    for mid, (name, unit, cat) in base.items():
        upsert_dim_metric(
            metric_id=mid,
            name=name,
            frequency="monthly",
            unit=unit,
            category=cat,
        )

    # 2) Deterministic derived metrics
    for mid, (name, unit, cat) in base.items():
        upsert_dim_metric(f"{mid}_mom", f"{name} (MoM)", "monthly", unit, cat)
        upsert_dim_metric(f"{mid}_yoy", f"{name} (YoY)", "monthly", unit, cat)

def repair_census_bp_placeholders():
    # Minimal deterministic mapping: category=census, unit=counts/unknown; name is safe fallback
    # You can later make this authoritative by importing from your census BP transform.
    con = get_connection()
    rows = con.execute("""
      SELECT metric_id
      FROM dim_metric
      WHERE category = ?
        AND LOWER(metric_id) LIKE 'census_bp_%'
    """, [PLACEHOLDER]).fetchall()
    con.close()

    for (mid,) in rows:
        # crude but not "subjective": it's a placeholder name; category is still placeholder until census catalog is wired
        # If you already have a census BP transform catalog, we should import it instead of this.
        upsert_dim_metric(
            metric_id=mid,
            name=mid,
            frequency="monthly",
            unit="unknown",
            category="census",
        )

def assert_no_unclassified():
    con = get_connection()
    n_bad = con.execute("SELECT COUNT(*) FROM dim_metric WHERE category = ?", [PLACEHOLDER]).fetchone()[0]
    sample = con.execute("""
      SELECT metric_id FROM dim_metric WHERE category = ?
      ORDER BY metric_id
      LIMIT 25
    """, [PLACEHOLDER]).fetchall()
    con.close()

    if n_bad:
        raise SystemExit(f"[catalog_repair] FAIL: {n_bad} metrics still {PLACEHOLDER}. Example: {[m for (m,) in sample]}")
    print("[catalog_repair] PASS: no unclassified metrics remain.")

def main():
    repair_redfin_and_derivatives()
    repair_census_bp_placeholders()
    assert_no_unclassified()

if __name__ == "__main__":
    main()
