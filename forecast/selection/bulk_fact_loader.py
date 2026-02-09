from __future__ import annotations
# forecast/selection/bulk_fact_loader.py

from datetime import date
from typing import Dict, Iterable, List, Optional, Tuple

import duckdb
import pandas as pd

from forecast.core.asof import normalize_month_end
from forecast.features.fact_loader import get_connection

Key = Tuple[str, str, str, Optional[date]]  # (metric_id, geo_id, pt_id, effective_asof)


def _canon_pt(pt: Optional[str]) -> str:
    return pt if pt is not None else "all"


def _canon_asof(d: Optional[date]) -> Optional[date]:
    if d is None:
        return None
    return normalize_month_end(d)


def load_series_many_from_fact(
    *,
    requests: List[Tuple[str, str, Optional[str], Optional[date]]],
) -> Dict[Key, pd.Series]:
    """
    Bulk load many (metric, geo, pt, effective_asof) series in ONE DuckDB query.

    Semantics match load_series_from_fact():
      - property_type_id None -> 'all'
      - effective_asof filters by (effective_asof IS NULL OR date <= effective_asof)
      - DOES NOT filter on source_id (consistent with current loader)
    """
    if not requests:
        return {}

    rows = []
    for metric_id, geo_id, pt_id, effective_asof in requests:
        rows.append(
            {
                "metric_id": str(metric_id),
                "geo_id": str(geo_id),
                "property_type_id": _canon_pt(pt_id),
                "effective_asof": _canon_asof(effective_asof),
            }
        )

    req_df = pd.DataFrame(rows).drop_duplicates()

    con = get_connection()
    try:
        # Register as a DuckDB view and join (fast + avoids huge IN lists)
        con.register("req", req_df)

        sql = """
        SELECT
            r.metric_id,
            r.geo_id,
            r.property_type_id,
            r.effective_asof,
            f.date,
            f.value
        FROM req r
        JOIN fact_timeseries f
          ON f.metric_id = r.metric_id
         AND f.geo_id = r.geo_id
         AND f.property_type_id = r.property_type_id
        WHERE (r.effective_asof IS NULL OR f.date <= r.effective_asof)
        ORDER BY r.metric_id, r.geo_id, r.property_type_id, r.effective_asof, f.date
        """
        df = con.execute(sql).fetchdf()
    finally:
        con.close()

    out: Dict[Key, pd.Series] = {}

    if df.empty:
        return out

    # Group into series
    for (metric_id, geo_id, pt_id, eff_asof), g in df.groupby(
        ["metric_id", "geo_id", "property_type_id", "effective_asof"], dropna=False
    ):
        s = g.set_index("date")["value"].astype(float)
        # DuckDB returns NaT for NULL effective_asof; normalize key to None
        eff_key = None if pd.isna(eff_asof) else eff_asof
        out[(metric_id, geo_id, pt_id, eff_key)] = s

    return out
