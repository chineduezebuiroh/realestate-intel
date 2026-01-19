# forecast/asof_policy.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, Literal, Optional

import duckdb


AsOfPolicy = Literal["global_min", "per_source"]


@dataclass(frozen=True)
class AsOfResolution:
    """
    - global_asof: used for the "global_min" strategy (and as a fallback).
    - asof_by_source: used for the "per_source" strategy (and optional future use).
    """
    global_asof: Optional[date]
    asof_by_source: Dict[str, date]


def load_source_max_dates(
    con: duckdb.DuckDBPyConnection,
    *,
    target,
    feature_specs: Optional[Iterable] = None,
) -> Dict[str, date]:
    """
    Return {source_id: max(date)} for sources relevant to this run.
    If feature_specs is None or has no source_id, returns max dates for ALL sources.
    """
    sources = set()
    if feature_specs:
        for s in feature_specs:
            sid = getattr(s, "source_id", None)
            if sid:
                sources.add(str(sid))

    if sources:
        placeholders = ",".join(["?"] * len(sources))
        sql = f"""
            SELECT source_id, MAX(date) AS max_date
            FROM fact_timeseries
            WHERE source_id IN ({placeholders})
            GROUP BY 1
        """
        params = list(sorted(sources))
        df = con.execute(sql, params).fetchdf()
    else:
        df = con.execute("""
            SELECT source_id, MAX(date) AS max_date
            FROM fact_timeseries
            GROUP BY 1
        """).fetchdf()

    out: Dict[str, date] = {}
    for _, row in df.iterrows():
        sid = row["source_id"]
        mx = row["max_date"]
        if sid and mx is not None:
            out[str(sid)] = mx if isinstance(mx, date) else mx.date()
    return out



def get_source_max_dates(
    con: duckdb.DuckDBPyConnection,
    *,
    source_ids: Iterable[str],
    metric_like: Optional[str] = None,
) -> Dict[str, date]:
    """
    Returns {source_id: max(date)} for each requested source_id, restricted to fact_timeseries.
    If metric_like is provided, applies: metric_id LIKE <metric_like>.
    """
    srcs = [s for s in (source_ids or []) if s]
    if not srcs:
        return {}

    # DuckDB doesn't support binding a list directly into IN (?) in all contexts cleanly,
    # so build a safe placeholder list.
    placeholders = ",".join(["?"] * len(srcs))

    where_metric = ""
    params = list(srcs)
    if metric_like:
        where_metric = " AND metric_id LIKE ?"
        params.append(metric_like)

    q = f"""
        SELECT source_id, MAX(date) AS max_date
        FROM fact_timeseries
        WHERE source_id IN ({placeholders})
          AND date IS NOT NULL
          {where_metric}
        GROUP BY 1
    """

    df = con.execute(q, params).fetchdf()
    out: Dict[str, date] = {}
    for _, r in df.iterrows():
        sid = r["source_id"]
        mx = r["max_date"]
        if sid and mx:
            out[str(sid)] = mx
    return out


def resolve_asof(
    policy: AsOfPolicy,
    source_max_dates: Dict[str, date],
) -> AsOfResolution:
    """
    policy="global_min": global_asof = min(source_max_dates.values())
    policy="per_source": global_asof still computed (min) as a conservative fallback,
                         but consumers should primarily use asof_by_source.
    """
    if source_max_dates:
        global_asof = min(source_max_dates.values())
        # Coerce pandas.Timestamp -> datetime.date
        if hasattr(global_asof, "date"):
            global_asof = global_asof.date()
    else:
        global_asof = None

    if policy == "global_min":
        return AsOfResolution(global_asof=global_asof, asof_by_source={})

    # per_source (also coerce any Timestamp values defensively)
    asof_by_source = {}
    for k, v in source_max_dates.items():
        if v is None:
            continue
        asof_by_source[k] = v.date() if hasattr(v, "date") else v

    return AsOfResolution(global_asof=global_asof, asof_by_source=asof_by_source)



def resolve_targetspec_asof(
    con: duckdb.DuckDBPyConnection,
    *,
    exog_source_ids: list[str],
    policy: AsOfPolicy,
    explicit_data_asof: Optional[date] = None,
) -> AsOfResolution:
    source_max_dates = get_source_max_dates(con, source_ids=exog_source_ids)
    asof_res = resolve_asof(policy, source_max_dates)

    # explicit CLI/user value wins
    if explicit_data_asof is not None:
        return AsOfResolution(global_asof=explicit_data_asof, asof_by_source=asof_res.asof_by_source)

    return asof_res
