from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Set, Tuple, Optional

from forecast.core.db_forecast import get_connection

@dataclass(frozen=True)
class Catalog:
    metric_category: Dict[str, str]          # metric_id -> category
    property_type_name: Dict[str, str]       # property_type_id -> name
    property_type_group: Dict[str, str]      # property_type_id -> group

def load_catalog() -> Catalog:
    con = get_connection()
    try:
        m = con.execute("SELECT metric_id, category FROM dim_metric").fetchall()
        p = con.execute("SELECT property_type_id, name, \"group\" FROM dim_property_type").fetchall()
    finally:
        con.close()

    metric_category = {metric_id: (cat or "uncategorized") for metric_id, cat in m}
    property_type_name = {ptid: (name or "") for ptid, name, _grp in p}
    property_type_group = {ptid: (_grp or "") for ptid, _name, _grp in p}
    return Catalog(metric_category=metric_category,
                   property_type_name=property_type_name,
                   property_type_group=property_type_group)

def metric_family(metric_id: str, catalog: Catalog) -> str:
    return (catalog.metric_category.get(metric_id) or "uncategorized").strip().lower()

def property_type_ids_matching(
    *,
    catalog: Catalog,
    name_contains: Optional[Tuple[str, ...]] = None,
    group_contains: Optional[Tuple[str, ...]] = None,
) -> Set[str]:
    out: Set[str] = set()

    if name_contains:
        needles = tuple(s.lower() for s in name_contains)
        for ptid, name in catalog.property_type_name.items():
            if any(n in (name or "").lower() for n in needles):
                out.add(ptid)

    if group_contains:
        needles = tuple(s.lower() for s in group_contains)
        for ptid, grp in catalog.property_type_group.items():
            if any(n in (grp or "").lower() for n in needles):
                out.add(ptid)

    return out
