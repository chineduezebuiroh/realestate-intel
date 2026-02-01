from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from collections import Counter

import numpy as np
import pandas as pd

from forecast.selection.scoring import ScoredCandidate

# ----------------------------
# Utilities
# ----------------------------

def _source_from_feature_id(fid: str) -> str:
    try:
        return str(fid).split("__")[-1]
    except Exception:
        return ""

def _is_redfin_source(src: str) -> bool:
    return str(src).lower().startswith("redfin")

def _safe_int(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0

def _top_counter(items: List[Any], n: int = 15) -> List[Tuple[str, int]]:
    c = Counter([str(x) for x in items])
    return [(k, int(v)) for k, v in c.most_common(n)]

# ----------------------------
# Stage summaries
# ----------------------------

@dataclass(frozen=True)
class SelectorStage1Summary:
    anchor: str
    max_base_series: int
    picked_base_series_n: int
    picked_sources_top: List[Tuple[str, int]]
    picked_metrics_top: List[Tuple[str, int]]
    picked_metric_pt_top: List[Tuple[str, int]]
    bucket_counts: List[Tuple[str, int]]
    category_counts: List[Tuple[str, int]]
    redfin_tier_quota: Optional[Dict[int, int]]
    redfin_tier_used: Optional[Dict[int, int]]

@dataclass(frozen=True)
class SelectorFinalKSummary:
    K_requested: int
    K_written: int
    min_non_redfin: int
    final_redfin_n: int
    final_non_redfin_n: int
    final_sources_top: List[Tuple[str, int]]
    displaced_n: int
    added_n: int
    displaced_sources_top: List[Tuple[str, int]]
    added_sources_top: List[Tuple[str, int]]

@dataclass(frozen=True)
class SelectorRunSummary:
    batch_id: str
    artifact_root: str
    out_parquet: str
    out_json: str
    target: Dict[str, str]
    seed: int
    data_asof_requested: Optional[str]
    data_asof_effective: str
    asof_clamp_reason: Optional[dict]
    feature_set_sha256: str
    stage1: SelectorStage1Summary
    final_k: SelectorFinalKSummary

def build_stage1_summary(
    *,
    anchor: str,
    picked: List[ScoredCandidate],
    max_base_series: int,
    bucket_of,
    redfin_tier_quota: Optional[Dict[int, int]] = None,
    redfin_tier_used: Optional[Dict[int, int]] = None,
) -> SelectorStage1Summary:
    specs = [it.spec for it in picked]

    sources = [(getattr(s, "source_id", None) or "unknown") for s in specs]
    metrics = [getattr(s, "metric_id", "") for s in specs]
    metric_pt = [(f"{getattr(s, 'metric_id', '')}__pt={getattr(s, 'property_type_id', '')}") for s in specs]
    buckets = [bucket_of(s) for s in specs]
    cats = [(getattr(s, "category", None) or "uncategorized").lower() for s in specs]

    return SelectorStage1Summary(
        anchor=anchor,
        max_base_series=_safe_int(max_base_series),
        picked_base_series_n=_safe_int(len(picked)),
        picked_sources_top=_top_counter(sources, 15),
        picked_metrics_top=_top_counter(metrics, 15),
        picked_metric_pt_top=_top_counter(metric_pt, 15),
        bucket_counts=_top_counter(buckets, 20),
        category_counts=_top_counter(cats, 20),
        redfin_tier_quota=redfin_tier_quota,
        redfin_tier_used=redfin_tier_used,
    )

def build_final_k_summary(
    *,
    fi_all: pd.DataFrame,
    fi_sel: pd.DataFrame,
    K_requested: int,
    min_non_redfin: int,
) -> SelectorFinalKSummary:
    # baseline = top-K by importance from fi_all (importance>0 already upstream)
    baseline = fi_all.head(int(K_requested)).copy()

    base_ids = set(baseline["feature_id"].astype(str).tolist())
    final_ids = set(fi_sel["feature_id"].astype(str).tolist())

    removed = sorted(list(base_ids - final_ids))
    added = sorted(list(final_ids - base_ids))

    sel_sources = fi_sel["feature_id"].astype(str).apply(_source_from_feature_id)
    sel_is_redfin = sel_sources.apply(_is_redfin_source)

    n_sel = int(len(fi_sel))
    n_sel_red = int(sel_is_redfin.sum())
    n_sel_non = int(n_sel - n_sel_red)

    return SelectorFinalKSummary(
        K_requested=_safe_int(K_requested),
        K_written=_safe_int(len(fi_sel)),
        min_non_redfin=_safe_int(min_non_redfin),
        final_redfin_n=_safe_int(n_sel_red),
        final_non_redfin_n=_safe_int(n_sel_non),
        final_sources_top=_top_counter(list(sel_sources), 15),
        displaced_n=_safe_int(len(removed)),
        added_n=_safe_int(len(added)),
        displaced_sources_top=_top_counter([_source_from_feature_id(x) for x in removed], 10),
        added_sources_top=_top_counter([_source_from_feature_id(x) for x in added], 10),
    )

def write_selector_summary(path: Path, payload: SelectorRunSummary) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(asdict(payload), f, indent=2, sort_keys=True)
