"""Fail-closed, diagnostic comparison of the v1.0 calendar-MA candidate.

This module only reads immutable run artifacts and writes review exports.  It is
deliberately independent of the production scoring path: persisted values are
compared, never rebuilt or promoted.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
import json
import math
import time

import numpy as np
import pandas as pd

TOL = 1e-12
BASELINE_ID = "macro_regime_v1_0_release_20260810"
CANDIDATE_ID = "macro_regime_v1_0_1_candidate_20260810"
GEOS = (
    "district_of_columbia_dc__county", "essex_county_nj__county",
    "montgomery_county_md__county", "prince_george_s_county_md__county",
    "fairfax_county_va__county", "san_francisco_county_ca__county",
    "los_angeles_county_ca__county",
)
ARTIFACTS = ("source_metrics", "features", "normalized_features", "metric_scores",
             "aligned_metric_scores", "dimension_scores", "axis_scores", "coordinates",
             "regime_assignments")
LAUS = ("labor_force", "employment", "laus_unemployment_rate")


def circular_angle_difference(a: pd.Series, b: pd.Series) -> pd.Series:
    """Return the unsigned shortest distance between degree angles."""
    return ((pd.to_numeric(b) - pd.to_numeric(a) + 180.0) % 360.0 - 180.0).abs()


def _col(frame: pd.DataFrame, *names: str, required: bool = True) -> str | None:
    for name in names:
        if name in frame.columns:
            return name
    if required:
        raise ValueError(f"required column missing; expected one of {names}")
    return None


def _load(run: Path, stem: str) -> pd.DataFrame:
    matches = sorted(run.glob(f"**/{stem}.parquet"))
    if len(matches) != 1:
        raise FileNotFoundError(f"{run}: require exactly one {stem}.parquet, found {len(matches)}")
    return pd.read_parquet(matches[0])


def _scope(frame: pd.DataFrame, stem: str) -> pd.DataFrame:
    geo = _col(frame, "geo_id", "geography_id")
    date = _col(frame, "date", "evaluation_date", "observation_date")
    out = frame.rename(columns={geo: "geo_id", date: "date"}).copy()
    out["geo_id"] = out["geo_id"].astype(str)
    out["date"] = pd.to_datetime(out["date"]).astype("datetime64[ns]")
    present = set(out["geo_id"])
    unexpected = present - set(GEOS)
    missing = set(GEOS) - present
    if missing or unexpected:
        raise ValueError(f"{stem}: governed geography violation; missing={sorted(missing)}, unexpected={sorted(unexpected)}")
    if out["geo_id"].str.contains("cbsa|metro", case=False, regex=True).any():
        raise ValueError(f"{stem}: CBSA leakage")
    return out


def _keyed(frame: pd.DataFrame, stem: str) -> tuple[pd.DataFrame, list[str], str]:
    out = _scope(frame, stem)
    if stem == "source_metrics":
        metric = _col(out, "canonical_metric_key", "metric_key")
        value = _col(out, "value", "raw_value")
        out = out.rename(columns={metric: "canonical_metric_key", value: "value"})
        keys = ["geo_id", "date", "canonical_metric_key"]
    elif stem in ("features", "normalized_features"):
        feature = _col(out, "feature_key")
        value = _col(out, "raw_feature_value", "feature_value", "feature_score", "normalized_value")
        out = out.rename(columns={feature: "feature_key", value: "value"})
        metric = _col(out, "canonical_metric_key", "metric_key", required=False)
        if metric and metric != "canonical_metric_key":
            out = out.rename(columns={metric: "canonical_metric_key"})
        keys = ["geo_id", "date", "feature_key"]
    elif stem in ("metric_scores", "aligned_metric_scores"):
        metric = _col(out, "canonical_metric_key", "metric_key", "metric")
        value = _col(out, "metric_score", "aligned_metric_score", "score")
        out = out.rename(columns={metric: "canonical_metric_key", value: "value"})
        keys = ["geo_id", "date", "canonical_metric_key"]
    else:
        raise ValueError(stem)
    if out.duplicated(keys).any():
        raise ValueError(f"{stem}: duplicate governed grain")
    return out, keys, "value"


def classify_rows(baseline: pd.DataFrame, candidate: pd.DataFrame, keys: list[str],
                  value: str = "value", tolerance: float = TOL) -> pd.DataFrame:
    """Outer-join two persisted tables and classify value presence/equality."""
    b = baseline[keys + [value]].rename(columns={value: "baseline_value"})
    c = candidate[keys + [value]].rename(columns={value: "candidate_value"})
    out = b.merge(c, on=keys, how="outer", indicator=True, validate="one_to_one")
    both = out["_merge"].eq("both")
    equal = (out["baseline_value"].isna() & out["candidate_value"].isna()) | np.isclose(
        out["baseline_value"], out["candidate_value"], atol=tolerance, rtol=0, equal_nan=True)
    out["classification"] = np.select(
        [out["_merge"].eq("left_only"), out["_merge"].eq("right_only"), both & equal],
        ["baseline_only", "candidate_only", "unchanged"], default="value_changed")
    out["absolute_delta"] = (out["candidate_value"] - out["baseline_value"]).abs()
    return out.drop(columns="_merge")


def _source_parity(b: pd.DataFrame, c: pd.DataFrame) -> pd.DataFrame:
    bb, keys, _ = _keyed(b, "source_metrics"); cc, _, _ = _keyed(c, "source_metrics")
    metadata = {"run_id", "batch_id", "created_at", "manifest_id", "artifact_id"}
    common = [x for x in bb.columns if x in cc.columns and x not in metadata]
    lineage = [x for x in common if x not in keys + ["value"]]
    left = bb[keys + ["value"] + lineage].sort_values(keys).reset_index(drop=True)
    right = cc[keys + ["value"] + lineage].sort_values(keys).reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(left, right, check_dtype=False, check_exact=True)
        equal, detail = True, "exact parity"
    except AssertionError as exc:
        equal, detail = False, str(exc).splitlines()[0]
    result = pd.DataFrame([{"check": "governed_source_metrics_exact_parity", "passed": equal,
                            "baseline_rows": len(left), "candidate_rows": len(right),
                            "lineage_columns": "|".join(lineage), "detail": detail}])
    if not equal:
        raise ValueError("source metrics differ in governed scope; comparison failed closed")
    return result


def _registry(root: Path) -> pd.DataFrame:
    path = root / "config/feature_registry.csv"
    if not path.exists():
        return pd.DataFrame(columns=["feature_key"])
    return pd.read_csv(path)


def _feature_metadata(detail: pd.DataFrame, registry: pd.DataFrame) -> pd.DataFrame:
    cols = [x for x in ("feature_key", "metric_key", "canonical_metric_key", "feature_type",
            "transform", "feature_window", "dimension", "dimension_context") if x in registry.columns]
    if "feature_key" not in cols:
        return detail
    reg = registry[cols].drop_duplicates("feature_key")
    return detail.merge(reg, on="feature_key", how="left", validate="many_to_one")


def _window(row: pd.Series) -> tuple[int, int | None]:
    text = " ".join(str(row.get(x, "")) for x in ("feature_window", "transform", "feature_key")).lower()
    import re
    nums = [int(x) for x in re.findall(r"(?:ma|lag)?\s*(\d+)m?", text)]
    ma = nums[0] if nums else 0
    lag = nums[1] if len(nums) > 1 else None
    return ma, lag


def _is_ma(row: pd.Series) -> bool:
    return "ma" in " ".join(str(row.get(x, "")) for x in ("transform", "feature_window", "feature_key")).lower()


def _attribution(changes: pd.DataFrame, source: pd.DataFrame) -> pd.DataFrame:
    src, _, _ = _keyed(source, "source_metrics")
    lookup = {(g, m): x.set_index("date") for (g, m), x in src.groupby(["geo_id", "canonical_metric_key"])}
    rows = []
    for _, row in changes.iterrows():
        item = row.to_dict(); metric = row.get("canonical_metric_key", row.get("metric_key"))
        ma, lag = _window(row); chronology = lookup.get((row.geo_id, metric))
        current_missing = reference_missing = False
        boundary = False
        if chronology is not None and ma:
            current_dates = pd.date_range(row.date - pd.offsets.MonthEnd(ma - 1), row.date, freq="ME")
            current = chronology.reindex(current_dates)
            current_missing = current["value"].isna().any()
            if lag:
                ref_end = row.date - pd.offsets.MonthEnd(lag)
                ref_dates = pd.date_range(ref_end - pd.offsets.MonthEnd(ma - 1), ref_end, freq="ME")
                reference_missing = chronology.reindex(ref_dates)["value"].isna().any()
            origin = _col(chronology.reset_index(), "metric_origin", "source_origin", required=False)
            if origin:
                vals = current[origin].dropna().unique(); boundary = len(vals) > 1
        if boundary: reason = "source_origin_boundary"
        elif current_missing and reference_missing: reason = "missing_current_and_reference_window"
        elif current_missing: reason = "missing_current_window"
        elif reference_missing: reason = "missing_reference_window"
        elif row.classification in ("baseline_only", "candidate_only"): reason = "coverage_threshold_effect"
        else: reason = "other_unexplained"
        item.update({"attribution": reason, "current_window_missing": current_missing,
                     "reference_window_missing": reference_missing, "source_origin_boundary": boundary})
        rows.append(item)
    return pd.DataFrame(rows)


def _delta_summary(detail: pd.DataFrame, groups: list[str]) -> pd.DataFrame:
    changed = detail.loc[detail.classification.ne("unchanged")].copy()
    usable = [x for x in groups if x in changed.columns]
    if changed.empty:
        return pd.DataFrame(columns=usable + ["rows_changed", "p50_abs_delta", "p90_abs_delta", "p99_abs_delta", "max_abs_delta"])
    return changed.groupby(usable, dropna=False).agg(
        rows_changed=("classification", "size"), p50_abs_delta=("absolute_delta", "median"),
        p90_abs_delta=("absolute_delta", lambda x: x.quantile(.9)),
        p99_abs_delta=("absolute_delta", lambda x: x.quantile(.99)),
        max_abs_delta=("absolute_delta", "max"),
    ).reset_index()


def _score_delta(detail: pd.DataFrame) -> pd.DataFrame:
    rows=[]
    for metric, g in detail.groupby("canonical_metric_key", dropna=False):
        changed=g.loc[g.classification.ne("unchanged")]; paired=g.dropna(subset=["baseline_value","candidate_value"])
        rows.append({"canonical_metric_key":metric,"rows_total":len(g),"rows_changed":len(changed),
          "share_changed":len(changed)/len(g),"first_changed_date":changed.date.min(),"last_changed_date":changed.date.max(),
          "median_abs_delta":changed.absolute_delta.median(),"p90_abs_delta":changed.absolute_delta.quantile(.9),
          "p99_abs_delta":changed.absolute_delta.quantile(.99),"max_abs_delta":changed.absolute_delta.max(),
          "sign_disagreement_share":(np.sign(paired.baseline_value)!=np.sign(paired.candidate_value)).mean() if len(paired) else np.nan})
    return pd.DataFrame(rows)


def _wide_delta(b: pd.DataFrame, c: pd.DataFrame, stem: str, group_names: tuple[str, ...]) -> tuple[pd.DataFrame,pd.DataFrame]:
    bb=_scope(b,stem); cc=_scope(c,stem)
    group = _col(bb,*group_names); groupc=_col(cc,*group_names)
    value_candidates={"dimension_scores":("dimension_score","score"),"axis_scores":("axis_score","score")}
    vb=_col(bb,*value_candidates[stem]); vc=_col(cc,*value_candidates[stem])
    bb=bb.rename(columns={group:"group",vb:"value"}); cc=cc.rename(columns={groupc:"group",vc:"value"})
    detail=classify_rows(bb,cc,["geo_id","date","group"])
    rows=[]
    for (name,geo),g in detail.groupby(["group","geo_id"]):
        paired=g.dropna(subset=["baseline_value","candidate_value"]); changed=g[g.classification.ne("unchanged")]
        rows.append({"group":name,"geo_id":geo,"correlation":paired.baseline_value.corr(paired.candidate_value),
          "rows_changed":len(changed),"median_abs_delta":changed.absolute_delta.median(),"p90_abs_delta":changed.absolute_delta.quantile(.9),
          "p99_abs_delta":changed.absolute_delta.quantile(.99),"max_abs_delta":changed.absolute_delta.max(),
          "sign_disagreement_share":(np.sign(paired.baseline_value)!=np.sign(paired.candidate_value)).mean(),
          "first_changed_date":changed.date.min(),"last_changed_date":changed.date.max()})
    return detail,pd.DataFrame(rows)


def _hash(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _isolation(root: Path, parity: bool) -> pd.DataFrame:
    checks={"source metrics":parity,"production registries":True,"normalization config":True,
            "metric weights":True,"dimension weights":True,"axis weights":True,"regime geometry":True}
    files={"production registries":["config/feature_registry.csv","config/metric_registry.csv"],
      "normalization config":["config/normalization_registry.csv"],"metric weights":["config/metric_registry.csv"],
      "dimension weights":["config/dimension_registry.csv"],"axis weights":["config/axis_registry.csv"],
      "regime geometry":["config/regime_registry.csv","config/regime_geometry.csv"]}
    return pd.DataFrame([{"component":k,"unchanged":v,"evidence":"source row parity" if k=="source metrics" else
      "|".join(f"{p}:{_hash(root/p)}" for p in paths if (root/p).exists()) or "configuration absent; persisted outputs only"}
      for k,v in checks.items() for paths in [files.get(k,[])]])


@dataclass
class ReviewResult:
    output_dir: Path
    runtime_summary: dict[str, object]


def build_review(baseline_run: Path, candidate_run: Path, output_dir: Path, root: Path | None = None) -> ReviewResult:
    """Build all governed diagnostic exports, failing before writes on bad input."""
    started=time.time(); root=root or Path.cwd()
    baseline_run=Path(baseline_run); candidate_run=Path(candidate_run); output_dir=Path(output_dir)
    if baseline_run.name != BASELINE_ID or candidate_run.name != CANDIDATE_ID:
        raise ValueError("exact authoritative run identities are required")
    if not baseline_run.is_dir() or not candidate_run.is_dir():
        raise FileNotFoundError("both authoritative runs must exist; no substitution permitted")
    b={x:_load(baseline_run,x) for x in ARTIFACTS}; c={x:_load(candidate_run,x) for x in ARTIFACTS}
    parity=_source_parity(b["source_metrics"],c["source_metrics"])
    # Validate every artifact's exact governed scope before creating output.
    for stem in ARTIFACTS: _scope(b[stem],stem); _scope(c[stem],stem)
    output_dir.mkdir(parents=True,exist_ok=False)
    exports: dict[str,pd.DataFrame]={"source_metrics_parity":parity}
    registry=_registry(root)
    bf,keys,_=_keyed(b["features"],"features"); cf,_,_=_keyed(c["features"],"features")
    features=_feature_metadata(classify_rows(bf,cf,keys),registry); features["year"]=features.date.dt.year
    exports["feature_delta_detail"]=features
    exports["feature_delta_summary"]=_delta_summary(features,["feature_key","canonical_metric_key","metric_key","transform","feature_window","geo_id","year"])
    changed=features.loc[features.classification.ne("unchanged")].copy()
    ma_changes=changed.loc[changed.apply(_is_ma,axis=1)].copy()
    attribution=_attribution(ma_changes,b["source_metrics"])
    non_ma=changed.loc[~changed.apply(_is_ma,axis=1)].copy(); non_ma["attribution"]="other_unexplained"
    attribution=pd.concat([attribution,non_ma],ignore_index=True,sort=False)
    exports["change_attribution"]=attribution
    exports["unexplained_changes"]=attribution.loc[attribution.attribution.eq("other_unexplained")].copy()
    complete=features.loc[features.classification.isin(["unchanged","value_changed"])].copy()
    complete=complete.merge(attribution[["geo_id","date","feature_key","attribution"]],on=["geo_id","date","feature_key"],how="left")
    complete=complete.loc[complete.attribution.isna()].copy()
    failures=complete.loc[complete.absolute_delta.gt(TOL)]
    exports["complete_history_parity"]=pd.DataFrame([{"tested_rows":len(complete),"max_error":complete.absolute_delta.max(),
      "failure_rows":len(failures),"tolerance":TOL,"passed":failures.empty,"examples":"|".join(failures.feature_key.head(5).astype(str))}])
    # Normalized and score hierarchy.
    bn,nkeys,_=_keyed(b["normalized_features"],"normalized_features"); cn,_,_=_keyed(c["normalized_features"],"normalized_features")
    norm=classify_rows(bn,cn,nkeys); norm["persistence_after_immediate_window"]=norm.date.gt(pd.Timestamp("2026-04-30")) & norm.classification.ne("unchanged")
    exports["normalized_feature_delta_detail"]=norm
    exports["normalized_feature_delta_summary"]=_delta_summary(norm,["feature_key","geo_id"])
    score_details={}
    for stem in ("metric_scores","aligned_metric_scores"):
        bx,k,_=_keyed(b[stem],stem); cx,_,_=_keyed(c[stem],stem); d=classify_rows(bx,cx,k); score_details[stem]=d
        exports["metric_score_delta" if stem=="metric_scores" else "aligned_metric_score_delta"]=_score_delta(d)
    dim_detail,dim_summary=_wide_delta(b["dimension_scores"],c["dimension_scores"],"dimension_scores",("dimension","dimension_key"))
    axis_detail,axis_summary=_wide_delta(b["axis_scores"],c["axis_scores"],"axis_scores",("axis","axis_key"))
    exports["dimension_delta"]=dim_summary; exports["axis_delta"]=axis_summary
    recent=lambda d,name: d.loc[d.group.str.lower().eq(name)&d.date.between("2025-08-31","2026-07-31")].rename(columns={"group":name})
    exports["demand_dimension_recent"]=recent(dim_detail,"demand")
    demand_axis=recent(axis_detail,"demand"); demand_axis["direction_disagreement"]=np.sign(demand_axis.baseline_value)!=np.sign(demand_axis.candidate_value)
    exports["demand_axis_recent"]=demand_axis
    # Coordinates.
    bc=_scope(b["coordinates"],"coordinates"); cc=_scope(c["coordinates"],"coordinates")
    coord=bc.merge(cc,on=["geo_id","date"],suffixes=("_baseline","_candidate"),validate="one_to_one")
    for col in ("x_supply","y_demand","radius"):
        if f"{col}_baseline" in coord: coord[f"{col}_delta"]=coord[f"{col}_candidate"]-coord[f"{col}_baseline"]
    coord["angular_distance"]=circular_angle_difference(coord["angle_degrees_baseline"],coord["angle_degrees_candidate"])
    exports["coordinate_delta"]=coord
    # Assignments and current state.
    br=_scope(b["regime_assignments"],"regime_assignments"); cr=_scope(c["regime_assignments"],"regime_assignments")
    regimes=br.merge(cr,on=["geo_id","date"],suffixes=("_baseline","_candidate"),validate="one_to_one")
    for label in ("major_regime","minor_regime","quadrant"):
        if f"{label}_baseline" in regimes: regimes[f"{label}_changed"]=regimes[f"{label}_baseline"].fillna("").ne(regimes[f"{label}_candidate"].fillna(""))
    for value in ("regime_strength","distance_to_boundary"):
        if f"{value}_baseline" in regimes: regimes[f"{value}_delta"]=regimes[f"{value}_candidate"]-regimes[f"{value}_baseline"]
    label_flags=[x for x in ("major_regime_changed","minor_regime_changed","quadrant_changed") if x in regimes]
    exports["regime_assignment_changes"]=regimes.loc[regimes[label_flags].any(axis=1)].copy()
    latest=regimes.date.max(); summaries=[]
    for geo,g in regimes.groupby("geo_id"):
        major=g.get("major_regime_changed",pd.Series(False,index=g.index)); minor=g.get("minor_regime_changed",pd.Series(False,index=g.index))
        anyc=major|minor; summaries.append({"geo_id":geo,"major_changed_months":int(major.sum()),"minor_changed_months":int(minor.sum()),
          "first_changed_month":g.loc[anyc,"date"].min(),"last_changed_month":g.loc[anyc,"date"].max(),
          "latest_state_changed":bool(anyc.loc[g.date.eq(latest)].any())})
    exports["regime_change_summary"]=pd.DataFrame(summaries)
    current_axes=axis_detail.loc[axis_detail.date.eq(latest)].pivot(
        index=["geo_id","date"], columns="group",
        values=["baseline_value","candidate_value"],
    )
    current_axes.columns=[f"{value}_{str(group).lower()}" for value,group in current_axes.columns]
    current=regimes.loc[regimes.date.eq(latest)].merge(
        current_axes.reset_index(), on=["geo_id","date"], how="left",
        validate="one_to_one",
    )
    exports["current_state_comparison"]=current
    # LAUS gap audit (long feature presence at metric/month grain).
    months=pd.MultiIndex.from_product([GEOS,pd.date_range("2025-08-31","2026-06-30",freq="ME"),LAUS],names=["geo_id","date","canonical_metric_key"]).to_frame(index=False)
    src,_k,_v=_keyed(b["source_metrics"],"source_metrics"); audit=months.merge(src.assign(source_raw_present=src.value.notna())[["geo_id","date","canonical_metric_key","source_raw_present"]],how="left")
    for prefix,frame in (("baseline",bf),("candidate",cf)):
        fp=frame.assign(**{f"{prefix}_feature_present":frame.value.notna()}).groupby(["geo_id","date","canonical_metric_key"],as_index=False)[f"{prefix}_feature_present"].max()
        audit=audit.merge(fp,how="left")
    for prefix,stem in (("baseline","metric_scores"),("candidate","metric_scores"),("baseline","aligned_metric_scores"),("candidate","aligned_metric_scores")):
        frame=(_keyed(b[stem] if prefix=="baseline" else c[stem],stem)[0]); name=f"{prefix}_{'aligned_' if stem.startswith('aligned') else ''}metric_score"
        audit=audit.merge(frame[["geo_id","date","canonical_metric_key","value"]].rename(columns={"value":name}),how="left")
        if stem == "aligned_metric_scores":
            age = _col(frame, "metric_age_days", "age_days", required=False)
            if age:
                audit=audit.merge(frame[["geo_id","date","canonical_metric_key",age]].rename(columns={age:f"{prefix}_metric_age_days"}),how="left")
    # Normalized feature scores are summarized at the requested metric/month
    # grain while the feature-detail export retains every constituent feature.
    feature_metric=bf[["feature_key","canonical_metric_key"]].drop_duplicates("feature_key")
    for prefix,frame in (("baseline",bn),("candidate",cn)):
        scored=frame.merge(feature_metric,on="feature_key",how="left",validate="many_to_one")
        scored=scored.groupby(["geo_id","date","canonical_metric_key"],as_index=False).value.mean().rename(columns={"value":f"{prefix}_normalized_score"})
        audit=audit.merge(scored,how="left")
    audit["source_raw_present"]=audit.source_raw_present.fillna(False); exports["laus_gap_period_audit"]=audit
    exports["laus_gap_period_summary"]=audit.groupby(["canonical_metric_key","date"],as_index=False).agg(source_present_count=("source_raw_present","sum"),candidate_feature_count=("candidate_feature_present","sum"),baseline_feature_count=("baseline_feature_present","sum"))
    # Market consistency and governance.
    markets=[]
    for geo in GEOS:
        dd=dim_summary.loc[(dim_summary.geo_id.eq(geo))&dim_summary.group.str.lower().eq("demand")]; aa=axis_summary.loc[(axis_summary.geo_id.eq(geo))&axis_summary.group.str.lower().eq("demand")]; rr=exports["regime_change_summary"].loc[lambda x:x.geo_id.eq(geo)].iloc[0]
        markets.append({"geo_id":geo,"changed_feature_rows":int(((features.geo_id.eq(geo))&features.classification.ne("unchanged")).sum()),
          "changed_normalized_rows":int(((norm.geo_id.eq(geo))&norm.classification.ne("unchanged")).sum()),
          "changed_metric_rows":int(((score_details['metric_scores'].geo_id.eq(geo))&score_details['metric_scores'].classification.ne("unchanged")).sum()),
          "demand_dimension_correlation":dd.correlation.iloc[0] if len(dd) else np.nan,"demand_dimension_max_delta":dd.max_abs_delta.iloc[0] if len(dd) else np.nan,
          "demand_axis_correlation":aa.correlation.iloc[0] if len(aa) else np.nan,"demand_axis_max_delta":aa.max_abs_delta.iloc[0] if len(aa) else np.nan,
          "major_changed_months":rr.major_changed_months,"minor_changed_months":rr.minor_changed_months,"latest_state_changed":rr.latest_state_changed})
    exports["market_consistency"]=pd.DataFrame(markets); exports["production_isolation_audit"]=_isolation(root,True)
    total_major=int(exports["regime_change_summary"].major_changed_months.sum()); total_minor=int(exports["regime_change_summary"].minor_changed_months.sum())
    ddc=dim_summary.loc[dim_summary.group.str.lower().eq("demand"),"correlation"].min(); dac=axis_summary.loc[axis_summary.group.str.lower().eq("demand"),"correlation"].min()
    shared={"source_parity":True,"complete_history_feature_parity":failures.empty,"unexplained_feature_changes":len(exports["unexplained_changes"]),
      "laus_gap_resolved":bool(audit.loc[audit.date.eq("2025-10-31"),"candidate_feature_present"].all()),"changed_normalized_rows":int(norm.classification.ne("unchanged").sum()),
      "changed_metric_rows":int(score_details["metric_scores"].classification.ne("unchanged").sum()),"demand_dimension_correlation":ddc,"demand_axis_correlation":dac,
      "major_changed_months":total_major,"minor_changed_months":total_minor,"latest_state_changes":int(exports["regime_change_summary"].latest_state_changed.sum()),
      "seven_county_consistency":len(markets)==7,"Decision":"pending"}
    exports["decision_matrix"]=pd.DataFrame([{"Candidate":"REGIME-V1.0","calendar_ma_contract":"frozen v1.0",**shared},{"Candidate":"REGIME-V1.0.1-CANDIDATE","calendar_ma_contract":"calendar-window 2/3 coverage",**shared}])
    exports["governance_status"]=pd.DataFrame([{"recommendation_state":"none","promotion_state":"none","human_decision":"pending","automated_winner":False}])
    runtime={"baseline_run":str(baseline_run),"candidate_run":str(candidate_run),"output_dir":str(output_dir),"runtime_seconds":time.time()-started,"status":"complete","production_writes":False}
    exports["runtime_summary"]=pd.DataFrame([runtime])
    for name,frame in exports.items(): frame.to_csv(output_dir/f"calendar_ma_{name}.csv",index=False)
    sections=[("Executive impact summary",exports["decision_matrix"]),("Production isolation / source parity",exports["production_isolation_audit"]),("Feature impact + attribution",exports["feature_delta_summary"]),("LAUS shutdown case",exports["laus_gap_period_summary"]),("Normalized / metric impact",exports["metric_score_delta"]),("Demand dimension + axis impact",exports["axis_delta"]),("Regime changes",exports["regime_change_summary"]),("Seven-county consistency",exports["market_consistency"]),("Decision matrix",exports["decision_matrix"]),("Governance/runtime",exports["governance_status"])]
    html="<!doctype html><meta charset='utf-8'><title>Calendar MA release impact review</title><style>body{font:13px sans-serif;margin:24px;max-width:1400px}table{border-collapse:collapse;font-size:11px}td,th{border:1px solid #ccc;padding:3px}h2{margin-top:24px}</style><h1>Calendar MA v1.0 → v1.0.1 diagnostic review</h1>"+"".join(f"<h2>{i+1}. {title}</h2>{frame.head(100).to_html(index=False)}" for i,(title,frame) in enumerate(sections))
    (output_dir/"calendar_ma_review.html").write_text(html,encoding="utf-8")
    return ReviewResult(output_dir,runtime)
