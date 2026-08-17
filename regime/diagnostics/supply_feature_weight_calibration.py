"""Supply Phase-2 bounded independent feature-weight calibration.

This diagnostic only reweights persisted normalized MA12 Supply features.  It
never constructs features, normalizes observations, or mutates registries.
"""
from __future__ import annotations

from pathlib import Path
import html
import numpy as np
import pandas as pd
from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points
from regime.diagnostics.correlation import safe_corr

from regime.diagnostics import price_feature_anatomy as canonical
from regime.diagnostics.supply_feature_anatomy import load_run as _load_phase1, resolve_contract

DC = canonical.DC
REVIEW_GEOS = canonical.REVIEW_GEOS
TARGET_METRICS = ("active_inventory", "permit_activity", "permit_intensity")
_dates, _metric_col, _periods = canonical._dates, canonical._metric_col, canonical._periods
_pool, _plot, _series_stats, _value_col = canonical._pool, canonical._plot, canonical._series_stats, canonical._value_col

PRODUCTION_FEATURE_WEIGHTS = {
    "active_inventory": (.50, .25, .25),
    "permit_activity": (.80, .10, .10),
    "permit_intensity": (.50, .25, .25),
}
EXPERIMENTS = {
    "active_inventory": {
        "I0": (.50, .25, .25), "I1": (.50, .20, .30),
        "I2": (.50, .15, .35), "I3": (.45, .15, .40),
        "I4": (.40, .15, .45), "I5": (.35, .15, .50),
    },
    "permit_activity": {
        "A0": (.80, .10, .10), "A1": (.80, .05, .15),
        "A2": (.75, .10, .15), "A3": (.75, .15, .10),
        "A4": (.70, .10, .20),
    },
    "permit_intensity": {
        "N0": (.50, .25, .25), "N1": (.50, .20, .30),
        "N2": (.50, .15, .35), "N3": (.45, .15, .40),
        "N4": (.40, .15, .45), "N5": (.35, .15, .50),
    },
}
POLICIES = {policy: weights for grid in EXPERIMENTS.values() for policy, weights in grid.items()}
POLICY_TARGET = {policy: metric for metric, grid in EXPERIMENTS.items() for policy in grid}
FEATURES = ("level", "short", "long")
PERIOD_NAMES = ("full_history", "2022_plus", "latest_36_months")
ADJACENT = (
    ("I0","I1"),("I1","I2"),("I2","I3"),("I3","I4"),("I4","I5"),
    ("A0","A1"),("A0","A2"),("A0","A3"),("A2","A4"),
    ("N0","N1"),("N1","N2"),("N2","N3"),("N3","N4"),("N4","N5"),
)
EXPORTS = (
 "scenario_registry","metric_chronology","feature_contributions","metric_statistics",
 "dimension_statistics","supply_axis_statistics","demand_axis_statistics","feature_reference_comparison",
 "raw_cycle_comparison","adjacent_comparisons","vs_p0","cross_metric_consistency",
 "by_county","period_sensitivity","evaluation_matrix","governance_status",
 "raw_cycle_chronology","turning_point_comparison","correlation_audit",
)

TURN_MATCH_WINDOW_MONTHS = 3

def load_run(run: Path) -> dict[str,pd.DataFrame]:
    out=_load_phase1(run)
    path=run/"axis_scores.parquet"
    if not path.is_file(): raise FileNotFoundError(f"authoritative run missing required axis_scores.parquet: {run}")
    out["axis_scores"]=pd.read_parquet(path)
    return out

def _extra_stats(s, dates):
    q=pd.DataFrame({"date":dates,"v":pd.to_numeric(s,errors="coerce")}).dropna().sort_values("date"); d=q.v.diff(); sign=np.sign(d).replace(0,np.nan); state=np.sign(q.v).replace(0,np.nan).ffill()
    reversals=sign.ne(sign.shift()) & sign.notna() & sign.shift().notna(); runs=state.ne(state.shift()).cumsum()
    turns=detect_turning_points(q[["date","v"]],"v") if len(q) else pd.DataFrame()
    out={"standard_deviation":q.v.std(),"range":q.v.max()-q.v.min(),"mean_absolute_monthly_change":d.abs().mean(),"reversals":int(reversals.sum()),"zero_crossings":int((state*state.shift()<0).sum()),
      "whipsaw_2m":float((sign.ne(sign.shift(2))&sign.notna()&sign.shift(2).notna()).mean()),"whipsaw_3m":float((sign.ne(sign.shift(3))&sign.notna()&sign.shift(3).notna()).mean()),
      "turning_point_count":int(turns["qualified"].sum()) if "qualified" in turns else 0,"persistence":1-int(reversals.sum())/max(len(d.dropna()),1),"mean_run_length":runs.value_counts().mean(),"time_above_zero":(q.v>0).mean(),"time_below_zero":(q.v<0).mean(),"average_absolute_score":q.v.abs().mean()}
    out["durable_reversals_2m"]=int((reversals & sign.eq(sign.shift(-1))).sum())
    out["durable_reversals_3m"]=int((reversals & sign.eq(sign.shift(-1)) & sign.eq(sign.shift(-2))).sum())
    return out

def _summaries(frame, keys, numeric):
    rows=[]
    for ids,g in frame.groupby(keys,dropna=False,sort=True):
      ids=(ids,) if not isinstance(ids,tuple) else ids
      for agg in ("mean","median","min","max"):
       row=dict(zip(keys,ids)); row["geo_id"]=f"seven_county_{agg}"; row.update(getattr(g[numeric],agg)().to_dict()); rows.append(row)
    return pd.DataFrame(rows)

def build_raw_cycle(source: pd.DataFrame, contract: pd.DataFrame) -> pd.DataFrame:
    """Build a diagnostic raw-supply cycle with exact calendar lag-12 semantics.

    The complete county/metric month-end calendar is retained so an absent
    source month cannot be mistaken for the twelfth preceding row.  The z-score
    is descriptive, within-county/metric, and is never fed into production.
    """
    directions = contract.groupby("metric").score_direction.agg(lambda x: set(str(v).strip().lower() for v in x))
    if directions.map(len).ne(1).any():
        raise ValueError("governed Supply score direction is ambiguous")
    direction_map = directions.map(lambda x: next(iter(x)))
    unsupported = set(direction_map) - {"positive", "negative"}
    if unsupported:
        raise ValueError(f"unsupported governed score direction: {sorted(unsupported)}")
    multiplier_map = direction_map.map({"positive": 1.0, "negative": -1.0})
    raw = _dates(source)
    mc = _metric_col(raw)
    rv = _value_col(
        raw,
        ("value", "metric_value", "raw_value"),
    )

    # Production source_metrics use canonical_metric_key, while some
    # deterministic fixtures / older diagnostic surfaces may carry the
    # registry metric identity. Accept either at the diagnostic boundary,
    # then canonicalize immediately.
    identity_map = (
        contract[
            ["registry_metric_key", "metric"]
        ]
        .drop_duplicates()
        .copy()
    )

    canonical_identity = (
        contract[["metric"]]
        .drop_duplicates()
        .assign(
            registry_metric_key=lambda q: q["metric"]
        )[
            ["registry_metric_key", "metric"]
        ]
    )

    identity_map = (
        pd.concat(
            [
                identity_map,
                canonical_identity,
            ],
            ignore_index=True,
        )
        .drop_duplicates(
            subset=["registry_metric_key"],
            keep="last",
        )
    )

    target_metrics = set(
        contract["metric"]
        .dropna()
        .astype(str)
        .unique()
    )

    raw = raw.rename(
        columns={
            mc: "source_metric_identity",
            rv: "raw_value",
        }
    )

    raw = raw.merge(
        identity_map.rename(
            columns={
                "registry_metric_key":
                    "source_metric_identity",
            }
        ),
        on="source_metric_identity",
        how="inner",
        validate="many_to_one",
    )

    raw["date"] = raw["date"].dt.to_period("M").dt.to_timestamp("M")
    raw = raw.loc[
        raw["geo_id"].isin(REVIEW_GEOS),
        [
            "geo_id",
            "date",
            "metric",
            "raw_value",
        ],
    ].copy()

    if raw.empty:
        raise ValueError(
            "No authoritative raw Supply chronology resolved "
            "from either canonical or registry metric identities; "
            f"expected canonical metrics={sorted(target_metrics)}"
        )

    resolved_metrics = set(
        raw["metric"]
        .dropna()
        .astype(str)
        .unique()
    )

    missing_metrics = target_metrics - resolved_metrics

    if missing_metrics:
        raise ValueError(
            "Authoritative raw Supply chronology is missing "
            f"canonical metrics={sorted(missing_metrics)}"
        )
    if raw.duplicated(["geo_id","date","metric"]).any():
        raise ValueError("duplicate raw Supply source observation")
    panels=[]
    for (geo,metric),g in raw.groupby(["geo_id","metric"],sort=True):
        idx=pd.date_range(g.date.min(),g.date.max(),freq="ME")
        q=g.set_index("date").reindex(idx).rename_axis("date").reset_index()
        q["geo_id"],q["metric"]=geo,metric
        # Reindexing makes shift(12) an exact calendar-month lag, not row lag.
        q["lag12_raw_value"]=q.raw_value.shift(12)
        q["raw_12m_change"]=q.raw_value.div(q.lag12_raw_value)-1
        q["score_direction"]=direction_map.loc[metric]
        q["orientation_multiplier"]=multiplier_map.loc[metric]
        q["oriented_raw_cycle"]=q.raw_12m_change*q.orientation_multiplier
        valid=q.raw_12m_change.dropna(); mean=valid.mean(); std=valid.std(ddof=0)
        q["raw_cycle_zscore"]=(q.raw_12m_change-mean)/std if pd.notna(std) and std>0 else np.nan
        oriented=q.oriented_raw_cycle.dropna(); omean=oriented.mean(); ostd=oriented.std(ddof=0)
        q["oriented_raw_cycle_zscore"]=(q.oriented_raw_cycle-omean)/ostd if pd.notna(ostd) and ostd>0 else np.nan
        panels.append(q)
    return pd.concat(panels,ignore_index=True)[["geo_id","date","metric","score_direction","orientation_multiplier","raw_value","lag12_raw_value","raw_12m_change","oriented_raw_cycle","raw_cycle_zscore","oriented_raw_cycle_zscore"]]

def _turn_evidence(score, ref, dates):
    q=pd.DataFrame({"date":pd.to_datetime(dates),"score":score,"ref":ref}).dropna().sort_values("date")
    rt=detect_turning_points(q[["date","ref"]],"ref")
    ct=detect_turning_points(q[["date","score"]],"score")
    matches=match_turning_points(rt,ct,TURN_MATCH_WINDOW_MONTHS)
    reference=matches.incumbent_date.notna() if len(matches) else pd.Series(dtype=bool)
    rm=matches.loc[reference] if len(matches) else matches
    hit=rm.loc[rm.matched] if len(rm) else rm
    delays=pd.to_numeric(hit.signed_delay_months,errors="coerce") if len(hit) else pd.Series(dtype=float)
    qualified_ref=int(rt.qualified.sum()) if "qualified" in rt else 0
    qualified_candidate=int(ct.qualified.sum()) if "qualified" in ct else 0
    return {"reference_turn_count":qualified_ref,"candidate_turn_count":qualified_candidate,
      "matched_turn_count":int(rm.matched.sum()) if len(rm) else 0,
      "missed_turn_count":int((~rm.matched).sum()) if len(rm) else qualified_ref,
      "turning_point_preservation":float(rm.matched.mean()) if len(rm) else np.nan,
      "median_turning_point_latency_months":float(delays.abs().median()) if len(delays) else np.nan,
      "same_month_turn_share":float(delays.abs().eq(0).mean()) if len(delays) else np.nan,
      "plus_minus_1_month_turn_share":float(delays.abs().le(1).mean()) if len(delays) else np.nan,
      "peak_latency_months":float(delays[hit.turning_point_type.eq("peak")].abs().median()) if len(delays) and hit.turning_point_type.eq("peak").any() else np.nan,
      "trough_latency_months":float(delays[hit.turning_point_type.eq("trough")].abs().median()) if len(delays) and hit.turning_point_type.eq("trough").any() else np.nan}

def _comparison(score, ref, dates):
    q=pd.DataFrame({"date":pd.to_datetime(dates),"score":score,"ref":ref}).dropna().sort_values("date")
    corr=safe_corr(q.score,q.ref)
    month_gap=(q.date.dt.year-q.date.shift().dt.year)*12+q.date.dt.month-q.date.shift().dt.month
    deltas=q[["score","ref"]].diff().where(month_gap.eq(1),axis=0).dropna()
    out={"valid_observation_count":len(q),"correlation":corr.correlation,
      "correlation_status":corr.status,"overlap_count":corr.overlap_count,
      "finite_left_count":corr.finite_left_count,"finite_right_count":corr.finite_right_count,
      "left_std":corr.left_std,"right_std":corr.right_std,
      "sign_agreement":float((np.sign(q.score)==np.sign(q.ref)).mean()) if len(q) else np.nan,
      "direction_agreement":float((np.sign(deltas.score)==np.sign(deltas.ref)).mean()) if len(deltas) else np.nan}
    out.update(_turn_evidence(q.score,q.ref,q.date)); return out

def build(artifacts: dict[str,pd.DataFrame], root: Path) -> dict[str,pd.DataFrame]:
    contract,mreg=resolve_contract(root)
    feature_sets=contract.groupby("metric").feature_type.agg(lambda values: set(values))
    if feature_sets.map(lambda values: values != set(FEATURES)).any():
        raise ValueError("Supply Phase 2 requires one governed level/short/long family per metric")
    registry=pd.DataFrame([{"experiment_metric":m,"policy":p,"scenario_id":f"{m}__MA12__{p}","level_weight":w[0],"short_weight":w[1],"long_weight":w[2],"ma_window":"MA12_FIXED","other_supply_metrics":"production_fixed"} for m,grid in EXPERIMENTS.items() for p,w in grid.items()])
    if not np.allclose(registry[["level_weight","short_weight","long_weight"]].sum(axis=1),1): raise ValueError("policy weights must sum to one")
    norm=_dates(artifacts["normalized_features"]); score=_value_col(norm,("feature_score","normalized_feature_score","normalized_value"))
    fmap=contract.set_index("feature_key")[["metric","feature_type"]]
    base=norm[norm.feature_key.isin(fmap.index)&norm.geo_id.isin(REVIEW_GEOS)].rename(columns={score:"normalized_feature_score"}).merge(fmap,left_on="feature_key",right_index=True,validate="many_to_one")
    base=base[["geo_id","date","metric","feature_key","feature_type","raw_feature_value","normalized_feature_score"]]
    # Supply sources mix month-start permits and month-end inventory. Phase-2
    # comparisons use the governed calendar-month evaluation identity.
    base["date"] = base["date"].dt.to_period("M").dt.to_timestamp("M")
    if base.duplicated(["geo_id","date","metric","feature_type"]).any(): raise ValueError("duplicate persisted normalized Supply feature")
    panels=[]
    for p,weights in POLICIES.items():
      target=POLICY_TARGET[p]
      q=base.copy(); q["policy"]=p; q["experiment_metric"]=target; q["scenario_id"]=f"{target}__MA12__{p}"
      q["configured_feature_weight"]=[dict(zip(FEATURES, weights if metric == target else PRODUCTION_FEATURE_WEIGHTS[metric]))[feature] for metric,feature in zip(q.metric,q.feature_type)]
      available=q.normalized_feature_score.notna(); q["available_weight_sum"]=q.configured_feature_weight.where(available,0).groupby([q.geo_id,q.date,q.metric]).transform("sum")
      q["effective_feature_weight"]=q.configured_feature_weight.div(q.available_weight_sum).where(available)
      q["weighted_contribution"]=q.normalized_feature_score*q.effective_feature_weight
      q["metric_score"]=q.groupby([q.geo_id,q.date,q.metric]).weighted_contribution.transform(lambda x:x.sum(min_count=1))
      panels.append(q)
    contrib=pd.concat(panels,ignore_index=True)
    # Isolation proof: scenario removal must leave one persisted upstream tuple.
    upstream=["raw_feature_value","normalized_feature_score"]
    if (contrib.groupby(["geo_id","date","metric","feature_type"])[upstream].nunique(dropna=False)>1).any().any(): raise ValueError("upstream feature inputs vary by policy")
    chron=contrib.drop_duplicates(["policy","geo_id","date","metric"])[["experiment_metric","policy","scenario_id","geo_id","date","metric","metric_score"]]
    stats=[]
    for (p,m,g),z in chron.groupby(["policy","metric","geo_id"]):
      for period,q in _periods(z): stats.append({"policy":p,"metric":m,"geo_id":g,"period":period,**_extra_stats(q.metric_score,q.date)})
    stats=pd.DataFrame(stats); numeric=[c for c in stats if c not in ("policy","metric","geo_id","period")]
    stats=pd.concat([stats,_summaries(stats,["policy","metric","period"],numeric)],ignore_index=True)
    wide=chron.pivot(index=["policy","geo_id","date"],columns="metric",values="metric_score").reset_index()
    metric_weights = contract[["metric", "metric_weight"]].drop_duplicates().set_index("metric")["metric_weight"]
    values = wide[list(TARGET_METRICS)]
    available = values.notna()
    denom = available.mul(metric_weights).sum(axis=1)
    wide["supply_dimension_score"] = values.mul(metric_weights).sum(axis=1, min_count=1).div(denom)
    dim=[]
    for (p,g),z in wide.groupby(["policy","geo_id"]):
      for period,q in _periods(z):
       gross=q[list(TARGET_METRICS)].abs().mul(metric_weights).sum(axis=1).div(q[list(TARGET_METRICS)].notna().mul(metric_weights).sum(axis=1)); row={"policy":p,"geo_id":g,"period":period,**_extra_stats(q.supply_dimension_score,q.date)}
       row["metric_level_cancellation"]=(1-q.supply_dimension_score.abs().div(gross.replace(0,np.nan))).mean(); dim.append(row)
    dim=pd.DataFrame(dim)
    # Propagate the unchanged registry-resolved Supply delta through persisted P0 Supply.
    axes=_dates(artifacts["axis_scores"]); axiscol=next((c for c in ("axis","axis_name") if c in axes),None); val=_value_col(axes,("axis_score","score"))
    supply=axes[axes[axiscol].astype(str).str.lower().eq("supply") & axes.geo_id.isin(REVIEW_GEOS)][["geo_id","date",val]].rename(columns={val:"p0_supply_dimension_axis"})
    wide["experiment_metric"]=wide.policy.map(POLICY_TARGET)
    wide["control_policy"]=wide.policy.str[0]+"0"
    p0=wide[wide.policy.eq(wide.control_policy)][["experiment_metric","geo_id","date","supply_dimension_score"]].rename(columns={"supply_dimension_score":"p0_supply_dimension"})
    axis_registry = pd.read_csv(root / "config/axis_registry.csv")
    supply_aff = axis_registry[(axis_registry.axis.eq("supply")) & (axis_registry.dimension.eq("supply")) & axis_registry.enabled.astype(str).str.lower().isin(("true", "1", "yes"))]
    if len(supply_aff) != 1: raise ValueError("Supply/Supply axis weight is missing or ambiguous")
    supply_weight = float(supply_aff.dimension_weight.iloc[0])
    dw=wide.merge(p0,on=["experiment_metric","geo_id","date"],validate="many_to_one").merge(supply,on=["geo_id","date"],validate="many_to_one")
    dw["supply_axis_score"]=dw.p0_supply_dimension_axis+supply_weight*(dw.supply_dimension_score-dw.p0_supply_dimension)
    dst=[]; correlation_audit=[]
    for (p,g),z in dw.groupby(["policy","geo_id"]):
      for period,q in _periods(z):
       row={"policy":p,"geo_id":g,"period":period,**_extra_stats(q.supply_axis_score,q.date)}
       p0_stats=_extra_stats(q.p0_supply_dimension_axis,q.date)
       corr=safe_corr(q.supply_axis_score,q.p0_supply_dimension_axis)
       row["chronology_correlation_to_p0"]=corr.correlation
       correlation_audit.append({"comparison_type":"supply_axis_to_p0","scenario":p,"metric":"supply_axis","geography":g,"period":period,"correlation":corr.correlation,"correlation_status":corr.status,"overlap_count":corr.overlap_count,"finite_left_count":corr.finite_left_count,"finite_right_count":corr.finite_right_count,"left_std":corr.left_std,"right_std":corr.right_std})
       row["direction_changes_vs_p0"]=(np.sign(q.supply_axis_score.diff())!=np.sign(q.p0_supply_dimension_axis.diff())).sum()
       row["sign_changes_vs_p0"]=(np.sign(q.supply_axis_score)!=np.sign(q.p0_supply_dimension_axis)).sum()
       row["turning_point_count_change_vs_p0"]=row["turning_point_count"]-p0_stats["turning_point_count"]
       row["reversal_change_vs_p0"]=row["reversals"]-p0_stats["reversals"]
       row["whipsaw_2m_change_vs_p0"]=row["whipsaw_2m"]-p0_stats["whipsaw_2m"]
       row["whipsaw_3m_change_vs_p0"]=row["whipsaw_3m"]-p0_stats["whipsaw_3m"]
       row["persistence_change_vs_p0"]=row["persistence"]-p0_stats["persistence"]
       dst.append(row)
    dst=pd.DataFrame(dst)
    # Supply calibration cannot alter Demand. Replicate the persisted Demand
    # control for every independent scenario so the review package proves it.
    demand=axes[axes[axiscol].astype(str).str.lower().eq("demand") & axes.geo_id.isin(REVIEW_GEOS)][["geo_id","date",val]].rename(columns={val:"demand_axis_score"})
    demand_rows=[]
    for policy in POLICIES:
      for geo,z in demand.groupby("geo_id"):
       for period,q in _periods(z):
        demand_rows.append({"experiment_metric":POLICY_TARGET[policy],"policy":policy,"geo_id":geo,"period":period,**_extra_stats(q.demand_axis_score,q.date),"chronology_correlation_to_production":1.0,"maximum_absolute_delta_from_production":0.0})
    demand_stats=pd.DataFrame(demand_rows)
    refs=[]; rawrefs=[]; turnrows=[]
    raw=build_raw_cycle(artifacts["source_metrics"],contract)
    rawdim=raw.pivot(index=["geo_id","date"],columns="metric",values="oriented_raw_cycle").reset_index()
    raw_available=rawdim[list(TARGET_METRICS)].notna()
    rawdim["oriented_supply_cycle"]=rawdim[list(TARGET_METRICS)].mul(metric_weights).sum(axis=1,min_count=1).div(raw_available.mul(metric_weights).sum(axis=1))
    for i,row in dim.iterrows():
      candidate=wide[(wide.policy.eq(row.policy))&(wide.geo_id.eq(row.geo_id))][["date","supply_dimension_score"]]
      joined=candidate.merge(rawdim[rawdim.geo_id.eq(row.geo_id)][["date","oriented_supply_cycle"]],on="date")
      period_frame=next(q for name,q in _periods(joined) if name==row.period)
      evidence=_comparison(period_frame.supply_dimension_score,period_frame.oriented_supply_cycle,period_frame.date)
      dim.loc[i,"oriented_cycle_correlation"]=evidence["correlation"]
      dim.loc[i,"oriented_cycle_sign_agreement"]=evidence["sign_agreement"]
      dim.loc[i,"oriented_cycle_direction_agreement"]=evidence["direction_agreement"]
      dim.loc[i,"oriented_cycle_correlation_status"]=evidence["correlation_status"]
      dim.loc[i,"oriented_cycle_overlap_count"]=evidence["overlap_count"]
      correlation_audit.append({"comparison_type":"supply_dimension_to_oriented_raw_cycle","scenario":row.policy,"metric":"supply_dimension","geography":row.geo_id,"period":row.period,**{k:evidence[k] for k in ("correlation","correlation_status","overlap_count","finite_left_count","finite_right_count","left_std","right_std")}})
    for (p,m,g),z in chron.groupby(["policy","metric","geo_id"]):
      for feature_type in FEATURES:
       feature = contrib[(contrib.policy.eq(p))&(contrib.metric.eq(m))&(contrib.geo_id.eq(g))&(contrib.feature_type.eq(feature_type))][["policy","geo_id","date","metric","normalized_feature_score"]]
       q=z.merge(feature,on=["policy","geo_id","date","metric"])
       for period,part in _periods(q):
        evidence=_comparison(part.metric_score,part.normalized_feature_score,part.date)
        refs.append({"policy":p,"metric":m,"geo_id":g,"period":period,"reference_type":f"{feature_type}_feature_reference",**evidence})
        correlation_audit.append({"comparison_type":f"candidate_to_{feature_type}_feature_reference","scenario":p,"metric":m,"geography":g,"period":period,**{k:evidence[k] for k in ("correlation","correlation_status","overlap_count","finite_left_count","finite_right_count","left_std","right_std")}})
      r=raw[(raw.metric.eq(m))&(raw.geo_id.eq(g))]; q=z.merge(r[["date","score_direction","orientation_multiplier","raw_12m_change","oriented_raw_cycle","oriented_raw_cycle_zscore"]],on="date")
      for period,part in _periods(q):
       if part.empty:
        continue
       # Every primary comparison follows governed production-score semantics.
       evidence=_comparison(part.metric_score,part.oriented_raw_cycle,part.date)
       turns=_turn_evidence(part.metric_score,part.oriented_raw_cycle_zscore,part.date)
       evidence.update(turns)
       evidence["missed_raw_cycle_turns"]=evidence["missed_turn_count"]
       evidence["median_absolute_latency"]=evidence["median_turning_point_latency_months"]
       rawrefs.append({"policy":p,"metric":m,"geo_id":g,"period":period,"reference_type":"oriented_raw_cycle_reference","score_direction":part.score_direction.iloc[0],"orientation_multiplier":part.orientation_multiplier.iloc[0],**evidence})
       correlation_audit.append({"comparison_type":"candidate_to_oriented_raw_cycle","scenario":p,"metric":m,"geography":g,"period":period,**{k:evidence[k] for k in ("correlation","correlation_status","overlap_count","finite_left_count","finite_right_count","left_std","right_std")}})
       turnrows.append({"policy":p,"metric":m,"geo_id":g,"period":period,"reference_type":"oriented_raw_cycle_reference","score_direction":part.score_direction.iloc[0],"orientation_multiplier":part.orientation_multiplier.iloc[0],"matching_tolerance_months":TURN_MATCH_WINDOW_MONTHS,**{k:v for k,v in evidence.items() if "turn" in k or "latency" in k}})
    refs=pd.DataFrame(refs); rawrefs=pd.DataFrame(rawrefs)
    fc=[]
    for (p,m),g in contrib.groupby(["policy","metric"]):
      absmeans=g.groupby("feature_type").weighted_contribution.apply(lambda x:x.abs().mean()); gross=g.groupby(["geo_id","date"]).weighted_contribution.apply(lambda x:x.abs().sum()); net=g.groupby(["geo_id","date"]).weighted_contribution.sum().abs()
      row={"policy":p,"metric":m,"net_to_gross_ratio":net.sum()/gross.sum(),"sign_disagreement_rate":g.groupby(["geo_id","date"]).normalized_feature_score.apply(lambda x:len(set(np.sign(x.dropna())))>1).mean()}
      for ft in FEATURES: row[f"{ft}_mean_absolute_contribution"]=absmeans.get(ft,np.nan); row[f"{ft}_share_of_absolute_contribution"]=absmeans.get(ft,0)/absmeans.sum()
      row["cancellation"]=1-row["net_to_gross_ratio"]; fc.append(row)
    fc=pd.DataFrame(fc)
    contrib=contrib.merge(fc,on=["policy","metric"],how="left",validate="many_to_one")
    basefull=stats[(stats.period.eq("full_history"))&~stats.geo_id.str.startswith("seven_county")]
    comparisons=[]
    for left,right in ADJACENT:
      a=basefull[basefull.policy.eq(left)].set_index(["metric","geo_id"]); b=basefull[basefull.policy.eq(right)].set_index(["metric","geo_id"])
      for idx in a.index.intersection(b.index):
       row={"from_policy":left,"to_policy":right,"metric":idx[0],"geo_id":idx[1]}; row.update({f"delta_{c}":b.loc[idx,c]-a.loc[idx,c] for c in numeric}); comparisons.append(row)
    comparisons=pd.DataFrame(comparisons)
    # Controlled comparisons combine chronology, stability, and contribution evidence.
    rawfull=rawrefs[rawrefs.period.eq("full_history")].set_index(["policy","metric","geo_id"])
    fcidx=fc.set_index(["policy","metric"])
    for i,row in comparisons.iterrows():
      left,right,metric,geo=row.from_policy,row.to_policy,row.metric,row.geo_id
      for c in ("correlation","sign_agreement","direction_agreement","turning_point_preservation","median_turning_point_latency_months"):
       comparisons.loc[i,f"delta_oriented_raw_cycle_{c}"]=rawfull.loc[(right,metric,geo),c]-rawfull.loc[(left,metric,geo),c]
      for c in ("level_share_of_absolute_contribution","short_share_of_absolute_contribution","long_share_of_absolute_contribution","cancellation","net_to_gross_ratio"):
       comparisons.loc[i,f"delta_{c}"]=fcidx.loc[(right,metric),c]-fcidx.loc[(left,metric),c]
    stats["experiment_metric"]=stats.policy.map(POLICY_TARGET)
    stats["control_policy"]=stats.policy.str[0]+"0"
    p0=stats[stats.policy.eq(stats.control_policy)].set_index(["experiment_metric","metric","geo_id","period"]); vp=[]
    for row in stats[~stats.policy.eq(stats.control_policy)].itertuples(index=False):
      key=(row.experiment_metric,row.metric,row.geo_id,row.period)
      if key in p0.index:
       x={"experiment_metric":row.experiment_metric,"policy":row.policy,"metric":row.metric,"geo_id":row.geo_id,"period":row.period}; x.update({f"delta_{c}":getattr(row,c)-p0.loc[key,c] for c in numeric}); vp.append(x)
    cross=fc.pivot(index="policy",columns="metric",values=["net_to_gross_ratio","cancellation"]).reset_index(); cross.columns=["_".join(x).strip("_") for x in cross.columns]
    governance=pd.DataFrame([{"recommendation_state":"none","promotion_state":"current_production_unchanged","human_decision":"supply_feature_weight_review_pending","automated_winner":False,"production_policy_changed":False,"ma_window":"MA12_FIXED","candidate_grid_closed":True,"experiments_independent":True,"other_supply_metrics_fixed_at_production":True,"metric_weights_changed":False,"ma_calibration":False,"capital_markets_changed":False,"supply_long_weight_boundary_unresolved":"empirical_review_required","supply_long_weight_boundary_supported":"empirical_review_required","feature_reference_role":"diagnostic_only_not_optimization_target","normalization_changed":False,"raw_cycle_orientation":"registry_score_direction","raw_cycle_standardization":"within_county_metric_zscore_ddof0_diagnostic_only"}])
    evaluation=pd.DataFrame([{"question":i,"status":"empirical_review_required","evidence":"authoritative review tables and plots; no automated winner"} for i in range(1,25)])
    return {"scenario_registry":registry,"metric_chronology":chron,"feature_contributions":contrib,"metric_statistics":stats,"dimension_statistics":dim,"supply_axis_statistics":dst,"demand_axis_statistics":demand_stats,"feature_reference_comparison":refs,"raw_cycle_comparison":rawrefs,"raw_cycle_chronology":raw,"turning_point_comparison":pd.DataFrame(turnrows),"adjacent_comparisons":comparisons,"vs_p0":pd.DataFrame(vp),"cross_metric_consistency":cross,"by_county":basefull,"period_sensitivity":stats,"evaluation_matrix":evaluation,"governance_status":governance,"correlation_audit":pd.DataFrame(correlation_audit),"_dimension_chronology":wide}

def _svg(path, series, title):
    width,height=1100,480; left,right,top,bottom=75,25,45,45; dates=pd.concat([pd.to_datetime(x.date).dropna() for _,x in series if len(x)]); lo,hi=dates.min(),dates.max(); span=max((hi-lo).total_seconds(),1)
    values=pd.concat([pd.to_numeric(x.value,errors="coerce") for _,x in series]); low,high=values.min(),values.max(); pad=max((high-low)*.05,.01); low-=pad; high+=pad
    colors=("#0f172a","#2563eb","#059669","#dc2626","#9333ea","#ea580c","#0891b2"); paths=[]
    for n,(label,q) in enumerate(series):
      cmd=[]; draw=False; prev=None
      for r in q.sort_values("date").itertuples(index=False):
       gap=prev is not None and (r.date.to_period("M")-prev.to_period("M")).n>1
       if pd.isna(r.value): draw=False; prev=r.date; continue
       x=left+(r.date-lo).total_seconds()/span*(width-left-right); y=top+(high-r.value)/(high-low)*(height-top-bottom); cmd.append(f'{"L" if draw and not gap else "M"}{x:.2f},{y:.2f}'); draw=True; prev=r.date
      paths.append(f'<path d="{" ".join(cmd)}" fill="none" stroke="{colors[n%len(colors)]}" stroke-width="1.5"/><text x="{left+120*n}" y="{height-12}" fill="{colors[n%len(colors)]}" font-family="sans-serif">{html.escape(label)}</text>')
    path.write_text(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}"><title>{html.escape(title)}</title><rect width="100%" height="100%" fill="white"/><text x="20" y="28" font-family="sans-serif" font-size="20">{html.escape(title)}</text><rect x="{left}" y="{top}" width="{width-left-right}" height="{height-top-bottom}" fill="none" stroke="#94a3b8"/>{"".join(paths)}</svg>',encoding="utf-8")

def _turn_plot(path: Path, reference: pd.DataFrame, candidate: pd.DataFrame, title: str) -> None:
    """Render aligned series panels and visible governed turning-point markers."""
    panels=[("oriented raw 12m cycle",reference),("candidate metric score",candidate)]
    _plot(path,panels,title)
    joined=reference.rename(columns={"value":"ref"}).merge(candidate.rename(columns={"value":"score"}),on="date").dropna()
    rt=detect_turning_points(joined[["date","ref"]],"ref")
    ct=detect_turning_points(joined[["date","score"]],"score")
    mt=match_turning_points(rt,ct,TURN_MATCH_WINDOW_MONTHS)
    marks=[(0,d,"#dc2626") for d in mt.loc[mt.incumbent_date.notna(),"incumbent_date"]]
    marks += [(1,d,"#059669") for d in mt.loc[mt.matched,"challenger_date"]]
    dates=pd.concat([reference.date,candidate.date]).dropna(); lo,hi=dates.min(),dates.max(); span=max((hi-lo).total_seconds(),1)
    extra=[]
    for panel,date,color in marks:
      x=95+(pd.Timestamp(date)-lo).total_seconds()/span*(1100-95-25); y=55+panel*190+12
      extra.append(f'<circle cx="{x:.2f}" cy="{y}" r="5" fill="{color}"/>')
    extra.append('<text x="720" y="30" font-family="sans-serif" font-size="12" fill="#dc2626">red: reference turns</text><text x="880" y="30" font-family="sans-serif" font-size="12" fill="#059669">green: matched candidate turns</text>')
    path.write_text(path.read_text(encoding="utf-8").replace("</svg>","".join(extra)+"</svg>"),encoding="utf-8")

def write_review(tables, out: Path):
    out.mkdir(parents=True,exist_ok=True)
    for name in EXPORTS: tables[name].to_csv(out/f"supply_phase2_{name}.csv",index=False)
    plots=[]; chron=tables["metric_chronology"]
    for metric in TARGET_METRICS:
      raw=tables["raw_cycle_chronology"].query("metric==@metric")
      for scope in ("dc","seven_county_equal_footing"):
       if scope=="dc": ref=raw[raw.geo_id.eq(DC)][["date","oriented_raw_cycle"]].rename(columns={"oriented_raw_cycle":"value"})
       else: ref=_pool(raw,"oriented_raw_cycle_zscore",["geo_id"]).rename(columns={"oriented_raw_cycle_zscore":"value"})
       panels=[("oriented raw cycle (production-score semantics)",ref)]
       for p in EXPERIMENTS[metric]:
        q=chron[(chron.metric.eq(metric))&(chron.policy.eq(p))]
        q=(q[q.geo_id.eq(DC)][["date","metric_score"]] if scope=="dc" else _pool(q,"metric_score",["geo_id","policy"]))
        panels.append((p,q.rename(columns={"metric_score":"value"})))
       fn=f"supply_phase2_{metric}_{scope}_raw_cycle.svg"; _plot(out/fn,panels,f"{metric} — {scope} — raw cycle oriented to production-score semantics"); plots.append(fn)
      dcraw=raw[raw.geo_id.eq(DC)][["date","oriented_raw_cycle_zscore"]].rename(columns={"oriented_raw_cycle_zscore":"value"})
      for p in EXPERIMENTS[metric]:
       cand=chron[(chron.metric.eq(metric))&(chron.policy.eq(p))&(chron.geo_id.eq(DC))][["date","metric_score"]].rename(columns={"metric_score":"value"})
       fn=f"supply_phase2_{metric}_dc_{p}_turning_point_overlay.svg"; _turn_plot(out/fn,dcraw,cand,f"{metric} — DC — {p} turning points"); plots.append(fn)
      for scope in ("dc","seven_county_equal_footing"):
       series=[]
       for p in EXPERIMENTS[metric]:
        q=chron[(chron.metric.eq(metric))&(chron.policy.eq(p))]
        q=(q[q.geo_id.eq(DC)].groupby("date",as_index=False).metric_score.mean() if scope=="dc" else _pool(q,"metric_score",["geo_id","policy"]))
        series.append((p,q.rename(columns={"metric_score":"value"})))
       fn=f"supply_phase2_{metric}_{scope}_policies.svg"; _svg(out/fn,series,f"{metric} — {scope}"); plots.append(fn)
       focus=series[1:]; fn=f"supply_phase2_{metric}_{scope}_focus.svg"; _svg(out/fn,focus,f"{metric} finalist neighborhood — {scope}"); plots.append(fn)
      q=tables["feature_contributions"]
      for policy in EXPERIMENTS[metric]:
       selected=q[(q.metric.eq(metric))&(q.policy.eq(policy))&(q.geo_id.eq(DC))]
       series=[]
       for feature_type in FEATURES:
        z=selected[selected.feature_type.eq(feature_type)][["date","weighted_contribution"]].rename(columns={"weighted_contribution":"value"})
        series.append((f"{feature_type} contribution",z))
       score=selected.drop_duplicates("date")[["date","metric_score"]].rename(columns={"metric_score":"value"})
       series.append(("final metric score",score))
       fn=f"supply_phase2_{metric}_{policy}_contribution_decomposition.svg"
       _svg(out/fn,series,f"{metric} — {policy} contribution decomposition"); plots.append(fn)
    d=tables["_dimension_chronology"]
    for scope in ("dc","seven_county_equal_footing"):
      series=[]
      for p in POLICIES:
       q=d[d.policy.eq(p)]; q=(q[q.geo_id.eq(DC)][["date","supply_dimension_score"]] if scope=="dc" else _pool(q,"supply_dimension_score",["geo_id","policy"])); series.append((p,q.rename(columns={"supply_dimension_score":"value"})))
      fn=f"supply_phase2_supply_dimension_{scope}.svg"; _svg(out/fn,series,f"Supply dimension — {scope}"); plots.append(fn)
    # Response-curve files use a proportional numeric policy axis represented as dates.
    for subject in (*TARGET_METRICS,"supply_dimension"):
      frame=tables["metric_statistics"] if subject!="supply_dimension" else tables["dimension_statistics"]
      q=frame[frame.period.eq("full_history")]
      if subject!="supply_dimension": q=q[(q.metric.eq(subject))&q.geo_id.eq("seven_county_mean")]
      else: q=q[q.geo_id.isin(REVIEW_GEOS)].groupby("policy",as_index=False).mean(numeric_only=True)
      series=[]
      for metric in ("reversals","whipsaw_2m","whipsaw_3m","persistence","standard_deviation","turning_point_count"):
       if metric in q:
        values=q.set_index("policy").reindex(POLICIES)[metric].values
        series.append((metric,pd.DataFrame({"date":pd.date_range("2000-01-31",periods=len(values),freq="ME"),"value":values})))
      fn=f"supply_phase2_{subject}_response_curves.svg"; _svg(out/fn,series,f"{subject} response curves"); plots.append(fn)
    files=[*(f"supply_phase2_{n}.csv" for n in EXPORTS),*plots]
    links="".join(f'<li><a href="{html.escape(x)}">{html.escape(x)}</a></li>' for x in files)
    (out/"supply_phase2_review_index.html").write_text(f"<!doctype html><meta charset=utf-8><title>Supply Phase 2</title><h1>Supply feature-weight calibration</h1><p>Diagnostic only; human review pending; production unchanged; MA12 fixed.</p><ul>{links}</ul>",encoding="utf-8")
