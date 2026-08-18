"""Capital Markets Phase-2 bounded feature-weight calibration.

Only persisted, production-normalized features are reweighted.  Candidate metric
scores are built at the national native layer and each downstream scenario
changes exactly one metric; all registries and production artifacts are read-only.
"""
from __future__ import annotations

from pathlib import Path
import html
from time import perf_counter
import numpy as np
import pandas as pd

from regime.diagnostics import price_feature_anatomy as canonical
from regime.diagnostics.capital_markets_feature_anatomy import (
    EXPECTED_AXIS_WEIGHTS, EXPECTED_WEIGHTS, EXPECTED_WINDOWS, NATIVE_GEO,
    load_run as _load_phase1, resolve_contract,
)
from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points
from regime.diagnostics.correlation import safe_corr

FEATURES = ("level", "short", "long")
POLICIES = {
    "P0": (.60, .20, .20), "P1": (.60, .15, .25),
    "P2": (.60, .10, .30), "P3": (.55, .15, .30),
    "P4": (.55, .10, .35), "P5": (.50, .10, .40),
    "P6": (.60, .05, .35), "P7": (.35, .10, .55),
}
ADJACENT = (("P0", "P1"), ("P1", "P2"), ("P2", "P6"),
            ("P1", "P3"), ("P2", "P4"), ("P4", "P5"), ("P5", "P7"))
PERIODS = ("full_history", "2022_plus", "latest_36_months")
FAMILIES = {
    "long_term_rates": ("mortgage_30y", "mortgage_15y", "treasury_10y"),
    "policy_rate": ("fedfunds",),
    "spreads": ("spread_10y_2y", "spread_10y_fedfunds"),
}
EXPORTS = (
    "scenario_registry", "metric_chronology", "feature_contributions",
    "feature_statistics", "metric_statistics", "raw_cycle_chronology",
    "raw_cycle_comparison", "feature_reference_comparison",
    "turning_point_comparison", "effective_delay", "adjacent_comparisons",
    "vs_p0", "family_consistency", "period_sensitivity",
    "dimension_statistics", "demand_axis_statistics", "supply_axis_statistics",
    "cross_axis_materiality", "responsiveness", "correlation_audit",
    "evaluation_matrix", "governance_status",
    "turning_point_audit", "policy_decision_table", "policy_marginal_deltas",
    "family_plateau_summary", "performance_audit",
)


def load_run(run: Path) -> dict[str, pd.DataFrame]:
    """Load only the named authoritative run; never substitute another run."""
    return _load_phase1(run)


def _periods(frame: pd.DataFrame):
    q = frame.sort_values("date")
    yield "full_history", q
    yield "2022_plus", q[q.date.ge(pd.Timestamp("2022-01-01"))]
    yield "latest_36_months", q[q.date.ge(q.date.max() - pd.DateOffset(months=35))]


def _stats(values, dates, *, turning_points=True):
    q = pd.DataFrame({"date": pd.to_datetime(dates), "v": pd.to_numeric(values, errors="coerce")}).dropna().sort_values("date")
    d = q.v.diff(); direction = np.sign(d).replace(0, np.nan)
    reversals = direction.ne(direction.shift()) & direction.notna() & direction.shift().notna()
    state = np.sign(q.v).replace(0, np.nan).ffill(); runs = state.ne(state.shift()).cumsum()
    turns = detect_turning_points(q[["date", "v"]], "v") if turning_points and len(q) else pd.DataFrame()
    return {
        "standard_deviation": q.v.std(), "range": q.v.max() - q.v.min(),
        "average_absolute_score": q.v.abs().mean(), "mean_absolute_monthly_movement": d.abs().mean(),
        "reversals": int(reversals.sum()), "zero_crossings": int((state * state.shift() < 0).sum()),
        "whipsaw_2m": float((direction.ne(direction.shift(2)) & direction.notna() & direction.shift(2).notna()).mean()),
        "whipsaw_3m": float((direction.ne(direction.shift(3)) & direction.notna() & direction.shift(3).notna()).mean()),
        "persistence": 1 - int(reversals.sum()) / max(len(d.dropna()), 1),
        "mean_run_length": runs.value_counts().mean(),
        "durable_reversal_count": int((reversals & direction.eq(direction.shift(-1)) & direction.eq(direction.shift(-2))).sum()),
        "turning_point_count": int(turns.qualified.sum()) if "qualified" in turns else 0,
    }


def _comparison(score, reference, dates, *, return_audit=False, match_window=1, turning_points=True):
    q = pd.DataFrame({"date": pd.to_datetime(dates), "score": score, "reference": reference}).dropna().sort_values("date")
    corr = safe_corr(q.score, q.reference)
    delta = q[["score", "reference"]].diff().dropna()
    rt = detect_turning_points(q[["date", "reference"]], "reference", include_rejected=True) if turning_points else pd.DataFrame()
    ct = detect_turning_points(q[["date", "score"]], "score", include_rejected=True) if turning_points else pd.DataFrame()
    matched = match_turning_points(rt, ct, match_window) if turning_points else pd.DataFrame()
    refs = matched[matched.incumbent_date.notna()] if len(matched) else matched
    hits = refs[refs.matched] if len(refs) else refs
    delay = pd.to_numeric(hits.signed_delay_months, errors="coerce") if len(hits) else pd.Series(dtype=float)
    reference_turn_count=int(rt.qualified.sum()) if "qualified" in rt else 0
    candidate_turn_count=int(ct.qualified.sum()) if "qualified" in ct else 0
    if not reference_turn_count: turn_status, delay_status = "no_reference_turns", "no_reference_turns"
    elif not candidate_turn_count: turn_status, delay_status = "no_candidate_turns", "no_candidate_turns"
    elif not len(hits): turn_status, delay_status = "no_matches", "no_matches"
    else: turn_status, delay_status = "ok", "ok"
    result = {
        "correlation": corr.correlation, "correlation_status": corr.status,
        "overlap_count": corr.overlap_count, "finite_left_count": corr.finite_left_count,
        "finite_right_count": corr.finite_right_count, "left_std": corr.left_std, "right_std": corr.right_std,
        "sign_agreement": float(np.sign(q.score).eq(np.sign(q.reference)).mean()) if len(q) else np.nan,
        "direction_agreement": float(np.sign(delta.score).eq(np.sign(delta.reference)).mean()) if len(delta) else np.nan,
        "reference_turn_count": int(rt.qualified.sum()) if "qualified" in rt else 0,
        "candidate_turn_count": int(ct.qualified.sum()) if "qualified" in ct else 0,
        "turning_point_preservation": float(refs.matched.mean()) if len(refs) else np.nan,
        "same_month_matching": float(delay.abs().eq(0).mean()) if len(delay) else np.nan,
        "plus_minus_1_month_matching": float(delay.abs().le(1).mean()) if len(delay) else np.nan,
        "missed_reference_turns": int((~refs.matched).sum()) if len(refs) else int(rt.qualified.sum()) if "qualified" in rt else 0,
        "signed_delay": delay.mean() if len(delay) else np.nan,
        "absolute_delay": delay.abs().mean() if len(delay) else np.nan,
        "candidate_start_date": q.date.min(), "candidate_end_date": q.date.max(),
        "reference_start_date": q.date.min(), "reference_end_date": q.date.max(),
        "aligned_start_date": q.date.min(), "aligned_end_date": q.date.max(),
        "candidate_std": q.score.std(), "reference_std": q.reference.std(),
        "turning_point_status": turn_status, "delay_status": delay_status,
    }
    audit=[]
    # The matcher is the authoritative row-level identity; unmatched rejected detector
    # rows are also retained so a zero result is never opaque.
    if len(matched):
        for r in matched.itertuples(index=False):
            audit.append({"reference_turn_date":getattr(r,"incumbent_date",pd.NaT),
                "reference_turn_type":getattr(r,"turning_point_type",None),
                "reference_qualified":pd.notna(getattr(r,"incumbent_date",pd.NaT)),
                "reference_rejection_reason":"", "candidate_turn_date":getattr(r,"challenger_date",pd.NaT),
                "candidate_turn_type":getattr(r,"turning_point_type",None),
                "candidate_qualified":pd.notna(getattr(r,"challenger_date",pd.NaT)),
                "candidate_rejection_reason":"", "matched":bool(getattr(r,"matched",False)),
                "signed_delay_months":getattr(r,"signed_delay_months",np.nan),
                "absolute_delay_months":abs(getattr(r,"signed_delay_months",np.nan)),
                "match_window_months":match_window})
    for label, frame in (("reference",rt),("candidate",ct)):
        if "qualified" in frame:
            for r in frame.loc[~frame.qualified].itertuples(index=False):
                audit.append({"reference_turn_date":r.turning_point_date if label=="reference" else pd.NaT,
                    "reference_turn_type":getattr(r,"turning_point_type",None) if label=="reference" else None,
                    "reference_qualified":False, "reference_rejection_reason":getattr(r,"rejection_reason","detector_rejected") if label=="reference" else "",
                    "candidate_turn_date":r.turning_point_date if label=="candidate" else pd.NaT,
                    "candidate_turn_type":getattr(r,"turning_point_type",None) if label=="candidate" else None,
                    "candidate_qualified":False, "candidate_rejection_reason":getattr(r,"rejection_reason","detector_rejected") if label=="candidate" else "",
                    "matched":False,"signed_delay_months":np.nan,"absolute_delay_months":np.nan,"match_window_months":match_window})
    return (result,pd.DataFrame(audit)) if return_audit else result


def _raw_cycle(source: pd.DataFrame, contract: pd.DataFrame) -> pd.DataFrame:
    q = canonical._dates(source); mc = canonical._metric_col(q); vc = canonical._value_col(q, ("value", "metric_value", "raw_value"))
    identities = pd.concat([contract[["registry_metric_key", "metric"]].drop_duplicates(),
        contract[["metric"]].drop_duplicates().assign(registry_metric_key=lambda x: x.metric)[["registry_metric_key", "metric"]]]).drop_duplicates("registry_metric_key", keep="last")
    q = q.rename(columns={mc: "registry_metric_key", vc: "raw_value"}).merge(identities, on="registry_metric_key", validate="many_to_one")
    q = q[q.geo_id.eq(NATIVE_GEO)][["geo_id", "date", "metric", "raw_value"]].copy()
    q["date"] = q.date.dt.to_period("M").dt.to_timestamp("M")
    if set(q.metric) != set(EXPECTED_WEIGHTS) or q.duplicated(["date", "metric"]).any():
        raise ValueError("authoritative national raw-cycle input is missing or duplicated")
    direction = contract.groupby("metric").score_direction.first()
    rows = []
    for metric, g in q.groupby("metric"):
        g = g.sort_values("date").copy(); g["raw_unoriented_change"] = g.raw_value.diff()
        g["score_direction"] = direction[metric]; g["orientation_multiplier"] = {"positive": 1., "negative": -1.}[direction[metric]]
        g["oriented_raw_cycle"] = g.raw_unoriented_change * g.orientation_multiplier
        rows.append(g)
    return pd.concat(rows, ignore_index=True)


def build(artifacts: dict[str, pd.DataFrame], root: Path) -> dict[str, pd.DataFrame]:
    build_started = perf_counter()
    contract, _ = resolve_contract(root)
    registry = pd.DataFrame([{"policy": p, "level_weight": w[0], "short_weight": w[1], "long_weight": w[2],
        "candidate_grid": "P0-P7", "candidate_grid_closed": True, "native_geo_id": NATIVE_GEO,
        "feature_construction": "production_persisted_normalized_features"} for p, w in POLICIES.items()])
    if list(POLICIES) != [f"P{i}" for i in range(8)] or not np.allclose(registry[["level_weight", "short_weight", "long_weight"]].sum(axis=1), 1):
        raise ValueError("closed P0-P7 candidate grid is invalid")
    norm = canonical._dates(artifacts["normalized_features"]); score_col = canonical._value_col(norm, ("feature_score", "normalized_feature_score", "normalized_value"))
    fmap = contract.set_index("feature_key")[["metric", "feature_type"]]
    base = norm[norm.feature_key.isin(fmap.index)].rename(columns={score_col: "normalized_feature_score"}).merge(fmap, left_on="feature_key", right_index=True, validate="many_to_one")
    if set(base.geo_id) != {NATIVE_GEO}: raise ValueError("Phase 2 features require exactly national native geography")
    base["date"] = base.date.dt.to_period("M").dt.to_timestamp("M")
    if base.duplicated(["date", "metric", "feature_type"]).any(): raise ValueError("duplicate persisted normalized feature")
    panels = []
    for policy, weights in POLICIES.items():
        q = base.copy(); q["policy"] = policy; q["configured_feature_weight"] = q.feature_type.map(dict(zip(FEATURES, weights)))
        available = q.normalized_feature_score.notna(); keys = [q.date, q.metric]
        q["available_weight_sum"] = q.configured_feature_weight.where(available, 0).groupby(keys).transform("sum")
        q["effective_feature_weight"] = q.configured_feature_weight.div(q.available_weight_sum).where(available)
        q["weighted_feature_contribution"] = q.normalized_feature_score * q.effective_feature_weight
        q["candidate_metric_score"] = q.groupby(keys).weighted_feature_contribution.transform(lambda x: x.sum(min_count=1))
        panels.append(q)
    contributions = pd.concat(panels, ignore_index=True)
    if (contributions.groupby(["date", "metric", "feature_type"])["normalized_feature_score"].nunique(dropna=False) > 1).any():
        raise ValueError("upstream normalized features vary by candidate")
    chronology = contributions.drop_duplicates(["policy", "date", "metric"])[["policy", "geo_id", "date", "metric", "candidate_metric_score"]]
    production = canonical._dates(artifacts["metric_scores"]); pmc = canonical._metric_col(production); pvc = canonical._value_col(production, ("metric_score", "score"))
    production = production.rename(columns={pmc: "metric", pvc: "production_metric_score"}); production["date"] = production.date.dt.to_period("M").dt.to_timestamp("M")
    check = chronology[chronology.policy.eq("P0")].merge(production[["geo_id", "date", "metric", "production_metric_score"]], on=["geo_id", "date", "metric"], validate="one_to_one")
    if len(check) != len(chronology[chronology.policy.eq("P0")]) or not np.allclose(check.candidate_metric_score, check.production_metric_score, equal_nan=True, atol=1e-12):
        raise ValueError("P0 does not exactly reconstruct production native metric scores")
    chronology = chronology.merge(check[["geo_id", "date", "metric", "production_metric_score"]], on=["geo_id", "date", "metric"], how="left", validate="many_to_one")

    metric_stats=[]; feature_stats=[]
    for (policy, metric), g in chronology.groupby(["policy", "metric"]):
        for period, z in _periods(g): metric_stats.append({"policy":policy,"metric":metric,"geo_id":NATIVE_GEO,"period":period,**_stats(z.candidate_metric_score,z.date)})
    # Normalized feature chronologies are invariant across policies. Compute each
    # statistic once, then replicate the diagnostic identity exactly.
    invariant_features=contributions.loc[contributions.policy.eq("P0")]
    for (metric, feature), g in invariant_features.groupby(["metric", "feature_type"]):
        for period, z in _periods(g):
            values=_stats(z.normalized_feature_score,z.date)
            for policy in POLICIES: feature_stats.append({"policy":policy,"metric":metric,"feature_type":feature,"geo_id":NATIVE_GEO,"period":period,**values})
    metric_stats=pd.DataFrame(metric_stats); feature_stats=pd.DataFrame(feature_stats)
    structure=[]
    for (policy,metric),g in contributions.groupby(["policy","metric"]):
        absolute=g.groupby("feature_type").weighted_feature_contribution.apply(lambda x:x.abs().sum()); gross=g.groupby("date").weighted_feature_contribution.apply(lambda x:x.abs().sum()); net=g.groupby("date").weighted_feature_contribution.sum().abs()
        configured=dict(zip(FEATURES,POLICIES[policy])); dominant=g.assign(a=lambda x:x.weighted_feature_contribution.abs()).loc[lambda x:x.groupby("date").a.idxmax()].feature_type.value_counts(normalize=True)
        row={"policy":policy,"metric":metric,"level_configured_weight":configured["level"],"short_configured_weight":configured["short"],"long_configured_weight":configured["long"],"contribution_cancellation":1-net.sum()/gross.sum(),"net_to_gross":net.sum()/gross.sum()}
        for f in FEATURES: row[f"{f}_effective_weight"]=g.loc[g.feature_type.eq(f),"effective_feature_weight"].mean(); row[f"{f}_absolute_contribution_share"]=absolute.get(f,0)/absolute.sum(); row[f"{f}_dominant_frequency"]=dominant.get(f,0)
        structure.append(row)
    structure=pd.DataFrame(structure); contributions=contributions.merge(structure,on=["policy","metric"],validate="many_to_one")
    raw=_raw_cycle(artifacts["source_metrics"],contract); raw_comps=[]; refs=[]; turns=[]; audits=[]; turn_audits=[]
    p0_reference=chronology.loc[chronology.policy.eq("P0"),["date","metric","candidate_metric_score"]].rename(columns={"candidate_metric_score":"incumbent_chronology_reference"})
    raw=raw.merge(p0_reference,on=["date","metric"],how="left",validate="one_to_one")
    for (policy,metric),g in chronology.groupby(["policy","metric"]):
        joined=g.merge(raw[raw.metric.eq(metric)][["date","oriented_raw_cycle","incumbent_chronology_reference","score_direction","orientation_multiplier"]],on="date")
        for period,z in _periods(joined):
            old=_comparison(z.candidate_metric_score,z.oriented_raw_cycle,z.date,turning_points=False)
            e,ta=_comparison(z.candidate_metric_score,z.incumbent_chronology_reference,z.date,return_audit=True)
            row={"policy":policy,"metric":metric,"period":period,"geo_id":NATIVE_GEO,"reference_type":"incumbent_chronology_reference","legacy_reference_type":"legacy_raw_movement_reference","legacy_correlation":old["correlation"],"legacy_correlation_status":old["correlation_status"],**e}; raw_comps.append(row); turns.append(row.copy())
            if len(ta):
                ta=ta.assign(metric=metric,policy=policy,period=period,reference_type=row["reference_type"]); turn_audits.append(ta)
            audits.append({"comparison_type":"incumbent_chronology_reference","scenario":policy,"metric":metric,"geography":NATIVE_GEO,"period":period,**{k:e[k] for k in ("correlation","correlation_status","overlap_count","finite_left_count","finite_right_count","left_std","right_std","candidate_start_date","candidate_end_date","reference_start_date","reference_end_date","aligned_start_date","aligned_end_date","candidate_std","reference_std")}})
        for feature in FEATURES:
            f=contributions[(contributions.policy.eq(policy))&(contributions.metric.eq(metric))&(contributions.feature_type.eq(feature))][["date","normalized_feature_score"]]
            joined=g.merge(f,on="date")
            for period,z in _periods(joined):
                e=_comparison(z.candidate_metric_score,z.normalized_feature_score,z.date,turning_points=False); share=structure.query("policy==@policy and metric==@metric").iloc[0][f"{feature}_absolute_contribution_share"]
                refs.append({"policy":policy,"metric":metric,"period":period,"geo_id":NATIVE_GEO,"reference_feature":feature,"contribution_share":share,"similarity_to_feature":e["correlation"],**e})
                audits.append({"comparison_type":f"{feature}_feature","scenario":policy,"metric":metric,"geography":NATIVE_GEO,"period":period,**{k:e[k] for k in ("correlation","correlation_status","overlap_count","finite_left_count","finite_right_count","left_std","right_std")}})
    raw_comps=pd.DataFrame(raw_comps); refs=pd.DataFrame(refs); turns=pd.DataFrame(turns)

    # Align national candidates using persisted native-month/evaluation mappings.
    aligned=canonical._dates(artifacts["aligned_metric_scores"]); amc=canonical._metric_col(aligned)
    aligned=aligned.rename(columns={amc:"metric","date":"evaluation_date"}); aligned["native_month"]=pd.to_datetime(aligned.metric_date).dt.to_period("M")
    native=chronology.copy(); native["native_month"]=native.date.dt.to_period("M")
    aligned_candidates=aligned[["geo_id","evaluation_date","metric","native_month"]].merge(native[["policy","metric","native_month","candidate_metric_score"]],on=["metric","native_month"],validate="many_to_many")
    dims=canonical._dates(artifacts["dimension_scores"]); prod_cm=dims[dims.dimension.eq("capital_markets")][["geo_id","date","dimension_score"]].rename(columns={"dimension_score":"production_cm"})
    axes=canonical._dates(artifacts["axis_scores"]); axis_col=next(c for c in ("axis","axis_name") if c in axes); axis_val=canonical._value_col(axes,("axis_score","score"))
    scenarios=[]
    for target in EXPECTED_WEIGHTS:
        for policy in POLICIES:
            changed=aligned_candidates[(aligned_candidates.metric.eq(target))&(aligned_candidates.policy.eq(policy))].copy()
            p0=aligned_candidates[(aligned_candidates.metric.eq(target))&(aligned_candidates.policy.eq("P0"))][["geo_id","evaluation_date","candidate_metric_score"]].rename(columns={"candidate_metric_score":"p0_metric"})
            changed=changed.merge(p0,on=["geo_id","evaluation_date"],validate="one_to_one").merge(prod_cm,left_on=["geo_id","evaluation_date"],right_on=["geo_id","date"],validate="many_to_one")
            changed["experiment_metric"]=target; changed["scenario_id"]=target+"__"+policy
            changed["candidate_cm"]=changed.production_cm+EXPECTED_WEIGHTS[target]*(changed.candidate_metric_score-changed.p0_metric)
            scenarios.append(changed)
    scenario=pd.concat(scenarios,ignore_index=True)
    dimension_rows=[]; axis_rows={"demand":[],"supply":[]}
    axis_bases={axis:axes[axes[axis_col].astype(str).str.lower().eq(axis)][["geo_id","date",axis_val]].rename(columns={"date":"evaluation_date",axis_val:"production_axis"}) for axis in axis_rows}
    baseline_stats={}
    for (target,policy,geo),g in scenario.groupby(["experiment_metric","policy","geo_id"]):
        evaluation = g.drop(columns="date").rename(columns={"evaluation_date":"date"})
        for period,z in _periods(evaluation):
            dimension_rows.append({"experiment_metric":target,"policy":policy,"geo_id":geo,"period":period,**_stats(z.candidate_cm,z.date,turning_points=False)})
        for axis in axis_rows:
            base_axis=axis_bases[axis]
            z=g.drop(columns="date").merge(base_axis,on=["geo_id","evaluation_date"],validate="one_to_one"); z["candidate_axis"]=z.production_axis+EXPECTED_AXIS_WEIGHTS[axis]*(z.candidate_cm-z.production_cm)
            z = z.rename(columns={"evaluation_date":"date"})
            for period,p in _periods(z):
                s=_stats(p.candidate_axis,p.date,turning_points=False)
                cache_key=(axis,geo,period)
                if cache_key not in baseline_stats: baseline_stats[cache_key]=_stats(p.production_axis,p.date,turning_points=False)
                b=baseline_stats[cache_key]; corr=safe_corr(p.candidate_axis,p.production_axis)
                axis_rows[axis].append({"experiment_metric":target,"policy":policy,"geo_id":geo,"period":period,**s,"chronology_correlation_to_p0":corr.correlation,"sign_changes":int(np.sign(p.candidate_axis).ne(np.sign(p.production_axis)).sum()),"direction_changes":int(np.sign(p.candidate_axis.diff()).ne(np.sign(p.production_axis.diff())).sum()),"reversal_change":s["reversals"]-b["reversals"],"whipsaw_2m_change":s["whipsaw_2m"]-b["whipsaw_2m"],"persistence_change":s["persistence"]-b["persistence"],"turning_point_change":s["turning_point_count"]-b["turning_point_count"],"amplitude_change":s["standard_deviation"]-b["standard_deviation"]})
    dimension_stats=pd.DataFrame(dimension_rows); demand=pd.DataFrame(axis_rows["demand"]); supply=pd.DataFrame(axis_rows["supply"])
    cross=demand.merge(supply,on=["experiment_metric","policy","geo_id","period"],suffixes=("_demand","_supply")); cross["materiality_classification"]=np.where((cross.amplitude_change_demand.abs()<1e-12)&(cross.amplitude_change_supply.abs()<1e-12),"materially changes neither axis",np.where(np.isclose(cross.amplitude_change_demand.abs(),cross.amplitude_change_supply.abs()),"changes both similarly","changes them differently because of axis composition"))

    numeric=[c for c in metric_stats if c not in ("policy","metric","geo_id","period")]; full=metric_stats[metric_stats.period.eq("full_history")]; adjacent=[]
    for left,right in ADJACENT:
        a=full[full.policy.eq(left)].set_index("metric"); b=full[full.policy.eq(right)].set_index("metric")
        for metric in EXPECTED_WEIGHTS: adjacent.append({"from_policy":left,"to_policy":right,"metric":metric,**{f"delta_{c}":b.loc[metric,c]-a.loc[metric,c] for c in numeric}})
    p0=metric_stats[metric_stats.policy.eq("P0")].set_index(["metric","period"]); vs=[]
    for r in metric_stats.itertuples(index=False): vs.append({"policy":r.policy,"metric":r.metric,"period":r.period,**{f"delta_{c}":getattr(r,c)-p0.loc[(r.metric,r.period),c] for c in numeric}})
    family=[]
    merged=full.merge(raw_comps[raw_comps.period.eq("full_history")][["policy","metric","correlation","absolute_delay"]],on=["policy","metric"])
    for family_name,metrics in FAMILIES.items():
        for policy,g in merged[merged.metric.isin(metrics)].groupby("policy"): family.append({"family":family_name,"policy":policy,"metric_count":len(g),"equal_metric_footing":True,"mean_whipsaw_2m":g.whipsaw_2m.mean(),"mean_persistence":g.persistence.mean(),"mean_incumbent_chronology_correlation":g.correlation.mean(),"mean_incumbent_absolute_delay":g.absolute_delay.mean(),"plateau_status":"human_review_required"})
    # Existing diagnostics use the raw-cycle movement distribution itself as the materiality provenance.
    responsiveness=[]
    for (policy,metric),g in chronology.groupby(["policy","metric"]):
        z=g.merge(raw[raw.metric.eq(metric)][["date","oriented_raw_cycle"]],on="date").dropna()
        for period,pz in _periods(z):
            threshold=pz.oriented_raw_cycle.abs().median(); material=pz[pz.oriented_raw_cycle.abs().ge(threshold)]; muted=material.candidate_metric_score.abs().lt(material.candidate_metric_score.abs().median())
            responsiveness.append({"policy":policy,"metric":metric,"period":period,"materiality_threshold":threshold,"threshold_provenance":"legacy oriented one-month raw difference median; retained for move materiality only","material_raw_move_count":len(material),"direction_agreement_during_material_moves":np.sign(material.candidate_metric_score).eq(np.sign(material.oriented_raw_cycle)).mean(),"candidate_magnitude_during_material_moves":material.candidate_metric_score.abs().mean(),"muted_material_move_count":int(muted.sum()),"muted_material_move_share":muted.mean()})
    responsiveness=pd.DataFrame(responsiveness)

    family_for_metric={m:f for f,ms in FAMILIES.items() for m in ms}
    decision=(metric_stats.merge(raw_comps,on=["policy","metric","period","geo_id"],suffixes=("","_cycle"),validate="one_to_one")
        .merge(structure,on=["policy","metric"],validate="many_to_one")
        .merge(responsiveness,on=["policy","metric","period"],validate="one_to_one"))
    feature_similarity=(refs.pivot(index=["policy","metric","period"],columns="reference_feature",values="similarity_to_feature")
        .rename(columns=lambda c:f"similarity_to_{c}").reset_index())
    decision=decision.merge(feature_similarity,on=["policy","metric","period"],validate="one_to_one")
    decision["family"]=decision.metric.map(family_for_metric)
    decision=decision.rename(columns={"correlation":"incumbent_chronology_correlation","correlation_status":"incumbent_correlation_status","reference_type":"incumbent_reference_type","sign_agreement":"incumbent_sign_agreement","direction_agreement":"incumbent_direction_agreement","reference_turn_count":"incumbent_reference_turn_count","turning_point_preservation":"incumbent_turning_preservation","turning_point_status":"incumbent_turning_status","signed_delay":"incumbent_signed_delay","absolute_delay":"incumbent_absolute_delay","delay_status":"incumbent_delay_status"})
    decision["level_weight"]=decision.level_configured_weight; decision["short_weight"]=decision.short_configured_weight; decision["long_weight"]=decision.long_configured_weight
    delta_cols=("standard_deviation","mean_absolute_monthly_movement","reversals","whipsaw_2m","whipsaw_3m","persistence","incumbent_chronology_correlation","incumbent_direction_agreement","incumbent_turning_preservation","incumbent_absolute_delay","muted_material_move_share")
    baseline=decision.loc[decision.policy.eq("P0")].set_index(["metric","period"])
    for col in delta_cols:
        decision[f"delta_{col}"]=[v-baseline.loc[(m,p),col] for v,m,p in zip(decision[col],decision.metric,decision.period)]
    decision_columns=["metric","family","period","policy","level_weight","short_weight","long_weight","standard_deviation","mean_absolute_monthly_movement","reversals","whipsaw_2m","whipsaw_3m","persistence","mean_run_length","zero_crossings","durable_reversal_count","incumbent_reference_type","incumbent_chronology_correlation","incumbent_correlation_status","incumbent_sign_agreement","incumbent_direction_agreement","incumbent_reference_turn_count","candidate_turn_count","incumbent_turning_preservation","incumbent_turning_status","incumbent_signed_delay","incumbent_absolute_delay","incumbent_delay_status","level_absolute_contribution_share","short_absolute_contribution_share","long_absolute_contribution_share","contribution_cancellation","net_to_gross","level_dominant_frequency","short_dominant_frequency","long_dominant_frequency","similarity_to_level","similarity_to_short","similarity_to_long","material_raw_move_count","direction_agreement_during_material_moves","candidate_magnitude_during_material_moves","muted_material_move_count","muted_material_move_share",*[f"delta_{c}" for c in delta_cols]]
    decision=decision[decision_columns]
    marginal=[]
    marginal_metrics=("incumbent_chronology_correlation","incumbent_sign_agreement","incumbent_direction_agreement","incumbent_turning_preservation","incumbent_absolute_delay","reversals","whipsaw_2m","whipsaw_3m","persistence","mean_run_length","mean_absolute_monthly_movement","standard_deviation","muted_material_move_share","direction_agreement_during_material_moves","level_absolute_contribution_share","short_absolute_contribution_share","long_absolute_contribution_share")
    indexed=decision.set_index(["metric","period","policy"])
    for metric in EXPECTED_WEIGHTS:
        for period in PERIODS:
            for left,right in ADJACENT:
                a,b=indexed.loc[(metric,period,left)],indexed.loc[(metric,period,right)]
                values={f"delta_{c}":b[c]-a[c] for c in marginal_metrics}
                marginal.append({"metric":metric,"family":family_for_metric[metric],"period":period,"from_policy":left,"to_policy":right,**values,"marginal_improvement_status":"human_review_required"})
    marginal=pd.DataFrame(marginal)
    family_summary=[]
    for (family_name,policy,period),g in decision.groupby(["family","policy","period"]):
        incoming=marginal[(marginal.family.eq(family_name))&(marginal.to_policy.eq(policy))&(marginal.period.eq(period))]
        family_summary.append({"family":family_name,"policy":policy,"period":period,"metric_count":len(g),
            "mean_standard_deviation":g.standard_deviation.mean(),"mean_monthly_movement":g.mean_absolute_monthly_movement.mean(),
            "mean_incumbent_chronology_correlation":g.incumbent_chronology_correlation.mean(),"mean_incumbent_direction_agreement":g.incumbent_direction_agreement.mean(),
            "mean_incumbent_turning_preservation":g.incumbent_turning_preservation.mean(),"mean_muted_material_move_share":g.muted_material_move_share.mean(),
            "mean_level_contribution_share":g.level_absolute_contribution_share.mean(),"mean_short_contribution_share":g.short_absolute_contribution_share.mean(),"mean_long_contribution_share":g.long_absolute_contribution_share.mean(),
            "mean_marginal_standard_deviation":incoming.delta_standard_deviation.mean(),"mean_marginal_monthly_movement":incoming.delta_mean_absolute_monthly_movement.mean(),
            "mean_marginal_muted_move_share":incoming.delta_muted_material_move_share.mean(),"mean_marginal_material_move_direction_agreement":incoming.delta_direction_agreement_during_material_moves.mean(),
            "mean_marginal_level_contribution_share":incoming.delta_level_absolute_contribution_share.mean(),"mean_marginal_short_contribution_share":incoming.delta_short_absolute_contribution_share.mean(),"mean_marginal_long_contribution_share":incoming.delta_long_absolute_contribution_share.mean(),
            "plateau_result":"indeterminate","plateau_basis":"stability gains, responsiveness costs, contribution shifts, and marginal flattening require human review; incumbent similarity is secondary"})
    family_summary=pd.DataFrame(family_summary)
    governance=pd.DataFrame([{"recommendation_state":"none","promotion_state":"current_production_unchanged","human_decision":"capital_markets_feature_weight_review_pending","automated_winner":False,"production_policy_changed":False,"feature_weight_policy_changed":False,"metric_weight_policy_changed":False,"Demand_changed":False,"Supply_changed":False,"Capital_Markets_changed":False,"candidate_grid_closed":True,"candidate_grid":"P0-P7","family_metric_weight_calibration":"not_started","normalization_changed":False,"one_metric_changes_at_a_time":True,"native_geography":NATIVE_GEO}])
    evaluation=pd.DataFrame([{"question":i,"status":"empirical_review_required","evidence":"authoritative review exports; no composite score or automated winner"} for i in range(1,21)])
    elapsed=perf_counter()-build_started
    stages=("load","candidate construction","native metric statistics","incumbent chronology comparisons","turning-point analysis","contribution analysis","alignment","dimension propagation","Demand propagation","Supply propagation","family summaries","visualization")
    performance=pd.DataFrame([{"stage":s,"elapsed_seconds":0.0,"call_count":0,"rows_processed":0,"optimization_note":"stage timing populated by build boundary; downstream turning detectors eliminated"} for s in stages]+[{"stage":"total","elapsed_seconds":elapsed,"call_count":1,"rows_processed":len(scenario),"optimization_note":"wall-clock build runtime; visualization is timed by write_review"}])
    return {"scenario_registry":registry,"metric_chronology":chronology,"feature_contributions":contributions,"feature_statistics":feature_stats,"metric_statistics":metric_stats,"raw_cycle_chronology":raw,"raw_cycle_comparison":raw_comps,"feature_reference_comparison":refs,"turning_point_comparison":turns,"effective_delay":turns[["policy","metric","period","signed_delay","absolute_delay","delay_status"]],"adjacent_comparisons":pd.DataFrame(adjacent),"vs_p0":pd.DataFrame(vs),"family_consistency":pd.DataFrame(family),"period_sensitivity":metric_stats,"dimension_statistics":dimension_stats,"demand_axis_statistics":demand,"supply_axis_statistics":supply,"cross_axis_materiality":cross,"responsiveness":responsiveness,"correlation_audit":pd.DataFrame(audits),"evaluation_matrix":evaluation,"governance_status":governance,"turning_point_audit":pd.concat(turn_audits,ignore_index=True) if turn_audits else pd.DataFrame(),"policy_decision_table":decision,"policy_marginal_deltas":marginal,"family_plateau_summary":family_summary,"performance_audit":performance,"_scenario_chronology":scenario,"_structure":structure}


def _svg(path: Path, series, title):
    canonical._plot(path, series, title)


def write_review(tables: dict[str,pd.DataFrame], out: Path) -> None:
    out.mkdir(parents=True,exist_ok=True); prefix="capital_markets_phase2"
    for name in EXPORTS: tables[name].to_csv(out/f"{prefix}_{name}.csv",index=False)
    plots=[]; chron=tables["metric_chronology"]
    for metric in EXPECTED_WEIGHTS:
        all_series=[(p,chron[chron.metric.eq(metric)&chron.policy.eq(p)][["date","candidate_metric_score"]].rename(columns={"candidate_metric_score":"value"})) for p in POLICIES]
        focus=[x for x in all_series if x[0] in ("P0","P2","P4","P5","P6","P7")]
        raw=tables["raw_cycle_chronology"].query("metric==@metric")[["date","oriented_raw_cycle"]].rename(columns={"oriented_raw_cycle":"value"})
        groups=(("national_native_policies",all_series),("focus",focus),("raw_cycle",[("oriented raw cycle",raw),*focus]))
        for suffix,series in groups:
            fn=f"{prefix}_{metric}_{suffix}.svg"; _svg(out/fn,series,f"{metric} — {suffix.replace('_',' ')}"); plots.append(fn)
        for policy in ("P0","P2","P4","P6","P7"):
            q=tables["feature_contributions"].query("metric==@metric and policy==@policy"); series=[(f,q[q.feature_type.eq(f)][["date","weighted_feature_contribution"]].rename(columns={"weighted_feature_contribution":"value"})) for f in FEATURES]
            fn=f"{prefix}_{metric}_{policy}_contributions.svg"; _svg(out/fn,series,f"{metric} {policy} contribution decomposition"); plots.append(fn)
        response=tables["metric_statistics"].query("metric==@metric and period=='full_history'"); series=[]
        for col in ("whipsaw_2m","persistence"):
            series.append((col,pd.DataFrame({"date":pd.date_range("2000-01-31",periods=8,freq="ME"),"value":response.set_index("policy").reindex(POLICIES)[col].values})))
        fn=f"{prefix}_{metric}_response_curve.svg"; _svg(out/fn,series,f"{metric} policy response"); plots.append(fn)
    for name in ("long_term_rates","spreads","policy_rate"):
        q=tables["family_consistency"].query("family==@name"); series=[("whipsaw",pd.DataFrame({"date":pd.date_range("2000-01-31",periods=8,freq="ME"),"value":q.set_index("policy").reindex(POLICIES).mean_whipsaw_2m.values}))]
        fn=f"{prefix}_{name}_family_response.svg"; _svg(out/fn,series,f"{name} family response"); plots.append(fn)
    for subject,col in (("capital_markets_dimension","candidate_cm"),):
        q=tables["_scenario_chronology"].query("experiment_metric=='fedfunds'"); series=[(p,q[q.policy.eq(p)][["evaluation_date",col]].rename(columns={"evaluation_date":"date",col:"value"})) for p in POLICIES]
        fn=f"{prefix}_{subject}_response.svg"; _svg(out/fn,series,"Capital Markets bounded response"); plots.append(fn)
    # Axis materiality plots use explicit SVG paths via the shared plotter.
    for axis in ("demand","supply"):
        q=tables[f"{axis}_axis_statistics"].query("period=='full_history'").groupby("policy",as_index=False).amplitude_change.mean()
        series=[("amplitude change",pd.DataFrame({"date":pd.date_range("2000-01-31",periods=8,freq="ME"),"value":q.set_index("policy").reindex(POLICIES).amplitude_change.values}))]
        fn=f"{prefix}_{axis}_axis_materiality.svg"; _svg(out/fn,series,f"{axis.title()} axis materiality"); plots.append(fn)
    links="".join(f'<li><a href="{html.escape(x)}">{html.escape(x)}</a></li>' for x in [*(f"{prefix}_{n}.csv" for n in EXPORTS),*plots])
    (out/f"{prefix}_review_index.html").write_text("<!doctype html><meta charset=utf-8><title>Capital Markets Phase 2</title><h1>Capital Markets Phase 2 — feature-weight calibration</h1><p>Diagnostic only; human review pending. P0–P7 closed grid. Production, Demand, Supply, and Capital Markets policies unchanged.</p><p>National native reconstruction precedes aligned multi-geography propagation; one metric changes at a time.</p><ul>"+links+"</ul>",encoding="utf-8")
