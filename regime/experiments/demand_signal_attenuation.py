"""Governed, diagnostic-only Demand signal attenuation review.

The builder reads an immutable persisted run and the active registries.  It has
no production write path and deliberately fails before creating its output
directory when identity, scope, registry contracts, or reconstruction parity
are not exact.
"""
from __future__ import annotations

from hashlib import sha256
from pathlib import Path
import html
import time

import numpy as np
import pandas as pd

from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points
from regime.experiments.demand_labor_finalist import reversal_events

TOL = 1e-12
RUN_ID = "macro_regime_v1_0_1_candidate_20260810"
GEOS = (
    "district_of_columbia_dc__county", "essex_county_nj__county",
    "montgomery_county_md__county", "prince_george_s_county_md__county",
    "fairfax_county_va__county", "san_francisco_county_ca__county",
    "los_angeles_county_ca__county",
)
CORE_DEMAND = ("population", "median_household_income", "gdp_annual",
               "labor_force", "employment", "laus_unemployment_rate")
STRUCTURAL = CORE_DEMAND[:3]
LABOR = CORE_DEMAND[3:]
LAUS_REGISTRY = {"laus_labor_force": "labor_force", "laus_employment": "employment",
                 "laus_unemployment_rate": "laus_unemployment_rate"}
DEMAND_DIMENSIONS = ("demand", "price", "affordability", "capital_markets")
WEIGHT_POLICIES = {
    "LAUS-W-25-35-40": (.25, .35, .40), "LAUS-W-40-30-30": (.40, .30, .30),
    "LAUS-W-50-25-25": (.50, .25, .25), "LAUS-W-60-20-20": (.60, .20, .20),
    "LAUS-W-70-15-15": (.70, .15, .15), "LAUS-W-80-10-10": (.80, .10, .10),
}
FEATURE_TYPES = ("level", "short", "long")


def cancellation(values: pd.Series) -> tuple[float, float, float]:
    """Return gross, net magnitude and cancellation; missing is unavailable."""
    valid = pd.to_numeric(values, errors="coerce").dropna()
    if valid.empty:
        return np.nan, np.nan, np.nan
    gross = float(valid.abs().sum()); net = float(abs(valid.sum()))
    return gross, net, (1.0 - net / gross if gross != 0 else np.nan)


def effective_contributions(scores: pd.Series, weights: pd.Series) -> pd.DataFrame:
    """Apply production missingness renormalization (unavailable is not zero)."""
    score = pd.to_numeric(scores, errors="coerce")
    configured = pd.to_numeric(weights, errors="raise")
    denom = float(configured.loc[score.notna()].sum())
    effective = pd.Series(np.nan, index=score.index, dtype=float)
    contribution = pd.Series(np.nan, index=score.index, dtype=float)
    if denom > 0:
        effective.loc[score.notna()] = configured.loc[score.notna()] / denom
        contribution.loc[score.notna()] = score.loc[score.notna()] * effective.loc[score.notna()]
    return pd.DataFrame({"effective_feature_weight": effective,
                         "weighted_feature_contribution": contribution})


def recent_36(frame: pd.DataFrame) -> pd.DataFrame:
    end = pd.to_datetime(frame["date"]).max()
    return frame.loc[pd.to_datetime(frame["date"]) >= end - pd.DateOffset(months=35)].copy()


def _col(frame: pd.DataFrame, *names: str) -> str:
    for name in names:
        if name in frame.columns:
            return name
    raise ValueError(f"required column missing; expected one of {names}")


def _load(run: Path, stem: str) -> pd.DataFrame:
    paths = sorted(run.glob(f"**/{stem}.parquet"))
    if len(paths) != 1:
        raise FileNotFoundError(f"{run}: require exactly one {stem}.parquet, found {len(paths)}")
    return pd.read_parquet(paths[0])


def _scope(frame: pd.DataFrame, stem: str, grain: list[str]) -> pd.DataFrame:
    geo = _col(frame, "geo_id", "geography_id"); date = _col(frame, "date", "evaluation_date")
    out = frame.rename(columns={geo: "geo_id", date: "date"}).copy()
    out["geo_id"] = out.geo_id.astype(str)
    out["date"] = pd.to_datetime(out.date).astype("datetime64[ns]")
    missing = set(GEOS) - set(out.geo_id)
    if missing: raise ValueError(f"{stem}: governed geography coverage missing={sorted(missing)}")
    out = out.loc[out.geo_id.isin(GEOS)].copy()
    if set(out.geo_id) != set(GEOS): raise ValueError(f"{stem}: exact seven-county scope failed")
    if out.geo_id.str.contains("cbsa|metro|__zip", case=False, regex=True).any():
        raise ValueError(f"{stem}: CBSA/ZIP leakage")
    if out.duplicated(grain).any(): raise ValueError(f"{stem}: duplicate governed grain")
    return out


def production_contract(root: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    fr = pd.read_csv(root / "config/feature_registry.csv")
    mr = pd.read_csv(root / "config/metric_dimension_registry.csv")
    ar = pd.read_csv(root / "config/axis_registry.csv")
    active = mr.loc[mr["enabled"].astype(bool)].copy()
    demand = active.loc[active.dimension.str.lower().eq("demand")]
    canonical = demand[_col(demand, "canonical_metric_key", "metric_key")].replace(LAUS_REGISTRY)
    if set(canonical) != set(CORE_DEMAND): raise ValueError("active core Demand membership drift")
    axis = ar.loc[ar["enabled"].astype(bool) & ar.axis.str.lower().eq("demand")]
    if set(axis.dimension.str.lower()) != set(DEMAND_DIMENSIONS): raise ValueError("Demand-axis membership drift")
    laus = fr.loc[fr.metric_key.isin(LAUS_REGISTRY)].copy()
    if len(laus) != 9: raise ValueError("LAUS contract must have nine features")
    expected = {"level": ("ma_level", .25, "6m"),
                "short_term_change": ("ma_pct_change", .35, "6m/lag3m"),
                "long_term_change": ("ma_pct_change", .40, "6m/lag12m")}
    for row in laus.itertuples():
        transform, weight, window = expected[row.feature_type]
        if (row.transform, float(row.feature_weight), str(row.feature_window)) != (transform, weight, window):
            raise ValueError(f"settled LAUS feature contract drift: {row.feature_key}")
    return fr, active, ar.loc[ar["enabled"].astype(bool)].copy()


def _feature_panel(run: Path, fr: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = _load(run, "normalized_features")
    feature = _col(raw, "feature_key"); score = _col(raw, "feature_score", "normalized_feature_score", "normalized_value")
    raw = raw.rename(columns={feature: "feature_key", score: "normalized_feature_score"})
    raw = _scope(raw, "normalized_features", ["geo_id", "date", "feature_key"])
    meta = fr[["feature_key", "metric_key", "feature_type", "feature_weight"]].drop_duplicates("feature_key")
    panel = raw.merge(meta, on="feature_key", how="inner", validate="many_to_one")
    panel["metric"] = panel.metric_key.replace(LAUS_REGISTRY)
    panel = panel.loc[panel.metric.isin(CORE_DEMAND)].copy()
    panel["feature_type"] = panel.feature_type.replace({"short_term_change":"short", "long_term_change":"long"})
    panel = panel.rename(columns={"feature_weight":"configured_feature_weight"})
    chunks=[]
    for _, group in panel.groupby(["geo_id","date","metric"], sort=False):
        calculated=effective_contributions(group.normalized_feature_score,group.configured_feature_weight)
        q=group.copy(); q[calculated.columns]=calculated.to_numpy(); chunks.append(q)
    panel=pd.concat(chunks,ignore_index=True)
    metrics=_load(run,"aligned_metric_scores"); metric=_col(metrics,"canonical_metric_key","metric_key","metric")
    value=_col(metrics,"aligned_metric_score","metric_score","score")
    metrics=metrics.rename(columns={metric:"metric",value:"metric_score"}); metrics["metric"]=metrics.metric.replace(LAUS_REGISTRY)
    metrics=_scope(metrics,"aligned_metric_scores",["geo_id","date","metric"])
    panel=panel.merge(metrics[["geo_id","date","metric","metric_score"]],on=["geo_id","date","metric"],how="left",validate="many_to_one")
    replay=panel.groupby(["geo_id","date","metric"],dropna=False).weighted_feature_contribution.sum(min_count=1).rename("replay").reset_index()
    replay=replay.merge(metrics,on=["geo_id","date","metric"],validate="one_to_one")
    return panel, replay


def _contribution_layer(
    scores: pd.DataFrame,
    registry: pd.DataFrame,
    parent: str,
    child: str,
    value: str,
    weight: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:

    governed = (
        registry[
            [parent, child, weight]
        ]
        .copy()
    )

    # A child may legitimately belong to more than one parent.
    # Example: capital_markets contributes to both Demand and Supply.
    #
    # The governed contract must therefore be unique on the
    # parent-child membership pair, not on child alone.
    duplicate_pairs = governed.duplicated(
        [parent, child],
        keep=False,
    )

    if duplicate_pairs.any():
        raise ValueError(
            "duplicate governed parent-child membership detected: "
            + governed.loc[
                duplicate_pairs,
                [parent, child],
            ]
            .drop_duplicates()
            .to_dict("records")
            .__str__()
        )

    detail = scores.merge(
        governed,
        on=child,
        how="inner",
        validate="many_to_many",
    )
    chunks=[]
    for _,g in detail.groupby(["geo_id","date",parent],sort=False):
        calc=effective_contributions(g[value],g[weight]); q=g.copy()
        q["effective_weight"]=calc.effective_feature_weight.to_numpy()
        q["contribution"]=calc.weighted_feature_contribution.to_numpy(); chunks.append(q)
    detail=pd.concat(chunks,ignore_index=True)
    monthly=[]
    for keys,g in detail.groupby(["geo_id","date",parent]):
        gross,net,cancel=cancellation(g.contribution); valid=g.loc[g.contribution.notna()].copy()
        dominant=valid.loc[valid.contribution.abs().idxmax()] if len(valid) else None
        monthly.append({"geo_id":keys[0],"date":keys[1],parent:keys[2],"gross_contribution":gross,
          "net_score":g.contribution.sum(min_count=1),"cancellation_index":cancel,"net_to_gross_ratio":net/gross if gross else np.nan,
          f"dominant_{child}":dominant[child] if dominant is not None else None,
          f"dominant_{child}_abs_share":abs(dominant.contribution)/gross if dominant is not None and gross else np.nan,
          f"positive_{child}_count":int((valid.contribution>0).sum()),f"negative_{child}_count":int((valid.contribution<0).sum())})
    return detail,pd.DataFrame(monthly)


def _summary(
    frame: pd.DataFrame,
    keys: list[str],
    cancellation_col: str = "cancellation_index",
) -> pd.DataFrame:

    if "net_score" in frame.columns:
        value_col = "net_score"
    elif "net_contribution" in frame.columns:
        value_col = "net_contribution"
    else:
        raise ValueError(
            "attenuation summary requires either "
            "`net_score` or `net_contribution`"
        )

    rows = []

    for period, q in (
        ("full_history", frame),
        ("recent_36_months", recent_36(frame)),
    ):
        for group, g in q.groupby(
            keys,
            dropna=False,
        ):
            group = (
                (group,)
                if not isinstance(group, tuple)
                else group
            )

            rows.append(
                dict(
                    zip(keys, group),
                    period=period,
                    observations=len(g),
                    median_cancellation_index=(
                        g[cancellation_col].median()
                    ),
                    p90_cancellation_index=(
                        g[cancellation_col].quantile(0.9)
                    ),
                    median_net_to_gross_ratio=(
                        g["net_to_gross_ratio"].median()
                    ),
                    score_std=g[value_col].std(),
                    median_abs_score=(
                        g[value_col].abs().median()
                    ),
                )
            )

    return pd.DataFrame(rows)


def _reversal_rate(frame: pd.DataFrame, value: str, horizon: int) -> float:
    moves=frame.sort_values(["geo_id","date"])[["geo_id","date",value]].copy()
    moves[value]=moves.groupby("geo_id")[value].diff()
    events=reversal_events(moves.rename(columns={value:"move"}),"move",(horizon,))
    return float(events.reversed.mean()) if len(events) else np.nan


def build_review(run: Path, output: Path, root: Path | None = None) -> Path:
    """Build all governed exports after all parity gates have passed."""
    started=time.time(); run=run.resolve(); root=(root or Path(__file__).resolve().parents[2]).resolve()
    if run.name != RUN_ID: raise ValueError(f"authoritative run identity must be {RUN_ID}")
    if not run.is_dir(): raise FileNotFoundError(f"authoritative run absent: {run}")
    fr,mr,ar=production_contract(root)
    features,replay=_feature_panel(run,fr)
    incumbent=replay.loc[replay.metric.isin(LABOR)]
    metric_error=float((incumbent.replay-incumbent.metric_score).abs().max())

    # Registry-driven metric -> dimension reconstruction.
    metric_col=_col(mr,"canonical_metric_key","metric_key"); mr=mr.rename(columns={metric_col:"metric"})
    mr["metric"]=mr.metric.replace(LAUS_REGISTRY); mr["dimension"]=mr.dimension.str.lower()
    metric_weight=_col(mr,"metric_weight","weight")
    # Source-precedence rows may share one canonical persisted metric.  At the
    # scoring boundary they are one metric and must carry one governed weight.
    conflicts=mr.groupby(["dimension","metric"])[metric_weight].nunique()
    if conflicts.gt(1).any(): raise ValueError("canonical metric has conflicting governed weights")
    mr=mr.drop_duplicates(["dimension","metric"])
    scores=_load(run,"aligned_metric_scores"); mc=_col(scores,"canonical_metric_key","metric_key","metric"); sv=_col(scores,"aligned_metric_score","metric_score","score")
    scores=scores.rename(columns={mc:"metric",sv:"score"}); scores["metric"]=scores.metric.replace(LAUS_REGISTRY)
    scores=_scope(scores,"aligned_metric_scores",["geo_id","date","metric"])
    dim_detail,dim_monthly=_contribution_layer(scores,mr,"dimension","metric","score",metric_weight)
    persisted_dim=_load(run,"dimension_scores"); dc=_col(persisted_dim,"dimension"); dv=_col(persisted_dim,"dimension_score","score")
    persisted_dim=persisted_dim.rename(columns={dc:"dimension",dv:"persisted"}); persisted_dim["dimension"]=persisted_dim.dimension.str.lower()
    persisted_dim=_scope(persisted_dim,"dimension_scores",["geo_id","date","dimension"])
    dim_parity=dim_monthly.merge(persisted_dim,on=["geo_id","date","dimension"])
    demand_dim_error=float((dim_parity.net_score-dim_parity.persisted).abs().max())

    # Registry-driven dimension -> axis reconstruction for both governed axes.
    ar=ar.copy(); ar["axis"]=ar.axis.str.lower(); ar["dimension"]=ar.dimension.str.lower(); axis_weight=_col(ar,"dimension_weight","weight")
    axis_detail,axis_monthly=_contribution_layer(persisted_dim.rename(columns={"persisted":"score"}),ar,"axis","dimension","score",axis_weight)
    persisted_axis=_load(run,"axis_scores"); ac=_col(persisted_axis,"axis"); av=_col(persisted_axis,"axis_score","score")
    persisted_axis=persisted_axis.rename(columns={ac:"axis",av:"persisted"}); persisted_axis["axis"]=persisted_axis.axis.str.lower()
    persisted_axis=_scope(persisted_axis,"axis_scores",["geo_id","date","axis"])
    axis_parity=axis_monthly.merge(persisted_axis,on=["geo_id","date","axis"])
    errors={"incumbent_laus_metric_replay":metric_error,"demand_dimension_reconstruction":demand_dim_error}
    for axis in ("demand","supply"):
        q=axis_parity.loc[axis_parity.axis.eq(axis)]
        errors[f"{axis}_axis_reconstruction"]=float((q.net_score-q.persisted).abs().max()) if len(q) else np.nan
    if any(not np.isfinite(v) or v>TOL for v in errors.values()):
        raise ValueError(f"production parity failed; analytical evidence suppressed: {errors}")

    # Feature cancellation.
    metric_monthly=[]
    for keys,g in features.groupby(["geo_id","date","metric"]):
        gross,net,cancel=cancellation(g.weighted_feature_contribution); valid=g.loc[g.weighted_feature_contribution.notna()]
        dominant=valid.loc[valid.weighted_feature_contribution.abs().idxmax()] if len(valid) else None
        signs=np.sign(valid.weighted_feature_contribution); majority=np.sign(valid.weighted_feature_contribution.sum())
        disagree=int((signs.ne(majority)&signs.ne(0)).sum()) if majority else int(signs.ne(0).sum())
        metric_monthly.append({"geo_id":keys[0],"date":keys[1],"metric":keys[2],"gross_contribution":gross,
          "net_contribution":net,"cancellation_index":cancel,"net_to_gross_ratio":net/gross if gross else np.nan,
          "sign_disagreement_count":disagree,"sign_disagreement_share":disagree/len(valid) if len(valid) else np.nan,
          "dominant_feature":dominant.feature_key if dominant is not None else None,
          "dominant_feature_abs_share":abs(dominant.weighted_feature_contribution)/gross if dominant is not None and gross else np.nan,
          "gross_feature_magnitude":gross,"net_metric_magnitude":net,"metric_score":g.metric_score.iloc[0]})
    metric_monthly=pd.DataFrame(metric_monthly)

    # LAUS policy replay, retaining exact production availability semantics.
    laus=features.loc[features.metric.isin(LABOR)].copy(); policies=[]; policy_chron=[]
    for policy,w in WEIGHT_POLICIES.items():
        for keys,g in laus.groupby(["geo_id","date","metric"]):
            weights=g.feature_type.map(dict(zip(FEATURE_TYPES,w))); calc=effective_contributions(g.normalized_feature_score,weights)
            contributions=calc.weighted_feature_contribution; gross,net,cancel=cancellation(contributions)
            policy_chron.append({"policy":policy,"geo_id":keys[0],"date":keys[1],"metric":keys[2],"metric_score":contributions.sum(min_count=1),"gross":gross,"cancellation_index":cancel,"net_to_gross_ratio":net/gross if gross else np.nan})
    policy_chron=pd.DataFrame(policy_chron)
    for (policy,metric),g in policy_chron.groupby(["policy","metric"]):
        turn_count=matched_count=0; lags=[]
        for geo,q in g.groupby("geo_id"):
            candidate=detect_turning_points(q[["date","metric_score"]].dropna().sort_values("date"),"metric_score")
            reference=policy_chron.loc[policy_chron.policy.eq("LAUS-W-25-35-40") & policy_chron.metric.eq(metric) & policy_chron.geo_id.eq(geo)]
            reference=detect_turning_points(reference[["date","metric_score"]].dropna().sort_values("date"),"metric_score")
            candidate=candidate.loc[candidate.qualified.eq(True)] if len(candidate) else candidate
            reference=reference.loc[reference.qualified.eq(True)] if len(reference) else reference
            turn_count += len(candidate)
            if len(candidate) and len(reference):
                matched=match_turning_points(reference,candidate)
                if len(matched):
                    lag_col=next((c for c in ("signed_delay_months","lag_months","turn_lag_months","absolute_lag_months") if c in matched),None)
                    match_col=next((c for c in ("matched","is_matched") if c in matched),None)
                    matched_count += int(matched[match_col].sum()) if match_col else len(matched)
                    if lag_col: lags.extend(pd.to_numeric(matched[lag_col],errors="coerce").dropna().abs().tolist())
        policies.append({"policy":policy,"metric":metric,"median_absolute_metric_score":g.metric_score.abs().median(),"metric_score_standard_deviation":g.metric_score.std(),
          "reversal_rate_1m":_reversal_rate(g,"metric_score",1),"reversal_rate_3m":_reversal_rate(g,"metric_score",3),"reversal_rate_6m":_reversal_rate(g,"metric_score",6),
          "same_sign_persistence":1-_reversal_rate(g,"metric_score",1),"turning_point_count":turn_count,"matched_turning_point_count":matched_count,"unmatched_turning_point_count":turn_count-matched_count,
          "median_turn_lag":np.median(lags) if lags else np.nan,"p90_turn_lag":np.quantile(lags,.9) if lags else np.nan,"cancellation_index":g.cancellation_index.median(),"net_gross_ratio":g.net_to_gross_ratio.median(),
          "recent_36m_volatility":recent_36(g).metric_score.std()})
    sensitivity=pd.DataFrame(policies)

    behavior=[]
    for (metric,ft),g in laus.groupby(["metric","feature_type"]):
        x=g.sort_values(["geo_id","date"]); delta=x.groupby("geo_id").normalized_feature_score.diff()
        behavior.append({"metric":metric,"feature_type":ft,"mean_absolute_normalized_score":x.normalized_feature_score.abs().mean(),
          "standard_deviation":x.normalized_feature_score.std(),"median_absolute_weighted_contribution":x.weighted_feature_contribution.abs().median(),
          "p90_absolute_weighted_contribution":x.weighted_feature_contribution.abs().quantile(.9),"reversal_rate_1m":_reversal_rate(x,"normalized_feature_score",1),
          "reversal_rate_3m":_reversal_rate(x,"normalized_feature_score",3),"reversal_rate_6m":_reversal_rate(x,"normalized_feature_score",6),
          "same_sign_persistence":1-_reversal_rate(x,"normalized_feature_score",1),"rolling_36m_volatility":x.groupby("geo_id").normalized_feature_score.rolling(36,min_periods=2).std().median(),
          "share_gross_contribution":x.weighted_feature_contribution.abs().sum()/laus.loc[laus.metric.eq(metric)].weighted_feature_contribution.abs().sum(),
          "share_net_movement_attribution":delta.abs().sum()/laus.loc[laus.metric.eq(metric)].groupby(["geo_id","feature_type"]).normalized_feature_score.diff().abs().sum()})
    behavior=pd.DataFrame(behavior)
    pairwise=[]
    wide=laus.pivot_table(index=["geo_id","date","metric"],columns="feature_type",values="normalized_feature_score").reset_index()
    for metric,g in wide.groupby("metric"):
        for a,b in (("level","short"),("level","long"),("short","long")):
            pairwise.append({"metric":metric,"feature_a":a,"feature_b":b,"score_correlation":g[a].corr(g[b]),"sign_agreement":(np.sign(g[a])==np.sign(g[b])).where(g[a].notna()&g[b].notna()).mean()})
    pairwise=pd.DataFrame(pairwise)

    structural=dim_detail.loc[dim_detail.dimension.eq("demand") & dim_detail.metric.isin(CORE_DEMAND)].copy()
    svl=[]
    for keys,g in structural.groupby(["geo_id","date"]):
        s=g.loc[g.metric.isin(STRUCTURAL)].contribution; l=g.loc[g.metric.isin(LABOR)].contribution
        sg,sn,sc=cancellation(s); lg,ln,lc=cancellation(l); total_gross=sg+lg; total_net=sn+ln
        svl.append({"geo_id":keys[0],"date":keys[1],"gross_structural_contribution":sg,"net_structural_contribution":s.sum(min_count=1),
          "gross_labor_contribution":lg,"net_labor_contribution":l.sum(min_count=1),"structural_cancellation_index":sc,"labor_cancellation_index":lc,
          "structural_vs_labor_sign_disagreement":np.sign(s.sum())!=np.sign(l.sum()),"structural_share_of_gross_contribution":sg/total_gross if total_gross else np.nan,
          "labor_share_of_gross_contribution":lg/total_gross if total_gross else np.nan,"structural_share_of_net_movement":sn/total_net if total_net else np.nan,"labor_share_of_net_movement":ln/total_net if total_net else np.nan})
    svl=pd.DataFrame(svl)

    # Benchmark and retention use compatible within-layer magnitudes only.
    benchmark=[]; county=[]
    for axis,g in axis_parity.loc[axis_parity.axis.isin(["demand","supply"])].groupby("axis"):
        for geo,q in [("POOLED",g),*g.groupby("geo_id")]:
            row={"axis":axis,"geo_id":geo,"score_standard_deviation":q.persisted.std(),"median_absolute_score":q.persisted.abs().median(),"p90_absolute_score":q.persisted.abs().quantile(.9),"max_absolute_score":q.persisted.abs().max(),"median_cancellation_index":q.cancellation_index.median(),"p90_cancellation_index":q.cancellation_index.quantile(.9),"net_gross_ratio":q.net_to_gross_ratio.median(),"reversal_rate_1m":_reversal_rate(q,"persisted",1),"reversal_rate_3m":_reversal_rate(q,"persisted",3),"reversal_rate_6m":_reversal_rate(q,"persisted",6),"same_sign_persistence":1-_reversal_rate(q,"persisted",1),"zero_crossing_count":int((np.sign(q.persisted)!=np.sign(q.persisted.shift())).sum()-1)}
            (benchmark if geo=="POOLED" else county).append(row)
    benchmark=pd.DataFrame(benchmark); county=pd.DataFrame(county)
    retention=[]
    for geo in GEOS:
        fg=features.loc[features.geo_id.eq(geo)].weighted_feature_contribution.abs().median(); mn=metric_monthly.loc[metric_monthly.geo_id.eq(geo)].net_metric_magnitude.median()
        dn=dim_monthly.loc[dim_monthly.geo_id.eq(geo)].net_score.abs().median(); an=axis_monthly.loc[axis_monthly.geo_id.eq(geo)&axis_monthly.axis.eq("demand")].net_score.abs().median()
        retention.append({"geo_id":geo,"magnitude_statistic":"median_absolute","feature_gross_magnitude":fg,"metric_net_magnitude":mn,"dimension_net_magnitude":dn,"axis_net_magnitude":an,"metric_retention":mn/fg if fg else np.nan,"dimension_retention":dn/mn if mn else np.nan,"axis_retention":an/dn if dn else np.nan})
    retention=pd.DataFrame(retention)

    parity=pd.DataFrame([{"check":k,"max_abs_error":v,"tolerance":TOL,"status":"pass"} for k,v in errors.items()])
    registry=pd.DataFrame([{"policy":p,"level_weight":w[0],"short_weight":w[1],"long_weight":w[2],"configured_weight_sum":sum(w),"effective_weight_sum":sum(v/sum(w) for v in w),"decision":"pending"} for p,w in WEIGHT_POLICIES.items()])
    governance=pd.DataFrame([{"recommendation_state":"none","promotion_state":"none","human_decision":"pending","automated_winner":False,"production_policy_changed":False}])
    decision=pd.DataFrame([{"policy":p,"scope":"production" if p=="INCUMBENT-DEMAND" else "LAUS feature-weight replay","Decision":"pending"} for p in ("INCUMBENT-DEMAND",*WEIGHT_POLICIES)])
    county_sensitivity=sensitivity.merge(policy_chron.groupby(["policy","metric","geo_id"]).agg(median_absolute_metric_score=("metric_score",lambda s:s.abs().median()),cancellation_index=("cancellation_index","median")).reset_index(),on=["policy","metric"],suffixes=("_pooled",""))
    dc="district_of_columbia_dc__county"; dc_recent=axis_parity.loc[axis_parity.geo_id.eq(dc)&axis_parity.date.ge(pd.Timestamp("2023-08-01")),["date","axis","persisted"]].pivot(index="date",columns="axis",values="persisted").reset_index()
    chronology=laus.loc[laus.date.ge(pd.Timestamp("2023-08-01"))].copy()
    runtime=pd.DataFrame([{"run_id":RUN_ID,"governed_counties":len(GEOS),"elapsed_seconds":time.time()-started,"deterministic_outputs":True}])
    exports={
      "demand_attenuation_feature_contributions":features[["geo_id","date","metric","feature_key","feature_type","normalized_feature_score","configured_feature_weight","effective_feature_weight","weighted_feature_contribution","metric_score"]],
      "demand_attenuation_metric_cancellation_monthly":metric_monthly,"demand_attenuation_metric_cancellation_summary":_summary(metric_monthly,["metric"]),
      "demand_attenuation_laus_feature_behavior":behavior,"demand_attenuation_laus_feature_pairwise":pairwise,"demand_attenuation_laus_recent_chronology":chronology,
      "demand_attenuation_laus_weight_policy_registry":registry,"demand_attenuation_laus_weight_sensitivity":sensitivity,"demand_attenuation_laus_weight_county_summary":county_sensitivity,"demand_attenuation_laus_weight_recent_chronology":policy_chron.loc[policy_chron.date.ge(pd.Timestamp("2023-08-01"))],
      "demand_attenuation_dimension_contributions":dim_detail,"demand_attenuation_dimension_cancellation_monthly":dim_monthly,"demand_attenuation_dimension_summary":_summary(dim_monthly,["dimension"]),"demand_attenuation_structural_vs_labor":svl,
      "demand_attenuation_axis_contributions":axis_detail,"demand_attenuation_axis_cancellation_monthly":axis_monthly,"demand_attenuation_axis_summary":_summary(axis_monthly,["axis"]),
      "demand_attenuation_demand_vs_supply":benchmark,"demand_attenuation_demand_vs_supply_by_county":county,"demand_attenuation_hierarchy_retention":retention,"demand_attenuation_dc_recent_chronology":dc_recent,
      "demand_attenuation_parity_audit":parity,"demand_attenuation_decision_matrix":decision,"demand_attenuation_governance_status":governance,"demand_attenuation_runtime_summary":runtime,
    }
    output.mkdir(parents=True,exist_ok=False)
    for name,frame in exports.items(): frame.to_csv(output/f"{name}.csv",index=False,date_format="%Y-%m-%d",float_format="%.15g")
    order=["Executive attenuation summary","Demand vs Supply dynamic-range comparison","Feature-level cancellation","LAUS Level / Short / Long behavior","LAUS feature-weight sensitivity","Metric → dimension attenuation","Structural vs labor decomposition","Dimension → Demand-axis attenuation","Seven-county consistency","DC recent chronology","Decision matrix","Governance / parity / runtime"]
    sections="".join(f"<section><h2>{html.escape(title)}</h2><p>See governed CSV exports.</p></section>" for title in order)
    (output/"demand_attenuation_review.html").write_text(f"<!doctype html><meta charset='utf-8'><title>Demand Signal Attenuation</title><h1>Diagnostic only — no winner</h1>{sections}",encoding="utf-8")
    return output
