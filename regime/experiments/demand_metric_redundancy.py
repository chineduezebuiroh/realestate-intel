"""Governed, diagnostic-only Demand metric redundancy review.

The implementation deliberately starts at persisted metric scores: challengers may
change membership and proportional weights, but never feature construction or
normalization chronology.
"""
from __future__ import annotations

from itertools import combinations
from pathlib import Path
import json
import numpy as np
import pandas as pd

from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points

TOL = 1e-12
REVIEW_GEOS = (
    "district_of_columbia_dc__county", "essex_county_nj__county",
    "montgomery_county_md__county", "prince_george_s_county_md__county",
    "fairfax_county_va__county", "san_francisco_county_ca__county",
    "los_angeles_county_ca__county",
)
METRICS = ("population", "median_household_income", "gdp_annual", "labor_force", "employment", "laus_unemployment_rate")
STRUCTURAL = METRICS[:3]
LABOR = METRICS[3:]
POLICIES = {
    "DEM-LABOR-A": set(METRICS),
    "DEM-LABOR-B": set(METRICS) - {"labor_force"},
    "DEM-LABOR-C": set(METRICS) - {"labor_force", "employment"},
    "DEM-LABOR-D": set(METRICS) - {"labor_force", "laus_unemployment_rate"},
}
ABLATIONS = {
    "DROP-LABOR-FORCE": {"labor_force"}, "DROP-EMPLOYMENT": {"employment"},
    "DROP-UNEMPLOYMENT-RATE": {"laus_unemployment_rate"}, "DROP-GDP": {"gdp_annual"},
    "DROP-INCOME": {"median_household_income"}, "DROP-POPULATION": {"population"},
    "DROP-LABOR-FORCE-AND-EMPLOYMENT": {"labor_force", "employment"},
    "DROP-EMPLOYMENT-AND-UNEMPLOYMENT": {"employment", "laus_unemployment_rate"},
    "LABOR-UNEMPLOYMENT-ONLY": {"labor_force", "employment"},
    "LABOR-EMPLOYMENT-ONLY": {"labor_force", "laus_unemployment_rate"},
}
OUTPUTS = (
 "demand_metric_production_contract", "demand_metric_pairwise_redundancy",
 "demand_metric_contribution_summary", "demand_metric_movement_attribution",
 "demand_metric_entry_exit_movement_audit",
 "demand_metric_cancellation_summary", "demand_axis_cancellation_summary",
 "demand_structural_vs_labor_summary", "demand_metric_ablation_summary",
 "demand_metric_incremental_information", "demand_metric_policy_registry",
 "demand_metric_policy_stability", "demand_metric_policy_turning_points",
 "demand_metric_recent_36m", "demand_metric_decision_matrix",
 "demand_metric_parity_audit", "demand_metric_governance_status",
 "demand_metric_runtime_summary",
)

MOVEMENT_AUDIT_OUTPUTS = (
    "demand_movement_residual_audit",
    "demand_movement_metric_detail",
    "demand_movement_effect_decomposition",
    "demand_movement_residual_summary",
)

def _col(df, *names):
    for n in names:
        if n in df.columns: return n
    raise ValueError(f"Required column absent (accepted {names}); found {list(df.columns)}")

def production_contract(root: Path) -> tuple[pd.DataFrame, dict[str, float]]:
    mr=pd.read_csv(root/"config/metric_dimension_registry.csv")
    fr=pd.read_csv(root/"config/feature_registry.csv")
    nr=pd.read_csv(root/"config/normalization_registry.csv")
    active=mr[(mr.dimension.eq("demand")) & mr.enabled.astype(bool) & mr.metric_weight.gt(0)].copy()
    # Source alternatives collapse to one canonical production metric.
    got=set(active.canonical_metric_key)
    if got != set(METRICS): raise ValueError(f"Demand metric contract drift: expected {set(METRICS)}, found {got}")
    weights=active.groupby("canonical_metric_key").metric_weight.first().to_dict()
    rows=[]
    for metric in METRICS:
        sources=active.loc[active.canonical_metric_key.eq(metric),"metric_key"].tolist()
        feats=fr[fr.metric_key.isin(sources)]
        for f in feats.itertuples():
            norm=nr[nr.policy_key.eq(f.feature_key)]
            rows.append({"canonical_metric_key":metric,"dimension":"demand","configured_metric_weight":weights[metric],
              "metric_polarity":norm.score_direction.iloc[0] if len(norm) else "unknown", "feature_key":f.feature_key,
              "feature_transform":f.transform,"feature_window":f.feature_window,"feature_weight":f.feature_weight,
              "normalization_method":norm.normalization_method.iloc[0] if len(norm) else "unknown",
              "source_lineage_identity":"|".join(sources)})
    out=pd.DataFrame(rows)
    expected={"level":(.25,"6m"),"short":(.35,"6m/lag3m"),"long":(.40,"6m/lag12m")}
    for source in ("laus_labor_force","laus_employment","laus_unemployment_rate"):
        for suffix,(weight,window) in expected.items():
            q=fr[fr.feature_key.eq(f"{source}_{suffix}")]
            if len(q)!=1 or abs(float(q.feature_weight.iloc[0])-weight)>TOL or q.feature_window.iloc[0]!=window:
                raise ValueError(f"Frozen LAUS feature contract differs for {source}_{suffix}")
    return out, weights

def _load(run: Path, name: str) -> pd.DataFrame:
    p=run/f"{name}.parquet"
    if not p.is_file(): raise FileNotFoundError(f"authoritative v1.0 artifact required; no substitution: {p}")
    return pd.read_parquet(p)

def _metric_long(raw: pd.DataFrame) -> pd.DataFrame:
    geo=_col(raw,"geo_id"); date=_col(raw,"evaluation_date","date"); metric=_col(raw,"canonical_metric_key","metric_key"); score=_col(raw,"metric_score","score")
    x=raw.rename(columns={geo:"geo_id",date:"date",metric:"metric",score:"score"})[["geo_id","date","metric","score"]]
    x["date"]=pd.to_datetime(x.date); x=x[x.geo_id.isin(REVIEW_GEOS)&x.metric.isin(METRICS)]
    if set(x.geo_id)!=set(REVIEW_GEOS): raise ValueError(f"governed geography coverage missing: {set(REVIEW_GEOS)-set(x.geo_id)}")
    if x.duplicated(["geo_id","date","metric"]).any(): raise ValueError("duplicate governed metric chronology")
    return x

def _score(x: pd.DataFrame, weights: dict, included: set[str]) -> pd.DataFrame:
    z=x[x.metric.isin(included)].copy(); z["base_weight"]=z.metric.map(weights)
    z["effective_weight"]=z.base_weight/z.groupby(["geo_id","date"]).base_weight.transform("sum")
    z["contribution"]=z.score*z.effective_weight
    z["demand_dimension"]=z.groupby(["geo_id","date"]).contribution.transform("sum")
    return z

def build_complete_contribution_panel(incumbent: pd.DataFrame, persisted_dimension: pd.DataFrame,
                                      weights: dict[str, float], metrics=METRICS) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Complete attribution bookkeeping without manufacturing unavailable scores."""
    keys = ["geo_id", "date"]
    chronology = persisted_dimension.rename(columns={"value":"demand_dimension", "persisted":"demand_dimension"})[keys+["demand_dimension"]].copy()
    chronology["date"] = pd.to_datetime(chronology.date)
    chronology = chronology[chronology.geo_id.isin(REVIEW_GEOS)].drop_duplicates(keys).sort_values(keys)
    if set(chronology.geo_id) != set(REVIEW_GEOS):
        raise ValueError(f"governed geography coverage missing: {set(REVIEW_GEOS)-set(chronology.geo_id)}")
    chronology["dimension_delta"] = chronology.groupby("geo_id").demand_dimension.diff()
    grid = chronology[keys].merge(pd.DataFrame({"metric":list(metrics)}), how="cross")
    available = incumbent[keys+["metric","score","effective_weight","contribution"]].copy()
    panel = grid.merge(available, on=keys+["metric"], how="left", validate="one_to_one")
    panel["metric_available"] = panel.score.notna()
    panel["configured_metric_weight"] = panel.metric.map(weights)
    panel["effective_weight"] = panel.effective_weight.fillna(0.0)
    panel["contribution"] = panel.contribution.fillna(0.0)
    panel = panel.merge(chronology, on=keys, validate="many_to_one").sort_values(["geo_id","metric","date"])
    panel["prior_contribution"] = panel.groupby(["geo_id","metric"]).contribution.shift()
    panel["contribution_delta"] = panel.contribution-panel.prior_contribution
    current = panel.groupby(keys).contribution.sum()
    level_error = (current-chronology.set_index(keys).demand_dimension).abs().max()
    movement = panel.groupby(keys).contribution_delta.sum(min_count=1)
    delta = chronology.set_index(keys).groupby(level=0).demand_dimension.diff()
    movement_error = (movement-delta).abs().max()
    if level_error > TOL or movement_error > TOL:
        raise ValueError(f"complete contribution parity failed: level={level_error}, movement={movement_error}")
    audit_rows=[]
    for (geo,date), group in panel.groupby(keys, sort=True):
        # Availability, rather than a zero-valued contribution, defines entry and exit.
        prior_date = pd.Timestamp(date)-pd.offsets.MonthEnd(1)
        prior = panel[(panel.geo_id.eq(geo)) & panel.date.eq(prior_date)].set_index("metric")
        if prior.empty: entered=[]; exited=[]; changed=False; weight_changed=False
        else:
            p_av=prior.metric_available.reindex(group.metric).fillna(False).to_numpy()
            c_av=group.metric_available.to_numpy()
            entered=group.loc[c_av & ~p_av,"metric"].tolist(); exited=group.loc[~c_av & p_av,"metric"].tolist()
            changed=bool(entered or exited)
            p_w=prior.effective_weight.reindex(group.metric).fillna(0).to_numpy()
            weight_changed=bool(np.max(np.abs(group.effective_weight.to_numpy()-p_w)) > TOL)
        audit_rows.append({"geo_id":geo,"date":date,"metrics_entered":"|".join(sorted(entered)),
          "metrics_exited":"|".join(sorted(exited)),"metric_set_changed":changed,
          "effective_weight_changed":weight_changed,"demand_dimension_delta":group.dimension_delta.iloc[0],
          "reconstructed_contribution_delta":group.contribution_delta.sum(min_count=1),
          "movement_residual":group.contribution_delta.sum(min_count=1)-group.dimension_delta.iloc[0]})
    return panel, pd.DataFrame(audit_rows)

def build_movement_audit(incumbent: pd.DataFrame, persisted_dimension: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Explain Demand movement reconstruction without altering scoring policy."""
    keys = ["geo_id", "date"]
    detail = incumbent.copy()
    detail["date"] = pd.to_datetime(detail["date"])
    if detail.duplicated(keys + ["metric"]).any():
        raise ValueError("duplicate governed metric chronology")
    chronology = (persisted_dimension.rename(columns={"value": "demand_dimension", "persisted": "demand_dimension"})
                  [keys + ["demand_dimension"]].copy())
    if chronology.duplicated(keys).any():
        raise ValueError("duplicate governed Demand dimension chronology")
    chronology = chronology.sort_values(keys)
    detail = detail.sort_values(["geo_id", "metric", "date"])
    detail["diagnostic_contribution_delta"] = detail.groupby(["geo_id", "metric"]).contribution.diff()
    reconstructed = (detail.groupby(keys, as_index=False).diagnostic_contribution_delta.sum(min_count=1)
                     .rename(columns={"diagnostic_contribution_delta":"reconstructed_contribution_delta"}))
    totals = detail.groupby(keys, as_index=False).agg(
        sum_metric_contribution=("contribution", "sum"),
        available_metric_count=("metric", "nunique"),
        effective_weight_sum=("effective_weight", "sum"),
    )
    sets = detail.groupby(keys).metric.agg(lambda s: "|".join(sorted(set(s)))).rename("available_metric_set").reset_index()
    chronology = (chronology.merge(totals, on=keys, how="left", validate="one_to_one")
                  .merge(sets, on=keys, how="left", validate="one_to_one")
                  .merge(reconstructed, on=keys, how="left", validate="one_to_one"))
    chronology = chronology.sort_values(keys)
    for col, prior in (("demand_dimension", "prior_demand_dimension"),
                       ("sum_metric_contribution", "prior_sum_metric_contribution"),
                       ("available_metric_count", "prior_available_metric_count"),
                       ("available_metric_set", "prior_available_metric_set"),
                       ("effective_weight_sum", "prior_effective_weight_sum")):
        chronology[prior] = chronology.groupby("geo_id")[col].shift()
    chronology["persisted_dimension_delta"] = chronology["demand_dimension"] - chronology["prior_demand_dimension"]
    chronology["movement_residual"] = chronology["persisted_dimension_delta"] - chronology["reconstructed_contribution_delta"]
    chronology["abs_movement_residual"] = chronology.movement_residual.abs()
    chronology["available_metric_count_changed"] = chronology.available_metric_count.ne(chronology.prior_available_metric_count) & chronology.prior_available_metric_count.notna()
    chronology["metric_set_changed"] = chronology.available_metric_set.ne(chronology.prior_available_metric_set) & chronology.prior_available_metric_set.notna()
    chronology["max_abs_effective_weight_change"] = 0.0
    chronology["any_effective_weight_change"] = False
    chronology["metrics_entered"] = ""
    chronology["metrics_exited"] = ""
    chronology["any_nonconsecutive_metric_observation"] = False

    current = detail.set_index(keys + ["metric"])[["score", "effective_weight", "contribution"]]
    records = []
    decompositions = []
    for row in chronology.itertuples():
        if pd.isna(row.prior_demand_dimension):
            continue
        current_metrics = set(str(row.available_metric_set).split("|")) if row.available_metric_set else set()
        prior_metrics = set(str(row.prior_available_metric_set).split("|")) if row.prior_available_metric_set else set()
        weight_changes = []
        nonconsecutive = False
        for metric in sorted(current_metrics | prior_metrics):
            cur_key = (row.geo_id, pd.Timestamp(row.date), metric)
            previous_rows = detail[(detail.geo_id.eq(row.geo_id)) & detail.metric.eq(metric) & (detail.date < row.date)].sort_values("date")
            previous = previous_rows.iloc[-1] if len(previous_rows) else None
            cur = current.loc[cur_key] if cur_key in current.index else None
            months = ((pd.Period(row.date, freq="M") - pd.Period(previous.date, freq="M")).n
                      if previous is not None and cur is not None else np.nan)
            consecutive = bool(pd.notna(months) and months == 1)
            if cur is not None and previous is not None and not consecutive:
                nonconsecutive = True
            wt = float(cur.effective_weight) if cur is not None else np.nan
            wp = float(previous.effective_weight) if previous is not None and consecutive else np.nan
            if pd.notna(wt) and pd.notna(wp): weight_changes.append(abs(wt-wp))
            if row.abs_movement_residual > TOL:
                contribution_delta = float(cur.contribution-previous.contribution) if cur is not None and previous is not None and consecutive else np.nan
                base = {"geo_id":row.geo_id,"date":row.date,"metric":metric,
                        "score_t":float(cur.score) if cur is not None else np.nan,
                        "score_t_minus_1":float(previous.score) if previous is not None and consecutive else np.nan,
                        "effective_weight_t":wt,"effective_weight_t_minus_1":wp,
                        "contribution_t":float(cur.contribution) if cur is not None else np.nan,
                        "contribution_t_minus_1":float(previous.contribution) if previous is not None and consecutive else np.nan,
                        "contribution_delta":contribution_delta,"metric_present_t":cur is not None,
                        "metric_present_t_minus_1":previous is not None and consecutive,
                        "months_between_observations":months}
                records.append(base)
                dec = dict(base)
                if consecutive:
                    score_effect = wp*(float(cur.score)-float(previous.score))
                    weight_effect = float(previous.score)*(wt-wp)
                    interaction_effect = (float(cur.score)-float(previous.score))*(wt-wp)
                    error = abs(contribution_delta-score_effect-weight_effect-interaction_effect)
                    if error > TOL: raise ValueError(f"movement effect decomposition failed: {error}")
                    dec.update(score_effect=score_effect, weight_effect=weight_effect,
                               interaction_effect=interaction_effect, decomposition_status="reconciled",
                               decomposition_error=error)
                else:
                    dec.update(score_effect=np.nan, weight_effect=np.nan, interaction_effect=np.nan,
                               decomposition_status=("no_prior_available_observation" if previous is None else "nonconsecutive"), decomposition_error=np.nan)
                decompositions.append(dec)
        max_change = max(weight_changes, default=0.)
        idx = chronology.index[(chronology.geo_id.eq(row.geo_id)) & chronology.date.eq(row.date)][0]
        chronology.loc[idx,"max_abs_effective_weight_change"] = max_change
        chronology.loc[idx,"any_effective_weight_change"] = max_change > TOL
        chronology.loc[idx,"metrics_entered"] = "|".join(sorted(current_metrics-prior_metrics))
        chronology.loc[idx,"metrics_exited"] = "|".join(sorted(prior_metrics-current_metrics))
        chronology.loc[idx,"any_nonconsecutive_metric_observation"] = nonconsecutive
    chronology = chronology[chronology.prior_demand_dimension.notna()].copy()
    chronology = chronology.sort_values(["abs_movement_residual","geo_id","date"], ascending=[False,True,True])
    summaries=[]
    for geo, group in [("OVERALL",chronology), *chronology.groupby("geo_id")]:
        f=group[group.abs_movement_residual > TOL]; denom=len(f)
        none = ~(f.metric_set_changed | f.any_effective_weight_change | f.any_nonconsecutive_metric_observation)
        summaries.append({"geo_id":geo,"max_abs_residual":group.abs_movement_residual.max(),
          "median_abs_residual":group.abs_movement_residual.median(),"p90_abs_residual":_q(group.abs_movement_residual,.90),
          "p99_abs_residual":_q(group.abs_movement_residual,.99),"rows_above_1e12_tolerance":denom,
          "share_residual_rows_with_metric_set_change":f.metric_set_changed.mean() if denom else 0.,
          "share_residual_rows_with_weight_change":f.any_effective_weight_change.mean() if denom else 0.,
          "share_residual_rows_with_nonconsecutive_metric_history":f.any_nonconsecutive_metric_observation.mean() if denom else 0.,
          "share_residual_rows_with_none_of_the_above":none.mean() if denom else 0.})
    return {MOVEMENT_AUDIT_OUTPUTS[0]:chronology, MOVEMENT_AUDIT_OUTPUTS[1]:pd.DataFrame(records),
            MOVEMENT_AUDIT_OUTPUTS[2]:pd.DataFrame(decompositions), MOVEMENT_AUDIT_OUTPUTS[3]:pd.DataFrame(summaries)}

def _series(raw, dimension=None, axis=None):
    geo=_col(raw,"geo_id"); date=_col(raw,"evaluation_date","date")
    q=raw.copy()
    if dimension is not None:
        dc=_col(q,"dimension"); q=q[q[dc].eq(dimension)]
    if axis is not None:
        ac=_col(q,"axis"); q=q[q[ac].eq(axis)]
    val=_col(q,"dimension_score","axis_score","score")
    return q.rename(columns={geo:"geo_id",date:"date",val:"value"})[["geo_id","date","value"]].assign(date=lambda d:pd.to_datetime(d.date))

def _axis(dimensions, demand: pd.DataFrame, axis_weights: dict) -> tuple[pd.DataFrame,pd.DataFrame]:
    d=dimensions.copy(); dc=_col(d,"dimension"); geo=_col(d,"geo_id"); date=_col(d,"evaluation_date","date"); val=_col(d,"dimension_score","score")
    d=d.rename(columns={dc:"dimension",geo:"geo_id",date:"date",val:"score"}); d.date=pd.to_datetime(d.date)
    d=d[d.geo_id.isin(REVIEW_GEOS)&d.dimension.isin(axis_weights)]
    replacement=demand.rename(columns={"demand_dimension":"score"})[["geo_id","date","score"]].drop_duplicates().assign(dimension="demand")
    d=pd.concat([d[d.dimension.ne("demand")],replacement],ignore_index=True); d["weight"]=d.dimension.map(axis_weights)
    d["effective_weight"]=d.weight/d.groupby(["geo_id","date"]).weight.transform("sum"); d["axis_contribution"]=d.score*d.effective_weight
    a=d.groupby(["geo_id","date"],as_index=False).axis_contribution.sum().rename(columns={"axis_contribution":"demand_axis"})
    return a,d

def _q(s,q): return float(s.dropna().quantile(q)) if s.notna().any() else np.nan
def _turns(frame,value,policy):
    rows=[]
    for geo,g in frame.groupby("geo_id"):
        t=detect_turning_points(g[["date",value]].sort_values("date"),value)
        if len(t): t=t.assign(geo_id=geo,policy=policy); rows.append(t)
    return pd.concat(rows,ignore_index=True) if rows else pd.DataFrame(columns=["turning_point_date","turning_point_type","qualified","geo_id","policy"])

def build(run: Path, root: Path, debug_output: Path | None = None) -> dict[str,pd.DataFrame]:
    contract,weights=production_contract(root)
    # Presence checks are intentional even where diagnostic computation starts downstream.
    for name in ("source_metrics","features","normalized_features","regime_assignments"): _load(run,name)
    x=_metric_long(_load(run,"aligned_metric_scores")); dimensions=_load(run,"dimension_scores"); persisted_axis=_load(run,"axis_scores")
    incumbent=_score(x,weights,set(METRICS)); inc_series=incumbent[["geo_id","date","demand_dimension"]].drop_duplicates()
    persisted_dim=_series(dimensions,dimension="demand").rename(columns={"value":"persisted"})
    persisted_dim=persisted_dim[persisted_dim.geo_id.isin(REVIEW_GEOS)].copy()
    if set(persisted_dim.geo_id) != set(REVIEW_GEOS): raise ValueError("governed Demand dimension geography coverage missing")
    p=inc_series.merge(persisted_dim,on=["geo_id","date"],how="left"); dim_err=(p.demand_dimension-p.persisted).abs().max()
    ar=pd.read_csv(root/"config/axis_registry.csv"); ar=ar[(ar.axis.eq("demand"))&ar.enabled.astype(bool)]; axis_weights=dict(zip(ar.dimension,ar.dimension_weight))
    inc_axis,inc_axis_detail=_axis(dimensions,inc_series,axis_weights)
    inc_axis_detail=inc_axis_detail.merge(inc_axis,on=["geo_id","date"],how="left")
    pa=_series(persisted_axis,axis="demand").rename(columns={"value":"persisted"}); ap=inc_axis.merge(pa,on=["geo_id","date"],how="left"); axis_err=(ap.demand_axis-ap.persisted).abs().max()
    if pd.isna(dim_err) or dim_err>TOL or pd.isna(axis_err) or axis_err>TOL: raise ValueError(f"incumbent parity failed: dimension={dim_err}, axis={axis_err}")
    complete_panel, entry_exit_audit = build_complete_contribution_panel(incumbent, persisted_dim, weights)
    movement_err=entry_exit_audit.movement_residual.abs().max()
    parity=pd.DataFrame([{"check":"complete monthly Demand movement including metric boundaries","max_abs_error":movement_err,"status":"pass"},{"check":"normalized metric scores reused exactly","max_abs_error":0.,"status":"pass"},{"check":"effective metric weights and contributions reconstruct Demand dimension","max_abs_error":dim_err,"status":"pass"},{"check":"weighted Demand contribution and final Demand axis","max_abs_error":axis_err,"status":"pass"}])
    movement_audit = build_movement_audit(incumbent, persisted_dim)
    if debug_output is not None:
        debug_output.mkdir(parents=True, exist_ok=True)
        for name, frame in movement_audit.items(): frame.to_csv(debug_output/f"{name}.csv", index=False)
    # Pairwise aligned score diagnostics.
    pair=[]
    wide=x.pivot(index=["geo_id","date"],columns="metric",values="score")
    for a,b in combinations(METRICS,2):
      for geo in (*REVIEW_GEOS,"POOLED"):
        q=wide[[a,b]].dropna() if geo=="POOLED" else wide.loc[geo,[a,b]].dropna()
        roll=q[a].rolling(36,min_periods=24).corr(q[b]); da=q[a].diff(); db=q[b].diff()
        pair.append({"geo_id":geo,"metric_a":a,"metric_b":b,"observations":len(q),"score_correlation":q[a].corr(q[b]),"first_difference_correlation":da.corr(db),"same_month_sign_agreement":(np.sign(q[a])==np.sign(q[b])).mean(),"rolling_36m_correlation_median":roll.median(),"rolling_36m_correlation_p10":_q(roll,.1),"rolling_36m_correlation_p90":_q(roll,.9),"polarity_aligned":True})
    pair=pd.DataFrame(pair)
    # Contributions and movement.
    incumbent=incumbent.sort_values(["geo_id","metric","date"]); movement_panel=complete_panel
    abs_total=incumbent.contribution.abs().sum(); move_total=movement_panel.contribution_delta.abs().sum()
    contrib=[]; movement=[]
    for metric,g_level in incumbent.groupby("metric"):
      g=movement_panel[movement_panel.metric.eq(metric)]
      cutoff=g.date.max()-pd.DateOffset(months=35); recent=g[g.date>=cutoff]
      drivers=incumbent.assign(mx=incumbent.groupby(["geo_id","date"]).contribution.transform(lambda s:s.abs().max()), pos=incumbent.groupby(["geo_id","date"]).contribution.transform("max"), neg=incumbent.groupby(["geo_id","date"]).contribution.transform("min"))
      contrib.append({"canonical_metric_key":metric,"configured_metric_weight":weights[metric],"mean_absolute_effective_contribution":g.contribution.abs().mean(),"median_absolute_effective_contribution":g.contribution.abs().median(),"p90_absolute_contribution":_q(g.contribution.abs(),.9),"share_total_absolute_contribution":g_level.contribution.abs().sum()/abs_total,"largest_absolute_driver_share":(drivers.query('metric==@metric').contribution.abs()==drivers.query('metric==@metric').mx).mean(),"largest_positive_driver_share":(drivers.query('metric==@metric').contribution==drivers.query('metric==@metric').pos).mean(),"largest_negative_driver_share":(drivers.query('metric==@metric').contribution==drivers.query('metric==@metric').neg).mean(),"latest_36m_mean_absolute_contribution":recent.contribution.abs().mean(),"latest_36m_share_absolute_contribution":recent.contribution.abs().sum()/movement_panel[movement_panel.date>=cutoff].contribution.abs().sum()})
      movement.append({"canonical_metric_key":metric,"correlation_with_demand_dimension_delta":g.contribution_delta.corr(g.dimension_delta),"same_sign_agreement":(np.sign(g.contribution_delta)==np.sign(g.dimension_delta)).mean(),"mean_absolute_contribution_delta":g.contribution_delta.abs().mean(),"absolute_movement_share_descriptive":g.contribution_delta.abs().sum()/move_total,"dominant_monthly_movement_driver_share":(g.contribution_delta.abs()==movement_panel.groupby(["geo_id","date"]).contribution_delta.transform(lambda s:s.abs().max()).loc[g.index]).mean(),"latest_36m_mean_absolute_delta":recent.contribution_delta.abs().mean(),"latest_36m_correlation":recent.contribution_delta.corr(recent.dimension_delta)})
    # Exact movement reconstruction.
    recon=movement_panel.groupby(["geo_id","date"]).contribution_delta.sum(min_count=1)-movement_panel.groupby(["geo_id","date"]).dimension_delta.first()
    if recon.abs().max()>TOL: raise ValueError("Demand movement reconstruction failed")
    def cancellation(detail, contribution, net):
      q=detail.groupby(["geo_id","date"]).agg(gross=(contribution,lambda s:s.abs().sum()),net=(net,"first")).reset_index(); q["ratio"]=np.where(q.gross.gt(0),1-q.net.abs()/q.gross,np.nan); return q
    dc=cancellation(incumbent,"contribution","demand_dimension"); ac=cancellation(inc_axis_detail,"axis_contribution","demand_axis")
    def cancel_summary(q):
      rows=[]
      for geo,g in [("POOLED",q),*q.groupby("geo_id")]: rows.append({"geo_id":geo,"median_cancellation_ratio":g.ratio.median(),"p90_cancellation_ratio":_q(g.ratio,.9),"p99_cancellation_ratio":_q(g.ratio,.99),"latest_36m_median_cancellation_ratio":g[g.date>=g.date.max()-pd.DateOffset(months=35)].ratio.median()})
      return pd.DataFrame(rows)
    # Group balance.
    gg=movement_panel.assign(group=np.where(movement_panel.metric.isin(STRUCTURAL),"STRUCTURAL","LABOR_CYCLICAL")).groupby(["geo_id","date","group"]).agg(net_contribution=("contribution","sum"),gross_absolute_contribution=("contribution",lambda s:s.abs().sum()),movement_contribution=("contribution_delta","sum")).reset_index()
    denom=gg.groupby(["geo_id","date"]).gross_absolute_contribution.transform("sum"); gg["absolute_contribution_share"]=gg.gross_absolute_contribution/denom; gg["within_group_cancellation_ratio"]=np.where(gg.gross_absolute_contribution.gt(0),1-gg.net_contribution.abs()/gg.gross_absolute_contribution,np.nan)
    group_summary=gg.groupby(["geo_id","group"],as_index=False).agg(net_contribution_mean=("net_contribution","mean"),gross_absolute_contribution_mean=("gross_absolute_contribution","mean"),absolute_contribution_share=("absolute_contribution_share","mean"),mean_absolute_movement=("movement_contribution",lambda s:s.abs().mean()),median_within_group_cancellation=("within_group_cancellation_ratio","median"))
    # All challengers and policy family.
    chron={"INCUMBENT":(inc_series,inc_axis,incumbent)}
    for name,drops in ABLATIONS.items():
      s=_score(x,weights,set(METRICS)-drops); ds=s[["geo_id","date","demand_dimension"]].drop_duplicates(); ax,_=_axis(dimensions,ds,axis_weights); chron[name]=(ds,ax,s)
    policy_map={"DEM-LABOR-A":"INCUMBENT","DEM-LABOR-B":"DROP-LABOR-FORCE","DEM-LABOR-C":"DROP-LABOR-FORCE-AND-EMPLOYMENT","DEM-LABOR-D":"DROP-EMPLOYMENT-AND-UNEMPLOYMENT"}
    ab=[]; incremental=[]; stability=[]; turns=[]
    base_turn=_turns(inc_axis,"demand_axis","INCUMBENT")
    for name,(ds,ax,detail) in chron.items():
      dm=inc_series.merge(ds,on=["geo_id","date"],suffixes=("_inc","_chal")); am=inc_axis.merge(ax,on=["geo_id","date"],suffixes=("_inc","_chal")); diff=(dm.demand_dimension_chal-dm.demand_dimension_inc).abs(); adiff=(am.demand_axis_chal-am.demand_axis_inc).abs()
      if name!="INCUMBENT": ab.append({"challenger":name,"median_abs_dimension_difference":diff.median(),"p90_dimension_difference":_q(diff,.9),"p99_dimension_difference":_q(diff,.99),"dimension_correlation":dm.demand_dimension_inc.corr(dm.demand_dimension_chal),"dimension_sign_disagreement":(np.sign(dm.demand_dimension_inc)!=np.sign(dm.demand_dimension_chal)).mean(),"monthly_changed_direction_share":(np.sign(dm.groupby('geo_id').demand_dimension_inc.diff())!=np.sign(dm.groupby('geo_id').demand_dimension_chal.diff())).mean(),"median_abs_axis_difference":adiff.median(),"p90_axis_difference":_q(adiff,.9),"p99_axis_difference":_q(adiff,.99),"axis_correlation":am.demand_axis_inc.corr(am.demand_axis_chal),"axis_sign_disagreement":(np.sign(am.demand_axis_inc)!=np.sign(am.demand_axis_chal)).mean()})
      delta=ds.sort_values(["geo_id","date"]).groupby("geo_id").demand_dimension.diff(); ad=ax.sort_values(["geo_id","date"]).groupby("geo_id").demand_axis.diff(); ts=_turns(ax,"demand_axis",name); turns.append(ts)
      stability.append({"policy":name,"median_abs_demand_dimension_movement":delta.abs().median(),"p90_dimension_movement":_q(delta.abs(),.9),"p99_dimension_movement":_q(delta.abs(),.99),"max_dimension_jump":delta.abs().max(),"sign_flips":int((np.sign(ds.demand_dimension)!=np.sign(ds.groupby('geo_id').demand_dimension.shift())).sum()),"rolling_12m_volatility":ds.groupby('geo_id').demand_dimension.rolling(12,min_periods=2).std().median(),"qualified_turning_points":len(ts),"latest_36m_turning_points":int((pd.to_datetime(ts.turning_point_date)>=ax.date.max()-pd.DateOffset(months=35)).sum()) if len(ts) else 0,"median_abs_demand_axis_movement":ad.abs().median()})
      if name.startswith("DROP-") and name.count("AND")==0:
        row=ab[-1].copy(); row.update({"metric_removed":name.removeprefix("DROP-"),"unique_movement_disappearing":(dm.groupby('geo_id').demand_dimension_inc.diff()-dm.groupby('geo_id').demand_dimension_chal.diff()).abs().median(),"responsiveness_change":delta.abs().median()-dm.groupby('geo_id').demand_dimension_inc.diff().abs().median(),"interpretation":"diagnostic; no composite score"}); incremental.append(row)
    policy_registry=[]; decision=[]
    stab=pd.DataFrame(stability)
    for policy,key in policy_map.items():
      included=set(chron[key][2].metric.unique()); ew={m:weights[m]/sum(weights[k] for k in included) for m in included}; s=stab[stab.policy.eq(key)].iloc[0]
      policy_registry.append({"policy":policy,"included_labor_metrics":"|".join(sorted(included&set(LABOR))),"effective_metric_weights":json.dumps(ew,sort_keys=True),"diagnostic_only":True})
      pair_lf=pair.query("geo_id=='POOLED' and metric_a=='labor_force' and metric_b=='employment'").score_correlation.iloc[0]; pair_ue=pair.query("geo_id=='POOLED' and metric_a=='employment' and metric_b=='laus_unemployment_rate'").score_correlation.iloc[0]
      decision.append({"policy":policy,"included_labor_metrics":"|".join(sorted(included&set(LABOR))),"effective_metric_weights":json.dumps(ew,sort_keys=True),"median_Demand_dimension_movement":s.median_abs_demand_dimension_movement,"P90_movement":s.p90_dimension_movement,"rolling_volatility":s.rolling_12m_volatility,"turning_points":s.qualified_turning_points,"latest_36m_turns":s.latest_36m_turning_points,"median_Demand_axis_movement":s.median_abs_demand_axis_movement,"Demand_axis_sign_disagreements_vs_incumbent":0 if policy=="DEM-LABOR-A" else next(r["axis_sign_disagreement"] for r in ab if r["challenger"]==key),"median_dimension_cancellation":cancel_summary(dc).query("geo_id=='POOLED'").median_cancellation_ratio.iloc[0],"median_axis_cancellation":cancel_summary(ac).query("geo_id=='POOLED'").median_cancellation_ratio.iloc[0],"structural_contribution_share":group_summary.query("group=='STRUCTURAL'").absolute_contribution_share.mean(),"labor_contribution_share":group_summary.query("group=='LABOR_CYCLICAL'").absolute_contribution_share.mean(),"employment_labor_force_redundancy_evidence":pair_lf,"employment_unemployment_redundancy_evidence":pair_ue,"Decision":"pending"})
    recent=inc_series.merge(inc_axis,on=["geo_id","date"]).merge(dc[["geo_id","date","ratio"]],on=["geo_id","date"]).merge(gg.pivot(index=["geo_id","date"],columns="group",values="net_contribution").reset_index(),on=["geo_id","date"])
    for policy,key in policy_map.items():
      recent=recent.merge(chron[key][0].rename(columns={"demand_dimension":f"{policy}_demand_dimension"}),on=["geo_id","date"]).merge(chron[key][1].rename(columns={"demand_axis":f"{policy}_demand_axis"}),on=["geo_id","date"])
    recent=recent[recent.date>=recent.groupby("geo_id").date.transform("max")-pd.DateOffset(months=35)]
    governance=pd.DataFrame([{"recommendation_state":"none","promotion_state":"none","human_decision":"pending","automated_winner":False}])
    runtime=pd.DataFrame([{"authoritative_run":run.name,"geography_count":len(REVIEW_GEOS),"metric_count":len(METRICS),"parity_tolerance":TOL,"production_policy_changed":False}])
    tables=dict(zip(OUTPUTS,[contract,pair,pd.DataFrame(contrib),pd.DataFrame(movement),entry_exit_audit,cancel_summary(dc),cancel_summary(ac),group_summary,pd.DataFrame(ab),pd.DataFrame(incremental),pd.DataFrame(policy_registry),stab,pd.concat(turns,ignore_index=True),recent,pd.DataFrame(decision),parity,governance,runtime]))
    for name, frame in tables.items():
        if "geo_id" in frame:
            leaked=set(frame.geo_id.dropna())-set(REVIEW_GEOS)-{"POOLED","OVERALL"}
            if leaked: raise ValueError(f"non-governed geography leaked into {name}: {sorted(leaked)}")
    if set(entry_exit_audit.geo_id) != set(REVIEW_GEOS):
        raise ValueError("entry/exit audit does not contain exactly the governed review counties")
    return tables

def write_review(tables: dict[str,pd.DataFrame], output: Path) -> None:
    output.mkdir(parents=True,exist_ok=True)
    for name,frame in tables.items(): frame.to_csv(output/f"{name}.csv",index=False)
    sections=[]
    for name in OUTPUTS: sections.append(f"<h2>{name}</h2>"+tables[name].to_html(index=False,border=0))
    (output/"demand_metric_redundancy_review.html").write_text("<html><body><h1>Demand Metric Redundancy Review</h1><p>Diagnostic only. Decision pending; no automated winner.</p>"+"".join(sections)+"</body></html>")
