"""Supply Phase-1 adapter and family-level descriptive evidence.

This module deliberately delegates feature and dimension reconstruction to the
canonical Price anatomy implementation.  It adds only Supply-family comparisons;
it never constructs a challenger or scores a prospective metric-weight policy.
"""
from __future__ import annotations

from itertools import combinations
from pathlib import Path
import numpy as np
import pandas as pd

from regime.diagnostics import price_feature_anatomy as canonical

DIMENSION = "supply"
EXPECTED_METRICS = ("active_inventory", "permit_activity", "permit_intensity")
EXPECTED_WEIGHTS = {"active_inventory": .60, "permit_activity": .20, "permit_intensity": .20}
REVIEW_GEOS = canonical.REVIEW_GEOS
DC = canonical.DC
OUTPUTS = (
    "production_contract", "raw_chronology", "feature_anatomy", "normalized_features",
    "feature_contributions", "feature_statistics", "metric_statistics",
    "supply_dimension_statistics", "monthly_coverage", "seasonality_noise",
    "raw_feature_relationship", "cross_metric_relationship", "permit_family_overlap",
    "dimension_contribution_structure", "evaluation_matrix", "governance_status",
)


def resolve_contract(root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    contract, registry = canonical.resolve_contract(root, DIMENSION)
    membership = tuple(sorted(contract.metric.unique()))
    weights = contract.groupby("metric").metric_weight.first().to_dict()
    if membership != tuple(sorted(EXPECTED_METRICS)):
        raise ValueError(f"unexpected governed Supply membership: {membership}")
    if any(not np.isclose(weights.get(k, np.nan), v) for k, v in EXPECTED_WEIGHTS.items()):
        raise ValueError(f"frozen Supply weights differ from 60/20/20: {weights}")
    return contract, registry


def load_run(run: Path) -> dict[str, pd.DataFrame]:
    return canonical.load_run(run)


def _periods(frame: pd.DataFrame):
    return canonical._periods(frame)


def _relationship_rows(metric: pd.DataFrame, contributions: pd.DataFrame, raw: pd.DataFrame) -> list[dict]:
    wide = metric.pivot(index=["geo_id", "date"], columns="metric", values="production_metric_score").reset_index()
    contrib = contributions.groupby(["geo_id", "date", "metric"], as_index=False).weighted_feature_contribution.sum(min_count=1)
    cw = contrib.pivot(index=["geo_id", "date"], columns="metric", values="weighted_feature_contribution").reset_index()
    rw = raw.pivot(index=["geo_id", "date"], columns="metric", values="raw_value").reset_index()
    rows = []
    for left, right in combinations(EXPECTED_METRICS, 2):
        for geo, group in wide.groupby("geo_id"):
            for period, q in _periods(group.sort_values("date")):
                z = q[["date", left, right]].dropna().sort_values("date")
                dc = cw[cw.geo_id.eq(geo)][["date", left, right]].dropna()
                raw_pair = rw[rw.geo_id.eq(geo)].merge(z[["date"]],on="date")[[left,right]].dropna()
                dl, dr = z[left].diff(), z[right].diff()
                lag_corr = {lag: z[left].corr(z[right].shift(lag)) for lag in range(-6, 7)}
                best = max(lag_corr, key=lambda x: abs(lag_corr[x]) if pd.notna(lag_corr[x]) else -1)
                rows.append({"left_metric": left, "right_metric": right, "geo_id": geo, "period": period,
                    "observation_count": len(z), "raw_chronology_correlation": raw_pair[left].corr(raw_pair[right]),
                    "normalized_metric_correlation": z[left].corr(z[right]),
                    "sign_agreement": (np.sign(z[left]) == np.sign(z[right])).mean(),
                    "direction_agreement": (np.sign(dl) == np.sign(dr)).iloc[1:].mean(),
                    "contribution_correlation": dc[left].corr(dc[right]),
                    "best_descriptive_lag_months": best, "best_absolute_lag_correlation": lag_corr[best]})
    return rows


def _permit_overlap(metric: pd.DataFrame, contributions: pd.DataFrame) -> pd.DataFrame:
    wide = metric.pivot(index=["geo_id", "date"], columns="metric", values="production_metric_score").reset_index()
    cw = contributions.groupby(["geo_id", "date", "metric"], as_index=False).weighted_feature_contribution.sum(min_count=1).pivot(index=["geo_id", "date"], columns="metric", values="weighted_feature_contribution").reset_index()
    rows=[]
    for geo,g in wide.groupby("geo_id"):
      for period,q in _periods(g.sort_values("date")):
        z=q.dropna(subset=["permit_activity","permit_intensity"]); a=z.permit_activity.diff(); i=z.permit_intensity.diff(); agree=np.sign(a)==np.sign(i)
        c=cw[cw.geo_id.eq(geo)].merge(z[["date"]],on="date")
        scale=pd.concat([a.abs(),i.abs()]).median(); material=max(float(scale) if pd.notna(scale) else 0, .05)
        rows.append({"geo_id":geo,"period":period,"chronology_correlation":z.permit_activity.corr(z.permit_intensity),
          "contribution_correlation":c.permit_activity.corr(c.permit_intensity),"direction_agreement_months":int(agree.iloc[1:].sum()),
          "direction_disagreement_months":int((~agree.iloc[1:]).sum()),"permit_activity_unique_material_moves":int(((a.abs()>=material)&(i.abs()<material/2)).sum()),
          "permit_intensity_unique_material_moves":int(((i.abs()>=material)&(a.abs()<material/2)).sum()),
          "unique_turning_months":int(((np.sign(a)*np.sign(a.shift())<0) ^ (np.sign(i)*np.sign(i.shift())<0)).sum())})
    return pd.DataFrame(rows)


def _structure(metric: pd.DataFrame) -> pd.DataFrame:
    wide=metric.pivot(index=["geo_id","date"],columns="metric",values="production_metric_score").reset_index()
    rows=[]
    for geo,g in wide.groupby("geo_id"):
      for period,q in _periods(g.sort_values("date")):
        z=q.dropna(subset=list(EXPECTED_METRICS)); weighted=z[list(EXPECTED_METRICS)].mul(pd.Series(EXPECTED_WEIGHTS)); permits=weighted.permit_activity+weighted.permit_intensity; inv=weighted.active_inventory
        gross=weighted.abs().sum(axis=1); net=(inv+permits).abs(); pa=z.permit_activity; pi=z.permit_intensity; inventory=z.active_inventory
        rows.append({"geo_id":geo,"period":period,"observation_count":len(z),"inventory_absolute_contribution_share":weighted.active_inventory.abs().sum()/gross.sum(),
          "combined_permit_absolute_contribution_share":weighted[["permit_activity","permit_intensity"]].abs().sum().sum()/gross.sum(),
          "inventory_opposes_both_permits_rate":(((np.sign(inventory)!=np.sign(pa))&(np.sign(inventory)!=np.sign(pi))).mean()),
          "permits_agree_against_inventory_rate":(((np.sign(pa)==np.sign(pi))&(np.sign(pa)!=np.sign(inventory))).mean()),
          "mean_cancellation_ratio":(1-net.div(gross.replace(0,np.nan))).mean(),"net_to_gross_contribution":net.sum()/gross.sum(),
          "inventory_metric_dominance_rate":(weighted.active_inventory.abs().ge(weighted[["permit_activity","permit_intensity"]].abs().max(axis=1))).mean(),
          "permit_family_dominance_rate":(permits.abs()>inv.abs()).mean()})
    return pd.DataFrame(rows)


def build(artifacts: dict[str, pd.DataFrame], root: Path) -> dict[str, pd.DataFrame]:
    resolve_contract(root)  # enforce the frozen contract before reconstruction
    tables=canonical.build(artifacts,root,DIMENSION)
    feature_registry=pd.read_csv(root/"config/feature_registry.csv",usecols=["feature_key","notes"])
    notes=feature_registry.set_index("feature_key").notes.reindex(tables["production_contract"].feature_key)
    provenance=notes.map(lambda value: "" if pd.isna(value) else str(value).strip())
    tables["production_contract"]["feature_policy_provenance"]=[value or "governed registry; no explicit promotion metadata" for value in provenance]
    tables["production_contract"]["prior_explicit_calibration_or_promotion"]=[any(token in value.lower() for token in ("promoted","promotion","calibrat")) for value in provenance]
    metric=tables["feature_contributions"][["geo_id","date","metric","production_metric_score"]].drop_duplicates()
    tables["cross_metric_relationship"]=pd.DataFrame(_relationship_rows(metric,tables["feature_contributions"],tables["raw_chronology"]))
    tables["permit_family_overlap"]=_permit_overlap(metric,tables["feature_contributions"])
    tables["dimension_contribution_structure"]=_structure(metric)
    turns=int(tables["metric_statistics"].turning_point_count.fillna(0).sum())
    health="pass" if turns>0 else "fail"
    bounds={"active_inventory":"Level .35-.65; Short .10-.35; Long .15-.45",
      "permit_activity":"Level .60-.90; Short .05-.25; Long .05-.25",
      "permit_intensity":"Level .35-.65; Short .10-.35; Long .15-.45"}
    tables["evaluation_matrix"]=pd.DataFrame([
      {"question":q,"metric":m,"status":"descriptive_evidence_only","evidence":"review exported county/equal-footing tables and plots; no candidate was scored",
       "bounded_future_candidate_region":bounds[m]} for q,m in enumerate(EXPECTED_METRICS,1)
    ] + [{"question":"raw_metric_anatomy_vs_incumbent_feature_policy_effects","metric":"supply_family",
      "status":"human_review_required","evidence":"separate underlying raw behavior from differences introduced by incumbent feature construction",
      "bounded_future_candidate_region":"not_applicable"}]).assign(turning_point_detector_health=health,qualified_turn_count=turns)
    tables["governance_status"]=pd.DataFrame([{"recommendation_state":"none","promotion_state":"current_production_unchanged",
      "human_decision":"supply_feature_anatomy_review_pending","automated_winner":False,"production_policy_changed":False,
      "metric_weight_policy_changed":False,"capital_markets_changed":False,"turning_point_detector_health":health}])
    tables["_metadata"]={"dimension":DIMENSION,"target_metrics":EXPECTED_METRICS,"dimension_score":"supply_dimension_score"}
    return tables


def _scope_series(frame: pd.DataFrame, col: str, scope: str) -> pd.DataFrame:
    q=frame[["geo_id","date",col]].dropna()
    if scope=="dc": return q[q.geo_id.eq(DC)].rename(columns={col:"value"})
    return canonical._pool(q,col,["geo_id"]).rename(columns={col:"value"})


def write_review(tables: dict[str, pd.DataFrame], out: Path) -> None:
    canonical.write_review(tables,out,DIMENSION)
    prefix="supply_phase1"; plots=[]
    # The canonical writer owns common anatomy exports; the adapter persists
    # its additional family tables under the same deterministic prefix.
    for name in OUTPUTS:
      tables[name].to_csv(out/f"{prefix}_{name}.csv",index=False)
    metric=tables["feature_contributions"][["geo_id","date","metric","production_metric_score"]].drop_duplicates()
    wide=metric.pivot(index=["geo_id","date"],columns="metric",values="production_metric_score").reset_index()
    wide=wide.merge(tables["_dimension"][["geo_id","date","supply_dimension_score"]],on=["geo_id","date"])
    for scope in ("dc","seven_county_standardized"):
      permit=[(m,_scope_series(wide,m,scope)) for m in ("permit_activity","permit_intensity")]
      fn=f"{prefix}_permit_family_{scope}_overlap.svg"; canonical._plot(out/fn,permit,f"Permit family overlap — {scope}",(-1,1)); plots.append(fn)
      panels=[]
      for m in EXPECTED_METRICS:
        q=wide[["geo_id","date",m]].copy(); q[m]=q[m]*EXPECTED_WEIGHTS[m]; panels.append((f"{m} contribution",_scope_series(q,m,scope)))
      panels.append(("Supply score",_scope_series(wide,"supply_dimension_score",scope)))
      fn=f"{prefix}_contribution_structure_{scope}.svg"; canonical._plot(out/fn,panels,f"Supply contribution structure — {scope}",(-1,1)); plots.append(fn)
    index=out/f"{prefix}_review_index.html"; body=index.read_text()
    note=("<p><strong>Interpretation note:</strong> The three Supply metrics enter this anatomy review under different incumbent "
      "feature policies. Therefore observed cross-metric differences may reflect both underlying metric behavior and current "
      "feature-policy construction. Phase 1 must not attribute all differences solely to the raw data-generating process.</p>")
    body=body.replace("<ul>",note+"<ul>").replace("</ul>","".join(f'<li><a href="{p}">{p}</a></li>' for p in plots)+"</ul>")
    index.write_text(body)
