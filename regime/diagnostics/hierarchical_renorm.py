"""Diagnostic-only bounded re-normalization above the feature layer.

The diagnostic consumes authoritative scoring artifacts.  It deliberately calls
the production dimension and axis scorers rather than reproducing their weighted
aggregation and availability-weight renormalization.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd

from regime._05_dimension_scorer import _build_dimension_weights, score_dimensions
from regime._06_axis_engine import _build_axis_weights, score_axes
from regime._07_coordinate_engine import build_coordinates
from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points
from regime.diagnostics.laus_finalist_stability import reversal_events, reversal_summary

PATHS = ("A", "B", "C")
DIMENSIONS = ("demand", "price", "affordability", "capital_markets")
MARKET_CONTEXT = "market_context"
LOOKBACK, MIN_PERIODS, CLIP_LOW, CLIP_HIGH = 120, 36, .01, .99
OUTPUT_NAMES = (
    "scenario_registry", "metric_statistics", "dimension_statistics", "axis_statistics",
    "chronology_comparison", "reversal_whipsaw", "turning_points",
    "contribution_influence", "cancellation", "extreme_decomposition",
    "cross_dimension_comparability", "regime_sensitivity", "period_sensitivity",
    "evaluation_matrix", "governance_status",
)


@dataclass(frozen=True)
class DiagnosticResult:
    tables: Mapping[str, pd.DataFrame]
    chronologies: Mapping[str, pd.DataFrame]


def rolling_percentile(values: pd.Series) -> pd.Series:
    """Causal 120-observation percentile, clipped then mapped to [-.98, .98]."""
    numeric = pd.to_numeric(values, errors="coerce")
    def rank_last(window: pd.Series) -> float:
        clean = window.dropna()
        if len(clean) < MIN_PERIODS or pd.isna(window.iloc[-1]):
            return np.nan
        # average rank makes ties deterministic and row-order independent
        return float(clean.rank(method="average", pct=True).iloc[-1])
    percentile = numeric.rolling(LOOKBACK, min_periods=MIN_PERIODS).apply(rank_last, raw=False)
    return (percentile.clip(CLIP_LOW, CLIP_HIGH) * 2.0 - 1.0).clip(-.98, .98)


def _renormalize(frame: pd.DataFrame, child: str, score: str) -> pd.DataFrame:
    out = frame.copy()
    out["date"] = pd.to_datetime(out["date"])
    out = out.sort_values(["geo_id", child, "date"], kind="mergesort")
    monthly = out.groupby(["geo_id", child], sort=False)["date"].apply(
        lambda x: pd.Series(x.dt.to_period("M").sort_values().astype("int64")).diff().dropna().eq(1).all())
    if len(out) and not monthly.all():
        raise ValueError("Diagnostic re-normalization requires contiguous monthly composite series")
    out[score] = out.groupby(["geo_id", child], sort=False)[score].transform(rolling_percentile)
    return out.reset_index(drop=True)


def _prepare(artifacts: Mapping[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    required = {"aligned_metric_scores", "dimension_scores", "axis_scores"}
    missing = required - set(artifacts)
    if missing:
        raise FileNotFoundError(f"Required authoritative artifacts absent: {sorted(missing)}")
    metrics = artifacts["aligned_metric_scores"].copy()
    date = "evaluation_date" if "evaluation_date" in metrics else "date"
    need = {"geo_id", date, "canonical_metric_key", "metric_score"}
    if need - set(metrics):
        raise ValueError(f"aligned_metric_scores missing columns: {sorted(need-set(metrics))}")
    metrics = metrics.rename(columns={date: "date"})
    metrics["date"] = pd.to_datetime(metrics.date)
    weights = _build_dimension_weights()
    metrics = metrics.merge(weights[["canonical_metric_key", "dimension"]], on="canonical_metric_key", how="left")
    unknown = metrics.dimension.isna()
    if unknown.any():
        raise ValueError("Authoritative metrics contain unmapped governed identities")
    metrics = metrics[metrics.dimension.isin(DIMENSIONS)].drop(columns="dimension")
    if metrics.empty:
        raise ValueError("No governed Demand-axis metrics in authoritative artifacts")
    dimensions = artifacts["dimension_scores"].copy()
    dimensions = dimensions.rename(columns={"evaluation_date": "date"})
    dimensions["date"] = pd.to_datetime(dimensions.date)
    dimensions = dimensions[dimensions.dimension.isin(DIMENSIONS)].copy()
    axes = artifacts["axis_scores"].copy().rename(columns={"evaluation_date": "date"})
    axes["date"] = pd.to_datetime(axes.date)
    axes = axes[axes.axis.eq("demand")].copy()
    if dimensions.empty or axes.empty:
        raise ValueError("Authoritative Demand dimension/axis chronologies are absent")
    return metrics.sort_values(["geo_id","canonical_metric_key","date"]), dimensions, axes


def build_paths(artifacts: Mapping[str, pd.DataFrame]) -> Mapping[str, pd.DataFrame]:
    """Build exactly A/B/C; Path A remains byte-for-value authoritative."""
    metrics_a, dims_a, axis_a = _prepare(artifacts)
    metric_base = metrics_a.rename(columns={"date": "evaluation_date"})
    metric_renorm = _renormalize(metrics_a, "canonical_metric_key", "metric_score")
    metric_b = metric_renorm.rename(columns={"date": "evaluation_date"})
    dims_b = score_dimensions(metric_b).query("dimension in @DIMENSIONS")
    dims_c = _renormalize(dims_b, "dimension", "dimension_score")
    axis_b = score_axes(dims_b).query("axis == 'demand'")
    axis_c = score_axes(dims_c).query("axis == 'demand'")
    frames = []
    for path, metric, dim, axis in (("A", metric_base, dims_a, axis_a),
                                    ("B", metric_b, dims_b, axis_b),
                                    ("C", metric_b, dims_c, axis_c)):
        for layer, frame, child, score in (
            ("metric", metric.rename(columns={"evaluation_date":"date"}), "canonical_metric_key", "metric_score"),
            ("dimension", dim, "dimension", "dimension_score"), ("axis", axis, "axis", "axis_score")):
            q = frame.copy(); q["path"] = path; q["layer"] = layer
            q["component"] = q[child]; q["score"] = q[score]
            frames.append(q[["path","layer","geo_id","date","component","score"]])
    return {"long": pd.concat(frames, ignore_index=True), "metrics_a": metrics_a,
            "metrics_renorm": metric_renorm, "dimensions_a": dims_a, "dimensions_b": dims_b,
            "dimensions_c": dims_c, "axis_a": axis_a, "axis_b": axis_b, "axis_c": axis_c}


def _periods(frame: pd.DataFrame):
    end = frame.date.max()
    return (("full_history", frame), ("2022_plus", frame[frame.date >= "2022-01-01"]),
            ("latest_36_months", frame[frame.date >= end-pd.DateOffset(months=35)]))


def _statistics(long: pd.DataFrame, layer: str) -> pd.DataFrame:
    rows=[]
    for period, scope in _periods(long[long.layer.eq(layer)]):
        for keys,g in scope.groupby(["path","component"], sort=True):
            x=g.score.dropna(); rows.append({"period":period,"path":keys[0],"component":keys[1],"count":len(x),
                "minimum":x.min(),"maximum":x.max(),"median":x.median(),"standard_deviation":x.std(),
                "p05":x.quantile(.05),"p95":x.quantile(.95),"share_le_neg_0_9":x.le(-.9).mean(),
                "share_le_neg_0_8":x.le(-.8).mean(),"share_ge_pos_0_8":x.ge(.8).mean(),"share_ge_pos_0_9":x.ge(.9).mean()})
    return pd.DataFrame(rows)


def _series_metrics(g: pd.DataFrame) -> dict:
    x=g.sort_values("date").score.dropna(); delta=x.diff(); directions=np.sign(delta).replace(0,np.nan)
    turns=detect_turning_points(g[["date","score"]], "score")
    return {"reversal_count":int((directions != directions.shift()).iloc[2:].sum()),
            "zero_crossings":int((np.sign(x) != np.sign(x.shift())).iloc[1:].sum()),
            "turning_point_count":int(turns.qualified.sum()) if len(turns) else 0,
            "persistence":directions.eq(directions.shift()).iloc[1:].mean(),
            "mean_absolute_monthly_change":delta.abs().mean()}


def _chronology(long: pd.DataFrame) -> pd.DataFrame:
    rows=[]
    for layer in ("metric","dimension","axis"):
      pivot=long[long.layer.eq(layer)].pivot_table(index=["geo_id","date","component"],columns="path",values="score").reset_index()
      for left,right in (("A","B"),("A","C"),("B","C")):
       for keys,g in pivot.groupby(["geo_id","component"]):
        q=g.dropna(subset=[left,right]).sort_values("date"); dl=q[left].diff(); dr=q[right].diff()
        rows.append({"layer":layer,"geo_id":keys[0],"component":keys[1],"comparison":f"{left}_vs_{right}",
          "observation_count":len(q),"chronology_correlation":q[left].corr(q[right]),
          "sign_agreement":np.sign(q[left]).eq(np.sign(q[right])).mean(),
          "direction_agreement":np.sign(dl).eq(np.sign(dr)).iloc[1:].mean(),
          **{f"{left}_{k}":v for k,v in _series_metrics(q.rename(columns={left:"score"})).items()},
          **{f"{right}_{k}":v for k,v in _series_metrics(q.rename(columns={right:"score"})).items()}})
    return pd.DataFrame(rows)


def _reversals(long: pd.DataFrame) -> pd.DataFrame:
    rows=[]
    q=long[long.layer.isin(["dimension","axis"])]
    for keys,g in q.groupby(["path","layer","geo_id","component"]):
        events=reversal_events(g, "score"); summary=reversal_summary(events)
        rows.append(dict(zip(("path","layer","geo_id","component"),keys), **summary,
                         persistence=_series_metrics(g)["persistence"]))
    return pd.DataFrame(rows)


def _turns(long: pd.DataFrame) -> pd.DataFrame:
    rows=[]
    for keys,g in long.groupby(["layer","geo_id","component"]):
      base=detect_turning_points(g[g.path.eq("A")],"score")
      for path in ("B","C"):
        matches=match_turning_points(base,detect_turning_points(g[g.path.eq(path)],"score"))
        matched=matches[matches.matched & matches.incumbent_date.notna()]
        rows.append({"layer":keys[0],"geo_id":keys[1],"component":keys[2],"comparison":f"A_vs_{path}",
          "reference_turns":int(base.qualified.sum()) if len(base) else 0,"matched_turns":len(matched),
          "missed_turns":int((~matches.matched & matches.incumbent_date.notna()).sum()),
          "median_latency_months":matched.signed_delay_months.abs().median(),
          "same_month_share":matched.signed_delay_months.abs().eq(0).mean(),
          "plus_1_month_share":matched.signed_delay_months.abs().eq(1).mean(),
          "plus_2_or_more_share":matched.signed_delay_months.abs().ge(2).mean()})
    return pd.DataFrame(rows)


def _contributions(paths: Mapping[str,pd.DataFrame]) -> tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame]:
    weights=_build_axis_weights().query("axis == 'demand' and dimension in @DIMENSIONS")
    all_rows=[]
    for path,key in (("A","dimensions_a"),("B","dimensions_b"),("C","dimensions_c")):
        q=paths[key].merge(weights,on="dimension"); q["available_weight_sum"]=q.groupby(["geo_id","date"])["dimension_weight"].transform("sum")
        q["effective_weight"]=q.dimension_weight/q.available_weight_sum
        q["weighted_contribution"]=q.dimension_score*q.effective_weight; q["path"]=path
        all_rows.append(q)
    detail=pd.concat(all_rows,ignore_index=True)
    gross=detail.groupby(["path","geo_id","date"]).weighted_contribution.transform(lambda x:x.abs().sum())
    detail["absolute_contribution_share"]=detail.weighted_contribution.abs()/gross.replace(0,np.nan)
    influence=detail.groupby(["path","dimension","dimension_weight"],as_index=False).agg(
        mean_absolute_contribution=("weighted_contribution",lambda x:x.abs().mean()),
        median_absolute_contribution=("weighted_contribution",lambda x:x.abs().median()),
        share_total_absolute_axis_contribution=("absolute_contribution_share","mean"),
        contribution_standard_deviation=("weighted_contribution","std"))
    cancel=detail.groupby(["path","geo_id","date"],as_index=False).agg(
        signed_net_contribution=("weighted_contribution","sum"),gross_absolute_contribution=("weighted_contribution",lambda x:x.abs().sum()),
        positive_contributor_count=("weighted_contribution",lambda x:x.gt(0).sum()),negative_contributor_count=("weighted_contribution",lambda x:x.lt(0).sum()))
    cancel["cancellation_amount"]=cancel.gross_absolute_contribution-cancel.signed_net_contribution.abs()
    cancel["cancellation_ratio"]=cancel.cancellation_amount/cancel.gross_absolute_contribution.replace(0,np.nan)
    return detail,influence,cancel


def _comparability(detail: pd.DataFrame) -> pd.DataFrame:
    q=detail[detail.path.isin(["A","C"])]
    return q.groupby(["path","dimension"],as_index=False).agg(long_run_standard_deviation=("dimension_score","std"),
        interquartile_range=("dimension_score",lambda x:x.quantile(.75)-x.quantile(.25)),
        average_absolute_score=("dimension_score",lambda x:x.abs().mean()),
        average_absolute_axis_contribution=("weighted_contribution",lambda x:x.abs().mean()),
        share_months_above_abs_0_5=("dimension_score",lambda x:x.abs().gt(.5).mean()),
        share_months_above_abs_0_8=("dimension_score",lambda x:x.abs().gt(.8).mean()))


def _regime(paths, artifacts):
    if "coordinates" not in artifacts or "regime_assignments" not in artifacts:
        return pd.DataFrame([{"status":"not_evaluated","reason":"coordinates or regime_assignments artifact absent"}])
    supply=artifacts["axis_scores"].rename(columns={"evaluation_date":"date"}).query("axis == 'supply'")
    rows=[]
    for path,key in (("A","axis_a"),("B","axis_b"),("C","axis_c")):
        axes=pd.concat([supply,paths[key]],ignore_index=True); coords=build_coordinates(axes)
        coords["path"]=path; rows.append(coords)
    merged=pd.concat(rows).pivot_table(index=["geo_id","date"],columns="path",values="y_demand").reset_index()
    for challenger in ("B","C"):
        rowsign=np.sign(merged.A).ne(np.sign(merged[challenger]))
        yieldrow={"comparison":f"A_vs_{challenger}","different_demand_sign_share":rowsign.mean(),
                  "material_magnitude_difference_share":merged.A.sub(merged[challenger]).abs().ge(.10).mean(),
                  "coordinate_changed_share":merged.A.ne(merged[challenger]).mean(),
                  "regime_assignment_changed_share":np.nan,"status":"partial_fail_closed_no_reclassification"}
        rows.append(yieldrow)
    return pd.DataFrame(rows[-2:])


def build_diagnostic(artifacts: Mapping[str,pd.DataFrame]) -> DiagnosticResult:
    paths=build_paths(artifacts); long=paths["long"]; detail,influence,cancel=_contributions(paths)
    registry=pd.DataFrame([
      {"path":"A","metric_renormalized":False,"dimension_renormalized":False,"axis_renormalized":False},
      {"path":"B","metric_renormalized":True,"dimension_renormalized":False,"axis_renormalized":False},
      {"path":"C","metric_renormalized":True,"dimension_renormalized":True,"axis_renormalized":False}])
    for c,v in (("method","rolling_percentile"),("lookback",LOOKBACK),("min_periods",MIN_PERIODS),("clip_low",CLIP_LOW),("clip_high",CLIP_HIGH),("direction","positive")):
        registry[c]=v
    cancellation_rows=[]
    for period,q in _periods(cancel):
      for keys,g in q.groupby(["path","geo_id"]):
        cancellation_rows.append({"period":period,"path":keys[0],"geo_id":keys[1],"mean_cancellation_ratio":g.cancellation_ratio.mean(),
          "median_cancellation_ratio":g.cancellation_ratio.median(),"p95_cancellation_ratio":g.cancellation_ratio.quantile(.95),
          "mean_signed_net_contribution":g.signed_net_contribution.mean(),"mean_gross_absolute_contribution":g.gross_absolute_contribution.mean(),
          "mean_cancellation_amount":g.cancellation_amount.mean(),"mean_positive_contributor_count":g.positive_contributor_count.mean(),"mean_negative_contributor_count":g.negative_contributor_count.mean()})
    tables={"scenario_registry":registry,"metric_statistics":_statistics(long,"metric"),
      "dimension_statistics":_statistics(long,"dimension"),"axis_statistics":_statistics(long,"axis"),
      "chronology_comparison":_chronology(long),"reversal_whipsaw":_reversals(long),"turning_points":_turns(long),
      "contribution_influence":influence,"cancellation":pd.DataFrame(cancellation_rows),
      "extreme_decomposition":_extremes(detail,paths),"cross_dimension_comparability":_comparability(detail),
      "regime_sensitivity":_regime(paths,artifacts),"period_sensitivity":_period_summary(long),
      "evaluation_matrix":pd.DataFrame([{"empirical_conclusion":"pending_human_review","amplitude_rescaling_only":pd.NA,
        "production_reopen_supported":False,"reason":"diagnostic does not automate production decisions"}]),
      "governance_status":pd.DataFrame([{"recommendation_state":"none","promotion_state":"current_production_unchanged",
        "human_decision":"hierarchical_renorm_review_pending","automated_winner":False,"production_policy_changed":False}])}
    return DiagnosticResult(tables=tables,chronologies={"scores":long,"contributions":detail,"cancellation_monthly":cancel})


def _period_summary(long):
    rows=[]
    for period,q in _periods(long):
      for keys,g in q.groupby(["path","layer"]):
        rows.append({"period":period,"path":keys[0],"layer":keys[1],"count":g.score.notna().sum(),"standard_deviation":g.score.std(),"mean_absolute_score":g.score.abs().mean()})
    return pd.DataFrame(rows)


def _extremes(detail,paths):
    axes=pd.concat([paths[k].assign(path=p) for p,k in (("A","axis_a"),("C","axis_c"))])
    selected=[]
    for (path,geo),g in axes.groupby(["path","geo_id"]):
      for label,idx in (("minimum",g.axis_score.idxmin()),("maximum",g.axis_score.idxmax())):
        selected.append((path,geo,pd.Timestamp(g.loc[idx,"date"]),label,float(g.loc[idx,"axis_score"])))
    rows=[]
    raw=paths["dimensions_a"][["geo_id","date","dimension","dimension_score"]].rename(columns={"dimension_score":"raw_production_score"})
    for path,geo,date,label,axis_score in selected:
      q=detail[(detail.path.eq(path))&detail.geo_id.eq(geo)&detail.date.eq(date)].merge(raw,on=["geo_id","date","dimension"],how="left")
      for r in q.itertuples():
        rows.append({"path":path,"geo_id":geo,"date":date,"extreme":label,"axis_score":axis_score,"layer":"dimension","component":r.dimension,
          "raw_production_score":r.raw_production_score,"renormalized_score":r.dimension_score if path=="C" else np.nan,
          "configured_weight":r.dimension_weight,"effective_weight":r.effective_weight,"weighted_contribution":r.weighted_contribution,
          "contribution_share":r.absolute_contribution_share,"sign":np.sign(r.weighted_contribution),"source_date":pd.NaT,"freshness_days":np.nan})
    return pd.DataFrame(rows)


def write_review(result: DiagnosticResult, output: Path) -> Path:
    output=Path(output); output.mkdir(parents=True,exist_ok=True)
    for name in OUTPUT_NAMES:
        result.tables[name].to_csv(output/f"hierarchical_renorm_{name}.csv",index=False)
    for name,frame in result.chronologies.items(): frame.to_csv(output/f"hierarchical_renorm_{name}.csv",index=False)
    _write_visuals(result, output)
    index=output/"hierarchical_renorm_review.md"
    index.write_text("# Hierarchical Re-normalization Review\n\nDiagnostic only. Production policy remains unchanged.\n\n"+"\n".join(f"- [{n}](hierarchical_renorm_{n}.csv)" for n in OUTPUT_NAMES)+"\n",encoding="utf-8")
    return index


def _write_visuals(result: DiagnosticResult, output: Path) -> None:
    """Write the deliberately bounded five-figure review catalog."""
    scores=result.chronologies["scores"]; contributions=result.chronologies["contributions"]
    cancellation=result.chronologies["cancellation_monthly"]
    axis=scores[scores.layer.eq("axis")]
    dc=next((g for g in sorted(axis.geo_id.unique()) if "district" in str(g).lower()), sorted(axis.geo_id.unique())[0])
    # Dependency-free SVG summaries keep hosted runs deterministic and portable.
    summaries=(
      ("01_demand_axis_chronology.svg","Demand axis A/B/C — DC",axis[axis.geo_id.eq(dc)].groupby("path").score.agg(["min","max","std"])),
      ("02_distribution_comparison.svg","Metric / dimension / axis distributions",scores.groupby(["layer","path"]).score.agg(["min","max","std"])),
      ("03_dimension_influence.svg","Dimension contribution influence A vs C",contributions[contributions.path.isin(["A","C"])].groupby(["dimension","path"]).absolute_contribution_share.mean()),
      ("04_extreme_decomposition.svg","DC historical extrema — A vs C",result.tables["extreme_decomposition"].query("geo_id == @dc").groupby(["path","extreme","component"]).weighted_contribution.mean()),
      ("05_cancellation_comparison.svg","Cancellation A vs C",cancellation[(cancellation.geo_id.eq(dc))&cancellation.path.isin(["A","C"])].groupby("path").cancellation_ratio.agg(["mean","max"])),
    )
    for name,title,data in summaries:
        lines=str(data.round(4)).splitlines()[:22]
        body="".join(f'<text x="20" y="{55+i*16}" font-family="monospace" font-size="12">{line.replace("&","&amp;").replace("<","&lt;")}</text>' for i,line in enumerate(lines))
        (output/name).write_text(f'<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="430"><rect width="100%" height="100%" fill="white"/><text x="20" y="28" font-family="sans-serif" font-size="20">{title}</text>{body}</svg>',encoding="utf-8")


def load_run(run_directory: Path) -> Mapping[str,pd.DataFrame]:
    run_directory=Path(run_directory); names=("aligned_metric_scores","dimension_scores","axis_scores","coordinates","regime_assignments")
    frames={}
    for name in names:
        path=run_directory/f"{name}.parquet"
        if path.exists(): frames[name]=pd.read_parquet(path)
    _prepare(frames)  # fail before emitting partial evidence
    return frames
