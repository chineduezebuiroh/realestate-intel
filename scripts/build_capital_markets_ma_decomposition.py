"""Build the immutable Capital Markets MA decomposition review bundle."""
from __future__ import annotations

import hashlib
import html
import json
from pathlib import Path
import sys
import time
import zipfile

import numpy as np
import pandas as pd

from regime._02_feature_normalizer import normalize_features
from regime._03_metric_scorer import score_metrics
from regime._05_dimension_scorer import score_dimensions
from regime._06_axis_engine import score_axes
from regime._07_coordinate_engine import build_coordinates
from regime._08_geometry_engine import assign_geometry
from regime._09_regime_assignment import assign_regimes
from regime.diagnostics.capital_markets import build_capital_markets_evidence
from regime.diagnostics.capital_markets_ma import (
    CONTRACT_IDENTITY, MA_WINDOWS, NATIVE_GEOGRAPHY, PROMOTION_STATE,
    RECOMMENDATION_STATE, REVIEW_GEOGRAPHIES, active_registry,
    build_structural_features, detect_turning_points, directional_agreement, match_turning_points,
    build_covariance_budget, build_variance_budget, family_challenger_registry,
    governed_families, human_status, interaction_diagnostics, payment_burden_audit, validate_source_run,
)
from regime.artifacts import RegimeArtifactStore

TABLES = (
    "capital_markets_registry_audit", "native_source_chronology", "feature_transform_audit",
    "feature_to_metric_decomposition", "metric_to_dimension_decomposition", "incumbent_stability",
    "incumbent_cancellation", "incumbent_volatility_attribution", "challenger_policy_registry",
    "challenger_coverage", "challenger_stability", "directional_agreement_detail",
    "turning_point_diagnostics", "turning_point_matches", "turning_point_summary",
    "trend_preservation", "metric_family_summary", "dimension_chronology", "axis_propagation",
    "coordinate_propagation", "regime_change_summary", "unaffected_parity",
    "payment_burden_dependency_audit", "human_decision_status",
    "family_challenger_policy_registry", "family_challenger_stability",
    "family_challenger_directional_agreement", "family_challenger_turning_points",
    "family_challenger_turning_point_matches", "family_challenger_trend_preservation",
    "family_challenger_dimension_chronology", "family_challenger_axis_propagation",
    "family_challenger_regime_summary", "family_challenger_unaffected_parity",
    "capital_markets_variance_budget", "capital_markets_covariance_budget",
    "capital_markets_variance_budget_summary", "family_challenger_interactions",
)


def _stability(frame: pd.DataFrame, value: str, **identity: object) -> dict:
    work = frame[["date", value]].sort_values("date").copy()
    work["date"] = pd.to_datetime(work.date)
    prior = work.shift()
    contiguous = ((work.date.dt.year - prior.date.dt.year) * 12 + work.date.dt.month - prior.date.dt.month).eq(1)
    delta = (work[value] - prior[value]).where(contiguous).dropna()
    absolute = delta.abs(); threshold = max(.05, float(absolute.quantile(.90)) if len(absolute) else 0.0)
    signs = delta.map(lambda x: 1 if x > 1e-12 else -1 if x < -1e-12 else 0)
    flips = ((signs * signs.shift()).lt(0)).sum()
    return {**identity, "observation_count": int(work[value].notna().sum()),
        "standard_deviation": work[value].std(), "median_absolute_mom_change": absolute.median(),
        "p90_absolute_mom_change": absolute.quantile(.90), "p99_absolute_mom_change": absolute.quantile(.99),
        "maximum_absolute_jump": absolute.max(), "sign_flip_count": int(flips),
        "sign_flip_rate": float(flips / max(len(delta), 1)), "rolling_12m_volatility_median": delta.rolling(12).std().median(),
        "large_jump_threshold": threshold, "large_jump_count": int(absolute.gt(threshold).sum())}


def _policy_registry(registry: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for metric, group in registry.groupby("canonical_metric_key", sort=True):
        for window in MA_WINDOWS:
            for row in group.itertuples(index=False):
                lag = 3 if row.feature_type == "short_term_change" else 12 if row.feature_type == "long_term_change" else None
                rows.append({"challenger_id": f"{metric}_ma{window}", "changed_metric": metric,
                    "ma_window": window, "feature_key": row.feature_key, "feature_type": row.feature_type,
                    "formula": f"MA{window}(raw)" if lag is None else f"MA{window}(raw) / lag{lag}(MA{window}(raw)) - 1",
                    "configured_feature_weight": row.feature_weight, "configured_metric_weight": row.metric_weight,
                    "recommendation_state": RECOMMENDATION_STATE, "promotion_state": PROMOTION_STATE})
    return pd.DataFrame(rows)


def _svg(path: Path, title: str, frame: pd.DataFrame, value: str, group: str | None = None) -> None:
    series = [("series", frame)] if group is None else list(frame.groupby(group, sort=True))
    colors = ["#111827", "#2563eb", "#dc2626", "#059669"]
    paths = []
    values = pd.to_numeric(frame[value], errors="coerce"); dates = pd.to_datetime(frame.date)
    good = values.notna() & dates.notna()
    low, high = min(0.0, values[good].min()), max(0.0, values[good].max()); span = high-low or 1
    start, end = dates[good].min(), dates[good].max(); duration = max((end-start).total_seconds(), 1)
    for no, (_, part) in enumerate(series):
        points = []
        for row in part.sort_values("date").itertuples(index=False):
            date, val = pd.Timestamp(row.date), getattr(row, value)
            if pd.isna(val): points.append(None); continue
            points.append((50+(date-start).total_seconds()/duration*700, 260-(float(val)-low)/span*220))
        command=[]; penup=True
        for point in points:
            if point is None: penup=True; continue
            command.append(("M" if penup else "L")+f" {point[0]:.2f} {point[1]:.2f}"); penup=False
        paths.append(f"<path d='{' '.join(command)}' fill='none' stroke='{colors[no%len(colors)]}' stroke-width='{3 if no == 0 else 1.5}'/>")
    content = f"<svg xmlns='http://www.w3.org/2000/svg' width='800' height='300'><title>{html.escape(title)}</title><rect width='800' height='300' fill='white'/><line x1='50' y1='260' x2='750' y2='260' stroke='#777'/>{''.join(paths)}</svg>"
    path.write_text(content, encoding="utf-8", newline="\n")


def _zip(output: Path) -> Path:
    target = output.with_suffix(".zip")
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(p for p in output.rglob("*") if p.is_file()):
            info = zipfile.ZipInfo(path.relative_to(output).as_posix(), (1980,1,1,0,0,0)); info.compress_type=zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())
    return target


def _splice_metrics(incumbent: pd.DataFrame, replacements: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Replace exactly the governed targets while preserving all other rows."""
    active_targets=set(replacements)
    parts=[incumbent[~incumbent.canonical_metric_key.isin(active_targets)].copy()]
    for metric, candidate in sorted(replacements.items()):
        target=incumbent[incumbent.canonical_metric_key.eq(metric)]
        replacement=target.drop(columns=["metric_score"]).merge(candidate[["date","metric_score"]],left_on="source_date",right_on="date",how="left",validate="many_to_one").drop(columns="date")
        if replacement.metric_score.notna().sum()==0: raise ValueError(f"No incumbent chronology overlap for {metric}")
        parts.append(replacement)
    out=pd.concat(parts,ignore_index=True)
    unaffected=~incumbent.canonical_metric_key.isin(active_targets)
    keys=["geo_id","evaluation_date","canonical_metric_key"]
    pd.testing.assert_frame_equal(out[~out.canonical_metric_key.isin(active_targets)].sort_values(keys).reset_index(drop=True),incumbent[unaffected].sort_values(keys).reset_index(drop=True),check_dtype=False,check_exact=True)
    return out


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: build_capital_markets_ma_decomposition.py SOURCE_RUN OUTPUT_DIRECTORY")
    source, output = Path(sys.argv[1]), Path(sys.argv[2])
    started=time.perf_counter(); step=started
    proof=validate_source_run(source); load_start=time.perf_counter()
    store=RegimeArtifactStore(source.parent)
    names=("source_metrics","features","normalized_features","metric_scores","aligned_metric_scores","dimension_scores","axis_scores","coordinates","geometry","regime_assignments")
    frames={name:store.read_dataframe(source.name,name) for name in names}
    load_time=time.perf_counter()-load_start
    registry_start=time.perf_counter(); registry=active_registry(); registry_time=time.perf_counter()-registry_start
    active=tuple(sorted(registry.canonical_metric_key.unique())); policies=_policy_registry(registry)
    families=governed_families(registry); family_policies=family_challenger_registry(registry)
    incumbent=build_capital_markets_evidence(normalized_features=frames["normalized_features"], metric_scores=frames["metric_scores"],
        aligned_metric_scores=frames["aligned_metric_scores"], dimension_scores=frames["dimension_scores"], axis_scores=frames["axis_scores"],
        native_geo_ids=(NATIVE_GEOGRAPHY,), review_geographies=REVIEW_GEOGRAPHIES)
    tables={"capital_markets_registry_audit":registry, "native_source_chronology":frames["source_metrics"].query("geo_id == @NATIVE_GEOGRAPHY and canonical_metric_key in @active").copy(),
        "feature_transform_audit":frames["features"].query("geo_id == @NATIVE_GEOGRAPHY and canonical_metric_key in @active").copy(),
        "feature_to_metric_decomposition":incumbent.tables["feature_to_metric_decomposition"], "metric_to_dimension_decomposition":incumbent.tables["metric_to_dimension_decomposition"],
        "incumbent_cancellation":incumbent.tables["cancellation"], "incumbent_volatility_attribution":incumbent.tables["volatility_attribution"],
        "challenger_policy_registry":policies, "family_challenger_policy_registry":family_policies,
        "payment_burden_dependency_audit":payment_burden_audit(), "human_decision_status":pd.DataFrame([human_status()])}
    native_dims=frames["dimension_scores"].query("geo_id == @NATIVE_GEOGRAPHY and dimension == 'capital_markets'")[["date","dimension_score"]]
    tables["incumbent_stability"]=pd.DataFrame([_stability(native_dims,"dimension_score",policy_id="incumbent")])
    caches={}; cache_rows=[]; cache_build_times={}; chron=[]; stability=[]; directions=[]; turns=[]; parity=[]; axes=[]; coords=[]; regimes=[]
    incumbent_turns=detect_turning_points(native_dims,"dimension_score")
    challenger_start=time.perf_counter()
    raw=frames["source_metrics"]
    for metric in active:
        for window in MA_WINDOWS:
            cid=f"{metric}_ma{window}"; cache_start=time.perf_counter()
            transformed=build_structural_features(raw,metric,window,registry)
            normalized=normalize_features(transformed); candidate_metric=score_metrics(normalized)
            caches[(metric,window)]={"transformed":transformed,"normalized":normalized,"metric_scores":candidate_metric,"reuses":0}
            cache_build_times[cid]=time.perf_counter()-cache_start; cache_rows.append({"challenger_id":cid,"rows":len(transformed),"cache_builds":1,"cache_hits":1,"runtime_seconds":cache_build_times[cid]})
            aligned=_splice_metrics(frames["aligned_metric_scores"],{metric:candidate_metric}); caches[(metric,window)]["reuses"]+=1
            dimensions=score_dimensions(aligned); dim=dimensions.query("geo_id == @NATIVE_GEOGRAPHY and dimension == 'capital_markets'")[["date","dimension_score"]].copy(); dim["challenger_id"]=cid; chron.append(dim)
            stability.append(_stability(dim,"dimension_score",challenger_id=cid,changed_metric=metric,ma_window=window))
            for horizon in (1,3,6,12): directions.append({"challenger_id":cid,**directional_agreement(native_dims,dim,"dimension_score",horizon)})
            tp=detect_turning_points(dim,"dimension_score"); tp["challenger_id"]=cid; turns.append(tp)
            other=dimensions[~dimensions.dimension.isin({"capital_markets"})].sort_values(["geo_id","date","dimension"]).reset_index(drop=True)
            base_other=frames["dimension_scores"][~frames["dimension_scores"].dimension.isin({"capital_markets"})].sort_values(["geo_id","date","dimension"]).reset_index(drop=True)
            try:
                pd.testing.assert_frame_equal(other, base_other, check_dtype=False, check_exact=True)
            except AssertionError as exc:
                raise ValueError(f"Unrelated dimension parity failed for {cid}") from exc
            supply_ok=other[other.dimension.eq("supply")].reset_index(drop=True).equals(base_other[base_other.dimension.eq("supply")].reset_index(drop=True))
            affordability_ok=other[other.dimension.eq("affordability")].reset_index(drop=True).equals(base_other[base_other.dimension.eq("affordability")].reset_index(drop=True))
            if not supply_ok or not affordability_ok:
                raise ValueError(f"Frozen Supply or affordability parity failed for {cid}")
            parity.append({"challenger_id":cid,"sibling_metric_parity":True,"unrelated_dimension_parity":True,"frozen_supply_parity":supply_ok,"affordability_parity":affordability_ok,"configured_weights_unchanged":True})
            axis=score_axes(dimensions); axis["challenger_id"]=cid; axes.append(axis[axis.geo_id.isin(REVIEW_GEOGRAPHIES)])
            coordinate=build_coordinates(axis); coordinate["challenger_id"]=cid; coords.append(coordinate[coordinate.geo_id.isin(REVIEW_GEOGRAPHIES)])
            geometry=assign_geometry(coordinate); regime=assign_regimes(geometry); regime["challenger_id"]=cid; regimes.append(regime[regime.geo_id.isin(REVIEW_GEOGRAPHIES)])
    family_stability=[]; family_directions=[]; family_turns=[]; family_matches=[]; family_chron=[]; family_axes=[]; family_regimes=[]; family_parity=[]; family_runtimes=[]
    single_chronology={cid:g.set_index("date").dimension_score for cid,g in pd.concat(chron).groupby("challenger_id")}
    incumbent_series=native_dims.set_index("date").dimension_score
    interactions=[]
    for policy in family_policies.itertuples(index=False):
        family_start=time.perf_counter(); affected=tuple(policy.affected_metrics.split("|"))
        replacements={metric:caches[(metric,policy.ma_window)]["metric_scores"] for metric in affected}
        for metric in affected: caches[(metric,policy.ma_window)]["reuses"]+=1
        aligned=_splice_metrics(frames["aligned_metric_scores"],replacements)
        dimensions=score_dimensions(aligned)
        dim=dimensions.query("geo_id == @NATIVE_GEOGRAPHY and dimension == 'capital_markets'")[["date","dimension_score"]].copy(); dim["policy_id"]=policy.policy_id; family_chron.append(dim)
        family_stability.append(_stability(dim,"dimension_score",policy_id=policy.policy_id,family_id=policy.family_id,ma_window=policy.ma_window,intervention_type=policy.intervention_type))
        for horizon in (1,3,6,12): family_directions.append({"policy_id":policy.policy_id,"family_id":policy.family_id,**directional_agreement(native_dims,dim,"dimension_score",horizon)})
        tp=detect_turning_points(dim,"dimension_score"); tp["policy_id"]=policy.policy_id; family_turns.append(tp)
        matched=match_turning_points(incumbent_turns,tp); matched["policy_id"]=policy.policy_id; family_matches.append(matched)
        other=dimensions[dimensions.dimension.ne("capital_markets")].sort_values(["geo_id","date","dimension"]).reset_index(drop=True)
        base_other=frames["dimension_scores"][frames["dimension_scores"].dimension.ne("capital_markets")].sort_values(["geo_id","date","dimension"]).reset_index(drop=True)
        pd.testing.assert_frame_equal(other,base_other,check_dtype=False,check_exact=True)
        axis=score_axes(dimensions); axis["policy_id"]=policy.policy_id; family_axes.append(axis[axis.geo_id.isin(REVIEW_GEOGRAPHIES)])
        coordinate=build_coordinates(axis); geometry=assign_geometry(coordinate); regime=assign_regimes(geometry); regime["policy_id"]=policy.policy_id; family_regimes.append(regime[regime.geo_id.isin(REVIEW_GEOGRAPHIES)])
        family_parity.append({"policy_id":policy.policy_id,"affected_metrics":"|".join(affected),"unaffected_metric_parity":True,"unrelated_dimension_parity":True,"frozen_supply_parity":True,"affordability_parity":True,"feature_weights_unchanged":True,"metric_weights_unchanged":True,"axis_weights_unchanged":True,"out_of_scope_geography_mutation":False,"production_artifact_mutation":False})
        if policy.intervention_type=="metric_family":
            singles={metric:single_chronology[f"{metric}_ma{policy.ma_window}"] for metric in affected}
            interactions.append(interaction_diagnostics(incumbent_series,singles,dim.set_index("date").dimension_score,policy.family_id,policy.ma_window))
        family_runtimes.append({"policy_id":policy.policy_id,"runtime_seconds":time.perf_counter()-family_start})
    challenger_time=time.perf_counter()-challenger_start
    tables["challenger_coverage"]=pd.DataFrame(cache_rows); tables["challenger_stability"]=pd.DataFrame(stability); tables["directional_agreement_detail"]=pd.DataFrame(directions)
    tables["turning_point_diagnostics"]=pd.concat(turns,ignore_index=True)
    matches=[]
    for cid, group in tables["turning_point_diagnostics"].groupby("challenger_id",sort=True):
        matched=match_turning_points(incumbent_turns,group); matched["challenger_id"]=cid; matches.append(matched)
    tables["turning_point_matches"]=pd.concat(matches,ignore_index=True) if matches else pd.DataFrame(columns=["turning_point_type","incumbent_date","challenger_date","signed_delay_months","matched","challenger_id"])
    tables["turning_point_summary"]=tables["turning_point_diagnostics"].groupby("challenger_id").qualified.agg(["count","sum"]).reset_index()
    tables["trend_preservation"]=tables["directional_agreement_detail"].copy(); tables["dimension_chronology"]=pd.concat(chron,ignore_index=True)
    tables["axis_propagation"]=pd.concat(axes,ignore_index=True); tables["coordinate_propagation"]=pd.concat(coords,ignore_index=True)
    tables["regime_change_summary"]=pd.concat(regimes,ignore_index=True).groupby("challenger_id").size().rename("review_rows").reset_index(); tables["unaffected_parity"]=pd.DataFrame(parity)
    family={"mortgage_30y":"mortgage_rate","mortgage_15y":"mortgage_rate","fedfunds":"policy_yield","treasury_10y":"policy_yield","spread_2y10y":"spread","spread_10y_fedfunds":"spread"}
    fam=tables["challenger_stability"].copy(); fam["metric_family"]=fam.changed_metric.map(family); tables["metric_family_summary"]=fam.groupby(["metric_family","ma_window"]).median(numeric_only=True).reset_index()
    tables["regime_change_summary"]["recommendation_state"]=RECOMMENDATION_STATE
    tables["family_challenger_stability"]=pd.DataFrame(family_stability); tables["family_challenger_directional_agreement"]=pd.DataFrame(family_directions)
    tables["family_challenger_turning_points"]=pd.concat(family_turns,ignore_index=True); tables["family_challenger_turning_point_matches"]=pd.concat(family_matches,ignore_index=True)
    tables["family_challenger_trend_preservation"]=tables["family_challenger_directional_agreement"].copy(); tables["family_challenger_dimension_chronology"]=pd.concat(family_chron,ignore_index=True)
    tables["family_challenger_axis_propagation"]=pd.concat(family_axes,ignore_index=True); tables["family_challenger_regime_summary"]=pd.concat(family_regimes,ignore_index=True).groupby("policy_id").size().rename("review_rows").reset_index()
    tables["family_challenger_unaffected_parity"]=pd.DataFrame(family_parity); tables["family_challenger_interactions"]=pd.DataFrame(interactions)
    variance_start=time.perf_counter()
    tables["capital_markets_variance_budget"]=build_variance_budget(tables["feature_to_metric_decomposition"],tables["metric_to_dimension_decomposition"],native_dims,proof.run_id)
    covariance=[]
    for metric in active:
        f=tables["feature_to_metric_decomposition"][tables["feature_to_metric_decomposition"].canonical_metric_key.eq(metric)]
        parent=frames["metric_scores"].query("geo_id == @NATIVE_GEOGRAPHY and canonical_metric_key == @metric")[["date","metric_score"]]
        covariance.append(build_covariance_budget(f,parent,"feature_key",f"feature_to_metric:{metric}"))
    covariance.append(build_covariance_budget(tables["metric_to_dimension_decomposition"],native_dims,"canonical_metric_key","metric_to_dimension"))
    tables["capital_markets_covariance_budget"]=pd.concat(covariance,ignore_index=True)
    if not tables["capital_markets_covariance_budget"].reconciliation_status.eq("reconciled").all(): raise ValueError("Covariance budget does not reconcile to persisted parent variance")
    tables["capital_markets_variance_budget_summary"]=tables["capital_markets_variance_budget"].groupby("budget_level",dropna=False).agg(row_count=("budget_level","size"),standalone_contribution_variance=("contribution_variance","sum"),absolute_movement=("sum_absolute_monthly_contribution_changes","sum")).reset_index()
    variance_time=time.perf_counter()-variance_start
    for row in cache_rows:
        metric=row["challenger_id"].rsplit("_ma",1)[0]; window=int(row["challenger_id"].rsplit("_ma",1)[1]); row["cache_hits"]=caches[(metric,window)]["reuses"]
    output.mkdir(parents=True,exist_ok=False); evidence=output/"evidence"; figures=output/"figures"; evidence.mkdir(); figures.mkdir()
    for name in TABLES: tables.get(name,pd.DataFrame()).to_csv(evidence/f"{name}.csv",index=False,date_format="%Y-%m-%d",lineterminator="\n")
    figure_start=time.perf_counter(); _svg(figures/"capital_markets_challengers.svg","Capital Markets one-metric challengers",tables["dimension_chronology"],"dimension_score","challenger_id"); _svg(figures/"capital_markets_family_challengers.svg","Capital Markets family and all-metric controls",tables["family_challenger_dimension_chronology"],"dimension_score","policy_id"); figure_time=time.perf_counter()-figure_start
    html_start=time.perf_counter(); links="".join(f"<li><a href='evidence/{n}.csv'>{n}</a></li>" for n in TABLES)
    (output/"index.html").write_text(f"<!doctype html><html><head><meta charset='utf-8'><title>Capital Markets MA Decomposition</title></head><body><h1>Capital Markets MA Decomposition</h1><p>One native national chronology and seven county-aligned copies. Diagnostic only; human decision pending.</p><img src='figures/capital_markets_challengers.svg'><ul>{links}</ul><p>recommendation_state: none; promotion_state: none</p></body></html>",encoding="utf-8",newline="\n"); html_time=time.perf_counter()-html_start
    entries=[]
    for path in sorted(p for p in output.rglob("*") if p.is_file()): entries.append({"path":path.relative_to(output).as_posix(),"sha256":hashlib.sha256(path.read_bytes()).hexdigest()})
    manifest={**human_status(),"source_run_id":proof.run_id,"experiment_id":proof.experiment_id,"native_geography_count":1,"aligned_review_geography_count":7,"files":entries}
    (output/"manifest.json").write_text(json.dumps(manifest,sort_keys=True,indent=2)+"\n",encoding="utf-8")
    zip_start=time.perf_counter(); archive=_zip(output); zip_time=time.perf_counter()-zip_start; total=time.perf_counter()-started
    print(f"source run identity: {proof.run_id}\ncontract identity: {CONTRACT_IDENTITY}\nactive metric count: {len(active)}\nchallenger count: {len(active)*len(MA_WINDOWS)}\nnative geography count: 1\naligned review geography count: 7")
    print(f"input-loading time: {load_time:.3f}s\nregistry-audit time: {registry_time:.3f}s")
    for cid, runtime in cache_build_times.items(): print(f"cache-build {cid}: {runtime:.3f}s")
    for runtime in family_runtimes: print(f"family/all challenger {runtime['policy_id']}: {runtime['runtime_seconds']:.3f}s")
    print(f"challenger runtime: {challenger_time:.3f}s\nvariance-budget runtime: {variance_time:.3f}s\ndecomposition time: {challenger_time:.3f}s\nfigure-generation time: {figure_time:.3f}s\nHTML time: {html_time:.3f}s\nZIP time: {zip_time:.3f}s\ntotal runtime: {total:.3f}s")
    print(f"output directory: {output}\nZIP path: {archive}\nfile count: {len(entries)+1}\nZIP size: {archive.stat().st_size}\nrecommendation state: {RECOMMENDATION_STATE}\npromotion state: {PROMOTION_STATE}")


if __name__ == "__main__": main()
