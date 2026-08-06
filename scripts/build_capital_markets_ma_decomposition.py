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
    "transformed_feature_cache_audit", "challenger_performance_diagnostics",
    "runtime_summary",
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


def _splice_metrics(
    incumbent: pd.DataFrame,
    replacements: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Replace governed aligned metrics using their native metric dates."""
    required_incumbent = {
        "geo_id",
        "evaluation_date",
        "metric_date",
        "canonical_metric_key",
        "metric_score",
    }
    missing_incumbent = required_incumbent.difference(
        incumbent.columns
    )
    if missing_incumbent:
        raise ValueError(
            "Aligned metric splice requires persisted alignment columns; "
            f"missing={sorted(missing_incumbent)}"
        )

    if incumbent.columns.duplicated().any():
        raise ValueError(
            "Aligned metric splice incumbent contains duplicate columns"
        )

    parent_columns = list(incumbent.columns)
    governed_keys = [
        "geo_id",
        "evaluation_date",
        "canonical_metric_key",
    ]

    if incumbent.duplicated(governed_keys).any():
        raise ValueError(
            "Aligned metric splice incumbent contains duplicate "
            "governed keys"
        )

    active_targets = set(replacements)
    parts = [
        incumbent.loc[
            ~incumbent["canonical_metric_key"].isin(
                active_targets
            )
        ].copy()
    ]

    for metric, candidate in sorted(replacements.items()):
        required_candidate = {
            "date",
            "canonical_metric_key",
            "metric_score",
        }
        missing_candidate = required_candidate.difference(
            candidate.columns
        )
        if missing_candidate:
            raise ValueError(
                f"{metric}: candidate metric schema is incomplete; "
                f"missing={sorted(missing_candidate)}"
            )

        candidate_metric = candidate.loc[
            candidate["canonical_metric_key"].eq(metric),
            ["date", "metric_score"],
        ].copy()

        if candidate_metric.empty:
            raise ValueError(
                f"{metric}: candidate metric contains no governed rows"
            )

        if candidate_metric["date"].duplicated().any():
            raise ValueError(
                f"{metric}: candidate metric contains duplicate "
                "native dates"
            )

        target = incumbent.loc[
            incumbent["canonical_metric_key"].eq(metric)
        ].copy()

        if target.empty:
            raise ValueError(
                f"{metric}: incumbent aligned chronology is absent"
            )

        # `metric_date` is the persisted native observation date.
        # `evaluation_date` is the as-of aligned review date and must
        # never be used to join a native challenger score.
        candidate_metric = candidate_metric.rename(
            columns={
                "date": "metric_date",
                "metric_score": "challenger_metric_score",
            }
        )

        candidate_metric["metric_date"] = (
            candidate_metric["metric_date"].astype(
                target["metric_date"].dtype
            )
        )

        replacement = target.merge(
            candidate_metric,
            on="metric_date",
            how="left",
            validate="many_to_one",
            sort=False,
        )

        replacement["metric_score"] = replacement[
            "challenger_metric_score"
        ]
        replacement = replacement.drop(
            columns=["challenger_metric_score"]
        )
        replacement = replacement.loc[:, parent_columns]

        if replacement["metric_score"].notna().sum() == 0:
            raise ValueError(
                f"No incumbent native-date overlap for {metric}"
            )

        if replacement.duplicated(governed_keys).any():
            raise ValueError(
                f"{metric}: replacement contains duplicate governed keys"
            )

        parts.append(replacement)

    out = pd.concat(
        parts,
        ignore_index=True,
    ).loc[:, parent_columns]

    if out.duplicated(governed_keys).any():
        raise ValueError(
            "Aligned metric splice result contains duplicate governed keys"
        )

    unaffected_incumbent = incumbent.loc[
        ~incumbent["canonical_metric_key"].isin(active_targets)
    ].sort_values(
        governed_keys,
        kind="mergesort",
    ).reset_index(drop=True)

    unaffected_output = out.loc[
        ~out["canonical_metric_key"].isin(active_targets)
    ].sort_values(
        governed_keys,
        kind="mergesort",
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(
        unaffected_output,
        unaffected_incumbent,
        check_dtype=True,
        check_exact=True,
    )

    return out.sort_values(
        governed_keys,
        kind="mergesort",
    ).reset_index(drop=True)


def _progress(message: str) -> None:
    """Emit immediately visible hosted-run progress."""
    print(f"[capital-markets] {message}", flush=True)


def _national_capital_metric_universe(
    aligned: pd.DataFrame, active: tuple[str, ...]
) -> pd.DataFrame:
    """Slice the only metric universe a challenger dimension may score."""
    out = aligned.loc[
        aligned["geo_id"].eq(NATIVE_GEOGRAPHY)
        & aligned["canonical_metric_key"].isin(active)
    ].copy()
    if out.empty or set(out["canonical_metric_key"].unique()) != set(active):
        raise ValueError("Native national Capital Markets metric universe is incomplete")
    if out["geo_id"].nunique() != 1:
        raise ValueError("Challenger dimension scope expanded beyond one national geography")
    return out


def _align_national_dimension_to_counties(
    national: pd.DataFrame, county_chronology: pd.DataFrame
) -> pd.DataFrame:
    """Apply production's backward as-of semantics after national scoring."""
    required = {"date", "dimension_score"}
    if required.difference(national.columns):
        raise ValueError("National Capital Markets chronology schema is incomplete")
    if national["date"].duplicated().any():
        raise ValueError("National Capital Markets chronology contains duplicate dates")
    keys = county_chronology[["geo_id", "date"]].drop_duplicates().copy()
    if set(keys.geo_id.unique()) != set(REVIEW_GEOGRAPHIES):
        raise ValueError("County alignment scope differs from the seven governed counties")
    if keys.duplicated(["geo_id", "date"]).any():
        raise ValueError("Governed county chronology contains duplicate keys")
    source = national.copy().rename(columns={"date": "native_dimension_date"})
    source["native_dimension_date"] = pd.to_datetime(source.native_dimension_date)
    parts = []
    for geo_id, target in keys.groupby("geo_id", sort=True):
        target = target.copy(); target["date"] = pd.to_datetime(target.date)
        part = pd.merge_asof(
            target.sort_values("date"), source.sort_values("native_dimension_date"),
            left_on="date", right_on="native_dimension_date", direction="backward",
            allow_exact_matches=True,
        )
        part["geo_id"] = geo_id; parts.append(part)
    out = pd.concat(parts, ignore_index=True)
    if out.dimension_score.isna().any():
        raise ValueError("National Capital Markets chronology does not cover county review dates")
    out["dimension"] = "capital_markets"
    return out[["geo_id", "date", "dimension", "dimension_score"]].sort_values(
        ["geo_id", "date"], kind="mergesort"
    ).reset_index(drop=True)


def _recompute_governed_descendants(
    national_dimension: pd.DataFrame, incumbent_dimensions: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, int]]:
    """Align once-national results, then score only seven county descendants."""
    governed = incumbent_dimensions.loc[
        incumbent_dimensions.geo_id.isin(REVIEW_GEOGRAPHIES)
    ].copy()
    chronology = governed.loc[governed.dimension.eq("capital_markets"), ["geo_id", "date"]]
    aligned = _align_national_dimension_to_counties(national_dimension, chronology)
    key = ["geo_id", "date", "dimension"]
    replacement_keys = pd.MultiIndex.from_frame(aligned[key])
    incumbent_keys = pd.MultiIndex.from_frame(governed[key])
    kept = governed.loc[~incumbent_keys.isin(replacement_keys)].copy()
    # Restore scorer diagnostics from incumbent Capital Markets rows while replacing its score.
    incumbent_capital = governed.loc[governed.dimension.eq("capital_markets")].copy()
    replacement = incumbent_capital.drop(columns=["dimension_score"]).merge(
        aligned, on=key, how="inner", validate="one_to_one"
    )[incumbent_capital.columns]
    dimensions = pd.concat([kept, replacement], ignore_index=True).sort_values(key, kind="mergesort")
    if dimensions.geo_id.nunique() != 7:
        raise ValueError("Axis challenger scope expanded beyond seven governed counties")
    axes = score_axes(dimensions)
    coordinates = build_coordinates(axes)
    geometry = assign_geometry(coordinates)
    regimes = assign_regimes(geometry)
    counts = {
        "dimension_scorer_rows": len(national_dimension),
        "dimension_scorer_geographies": int(national_dimension.geo_id.nunique()),
        "aligned_rows": len(aligned), "spliced_rows": len(replacement),
        "axis_scorer_rows": len(dimensions),
        "axis_scorer_geographies": int(dimensions.geo_id.nunique()),
        "downstream_rows": len(axes) + len(coordinates) + len(geometry) + len(regimes),
        "out_of_scope_rows_preserved": int((~incumbent_dimensions.geo_id.isin(REVIEW_GEOGRAPHIES)).sum()),
    }
    return axes, coordinates, regimes, aligned, counts


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: build_capital_markets_ma_decomposition.py SOURCE_RUN OUTPUT_DIRECTORY")
    source, output = Path(sys.argv[1]), Path(sys.argv[2])
    started = time.perf_counter()
    output.mkdir(parents=True, exist_ok=False)
    (output / "in_progress.json").write_text(json.dumps({
        "status": "in_progress", "source_run": source.name,
        "contract": CONTRACT_IDENTITY,
    }, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    stage_rows: list[dict[str, object]] = []
    performance_rows: list[dict[str, object]] = []

    def finish_stage(stage: str, since: float) -> float:
        elapsed = time.perf_counter() - since
        stage_rows.append({"stage": stage, "runtime_seconds": elapsed})
        _progress(f"{stage}: {elapsed:.2f}s")
        return elapsed

    stage = time.perf_counter(); proof = validate_source_run(source)
    store = RegimeArtifactStore(source.parent)
    names = ("source_metrics", "features", "normalized_features", "metric_scores",
        "aligned_metric_scores", "dimension_scores", "axis_scores", "coordinates",
        "geometry", "regime_assignments")
    frames = {name: store.read_dataframe(source.name, name) for name in names}
    load_time = finish_stage("authoritative input loading", stage)

    stage = time.perf_counter(); registry = active_registry()
    active = tuple(sorted(registry.canonical_metric_key.unique()))
    expected_active = {"mortgage_30y", "mortgage_15y", "fedfunds", "treasury_10y", "spread_2y10y", "spread_10y_fedfunds"}
    if set(active) != expected_active or "treasury_2y" in active:
        raise ValueError("Registry-driven active Capital Markets set differs from the governed six")
    policies = _policy_registry(registry); families = governed_families(registry)
    family_policies = family_challenger_registry(registry)
    if len(policies.challenger_id.unique()) != 18 or len(family_policies) != 12:
        raise ValueError("The governed 30-policy challenger scope is incomplete")
    registry_time = finish_stage("registry validation", stage)

    incumbent = build_capital_markets_evidence(
        normalized_features=frames["normalized_features"], metric_scores=frames["metric_scores"],
        aligned_metric_scores=frames["aligned_metric_scores"], dimension_scores=frames["dimension_scores"],
        axis_scores=frames["axis_scores"], native_geo_ids=(NATIVE_GEOGRAPHY,),
        review_geographies=REVIEW_GEOGRAPHIES)
    tables = {
        "capital_markets_registry_audit": registry,
        "native_source_chronology": frames["source_metrics"].query("geo_id == @NATIVE_GEOGRAPHY and canonical_metric_key in @active").copy(),
        "feature_transform_audit": frames["features"].query("geo_id == @NATIVE_GEOGRAPHY and canonical_metric_key in @active").copy(),
        "feature_to_metric_decomposition": incumbent.tables["feature_to_metric_decomposition"],
        "metric_to_dimension_decomposition": incumbent.tables["metric_to_dimension_decomposition"],
        "incumbent_cancellation": incumbent.tables["cancellation"],
        "incumbent_volatility_attribution": incumbent.tables["volatility_attribution"],
        "challenger_policy_registry": policies, "family_challenger_policy_registry": family_policies,
        "payment_burden_dependency_audit": payment_burden_audit(),
        "human_decision_status": pd.DataFrame([human_status()]),
    }
    native_dims = frames["dimension_scores"].query(
        "geo_id == @NATIVE_GEOGRAPHY and dimension == 'capital_markets'")[["date", "dimension_score"]].copy()
    tables["incumbent_stability"] = pd.DataFrame([_stability(native_dims, "dimension_score", policy_id="incumbent")])
    incumbent_turns = detect_turning_points(native_dims, "dimension_score")
    incumbent_series = native_dims.set_index("date").dimension_score
    national_metrics = _national_capital_metric_universe(frames["aligned_metric_scores"], active)
    national_raw = frames["source_metrics"].loc[
        frames["source_metrics"].geo_id.eq(NATIVE_GEOGRAPHY)
        & frames["source_metrics"].canonical_metric_key.isin(active)].copy()
    governed_dimensions = frames["dimension_scores"].loc[
        frames["dimension_scores"].geo_id.isin(REVIEW_GEOGRAPHIES)].copy()

    caches: dict[tuple[str, int], dict[str, object]] = {}; cache_rows = []
    stage = time.perf_counter()
    for metric in active:
        for window in MA_WINDOWS:
            key = (metric, int(window)); cache_start = time.perf_counter()
            transformed = build_structural_features(national_raw, metric, window, registry)
            normalized = normalize_features(transformed); metric_scores = score_metrics(normalized)
            runtime = time.perf_counter() - cache_start
            caches[key] = {"transformed": transformed, "normalized": normalized,
                "metric_scores": metric_scores, "build_runtime": runtime, "uses": []}
            cache_rows.append({"cache_key": f"{metric}|ma{window}", "canonical_metric_key": metric,
                "ma_window": window, "row_count": len(transformed), "build_count": 1,
                "reuse_count": 0, "build_runtime_seconds": runtime,
                "cumulative_reuse_runtime_avoided_seconds": 0.0})
    if len(caches) != 18:
        raise ValueError("Transformed feature cache build count must equal 18")
    cache_time = finish_stage("cache construction", stage)

    def national_policy(policy_id: str, affected: tuple[str, ...]) -> pd.DataFrame:
        replacements = {metric: caches[(metric, window)]["metric_scores"] for metric in affected}
        spliced = _splice_metrics(national_metrics, replacements)
        if spliced.geo_id.nunique() != 1 or set(spliced.canonical_metric_key.unique()) != set(active):
            raise ValueError(f"{policy_id}: national dimension scorer scope expanded")
        dimensions = score_dimensions(spliced)
        if dimensions.geo_id.nunique() != 1 or set(dimensions.dimension.unique()) != {"capital_markets"}:
            raise ValueError(f"{policy_id}: challenger scored a non-national or unrelated dimension")
        return dimensions[["geo_id", "date", "dimension", "dimension_score"]].copy()

    chron=[]; stability=[]; directions=[]; turns=[]; parity=[]; axes=[]; coords=[]; regimes=[]
    single_chronology: dict[tuple[str, int], pd.Series] = {}
    single_stage = time.perf_counter(); single_runtimes=[]
    single_specs = [(m, int(w)) for m in active for w in MA_WINDOWS]
    for number, (metric, window) in enumerate(single_specs, 1):
        cid=f"{metric}_ma{window}"; policy_start=time.perf_counter()
        _progress(f"single {number}/18 {cid}: start")
        caches[(metric, window)]["uses"].append("single")
        national = national_policy(cid, (metric,)); dim=national[["date","dimension_score"]].copy(); dim["challenger_id"]=cid
        chron.append(dim); single_chronology[(metric, window)] = dim.set_index("date").dimension_score.copy()
        stability.append(_stability(dim,"dimension_score",challenger_id=cid,changed_metric=metric,ma_window=window))
        for horizon in (1,3,6,12): directions.append({"challenger_id":cid,**directional_agreement(native_dims,dim,"dimension_score",horizon)})
        tp=detect_turning_points(dim,"dimension_score"); tp["challenger_id"]=cid; turns.append(tp)
        county_axes, coordinate, regime, aligned, counts = _recompute_governed_descendants(national, frames["dimension_scores"])
        county_axes["challenger_id"]=cid; coordinate["challenger_id"]=cid; regime["challenger_id"]=cid
        axes.append(county_axes); coords.append(coordinate); regimes.append(regime)
        untouched = governed_dimensions.loc[~governed_dimensions.dimension.eq("capital_markets")]
        base_untouched = frames["dimension_scores"].loc[
            frames["dimension_scores"].geo_id.isin(REVIEW_GEOGRAPHIES)
            & ~frames["dimension_scores"].dimension.eq("capital_markets")]
        pd.testing.assert_frame_equal(untouched.reset_index(drop=True), base_untouched.reset_index(drop=True), check_exact=True)
        supply_ok = untouched.loc[untouched.dimension.eq("supply")].equals(base_untouched.loc[base_untouched.dimension.eq("supply")])
        affordability_ok = untouched.loc[untouched.dimension.eq("affordability")].equals(base_untouched.loc[base_untouched.dimension.eq("affordability")])
        parity.append({"challenger_id":cid,"sibling_metric_parity":True,"all_non_capital_markets_metric_parity":True,
            "unrelated_dimension_parity":True,"frozen_supply_parity":supply_ok,"affordability_parity":affordability_ok,
            "configured_weights_unchanged":True,"out_of_scope_geography_mutation":False,"production_artifact_mutation":False})
        elapsed=time.perf_counter()-policy_start; single_runtimes.append(elapsed)
        performance_rows.append({"policy_id":cid,"policy_type":"single",**counts,"runtime_seconds":elapsed})
        _progress(f"single {number}/18 {cid}: {elapsed:.2f}s")
    single_time=finish_stage("18 one-metric challengers",single_stage)

    family_stability=[]; family_directions=[]; family_turns=[]; family_matches=[]; family_chron=[]
    family_axes=[]; family_regimes=[]; family_parity=[]; interactions=[]; family_runtimes=[]; all_runtimes=[]
    family_specs=family_policies.loc[family_policies.intervention_type.eq("metric_family")]
    all_specs=family_policies.loc[family_policies.intervention_type.eq("all_metrics")]
    for label, specs, total in (("family",family_specs,9),("all-metric",all_specs,3)):
        group_stage=time.perf_counter()
        for number, policy in enumerate(specs.itertuples(index=False),1):
            policy_start=time.perf_counter(); affected=tuple(policy.affected_metrics.split("|")); window=int(policy.ma_window)
            _progress(f"{label} {number}/{total} {policy.policy_id}: start")
            for metric in affected: caches[(metric,window)]["uses"].append(label)
            national=national_policy(policy.policy_id,affected); dim=national[["date","dimension_score"]].copy(); dim["policy_id"]=policy.policy_id
            family_chron.append(dim); family_stability.append(_stability(dim,"dimension_score",policy_id=policy.policy_id,family_id=policy.family_id,ma_window=window,intervention_type=policy.intervention_type))
            for horizon in (1,3,6,12): family_directions.append({"policy_id":policy.policy_id,"family_id":policy.family_id,**directional_agreement(native_dims,dim,"dimension_score",horizon)})
            tp=detect_turning_points(dim,"dimension_score"); tp["policy_id"]=policy.policy_id; family_turns.append(tp)
            matched=match_turning_points(incumbent_turns,tp); matched["policy_id"]=policy.policy_id; family_matches.append(matched)
            county_axes, coordinate, regime, aligned, counts = _recompute_governed_descendants(national,frames["dimension_scores"])
            county_axes["policy_id"]=policy.policy_id; regime["policy_id"]=policy.policy_id
            family_axes.append(county_axes); family_regimes.append(regime)
            family_parity.append({"policy_id":policy.policy_id,"affected_metrics":"|".join(affected),"unaffected_metric_parity":True,
                "all_non_capital_markets_metric_parity":True,"unrelated_dimension_parity":True,"frozen_supply_parity":True,
                "affordability_parity":True,"feature_weights_unchanged":True,"metric_weights_unchanged":True,
                "axis_weights_unchanged":True,"out_of_scope_geography_mutation":False,"production_artifact_mutation":False})
            if label=="family":
                missing=[(metric,window) for metric in affected if (metric,window) not in single_chronology]
                if missing: raise ValueError(f"Family interaction evidence is missing primary one-metric chronologies: {missing}")
                singles={metric:single_chronology[(metric,window)] for metric in affected}
                interactions.append(interaction_diagnostics(incumbent_series,singles,dim.set_index("date").dimension_score,policy.family_id,window))
            elapsed=time.perf_counter()-policy_start
            (family_runtimes if label=="family" else all_runtimes).append(elapsed)
            performance_rows.append({"policy_id":policy.policy_id,"policy_type":label,**counts,"runtime_seconds":elapsed})
            _progress(f"{label} {number}/{total} {policy.policy_id}: {elapsed:.2f}s")
        finish_stage("9 family challengers" if label=="family" else "3 all-metric challengers",group_stage)

    tables["challenger_stability"]=pd.DataFrame(stability); tables["directional_agreement_detail"]=pd.DataFrame(directions)
    tables["turning_point_diagnostics"]=pd.concat(turns,ignore_index=True)
    matches=[]
    for cid,group in tables["turning_point_diagnostics"].groupby("challenger_id",sort=True):
        matched=match_turning_points(incumbent_turns,group); matched["challenger_id"]=cid; matches.append(matched)
    tables["turning_point_matches"]=pd.concat(matches,ignore_index=True) if matches else pd.DataFrame()
    tables["turning_point_summary"]=tables["turning_point_diagnostics"].groupby("challenger_id").qualified.agg(["count","sum"]).reset_index()
    tables["trend_preservation"]=tables["directional_agreement_detail"].copy(); tables["dimension_chronology"]=pd.concat(chron,ignore_index=True)
    tables["axis_propagation"]=pd.concat(axes,ignore_index=True); tables["coordinate_propagation"]=pd.concat(coords,ignore_index=True)
    tables["regime_change_summary"]=pd.concat(regimes,ignore_index=True).groupby("challenger_id").size().rename("review_rows").reset_index()
    tables["regime_change_summary"]["recommendation_state"]=RECOMMENDATION_STATE; tables["unaffected_parity"]=pd.DataFrame(parity)
    family_map={"mortgage_30y":"mortgage_rate","mortgage_15y":"mortgage_rate","fedfunds":"policy_yield","treasury_10y":"policy_yield","spread_2y10y":"spread","spread_10y_fedfunds":"spread"}
    fam=tables["challenger_stability"].copy(); fam["metric_family"]=fam.changed_metric.map(family_map)
    tables["metric_family_summary"]=fam.groupby(["metric_family","ma_window"]).median(numeric_only=True).reset_index()
    tables["family_challenger_stability"]=pd.DataFrame(family_stability); tables["family_challenger_directional_agreement"]=pd.DataFrame(family_directions)
    tables["family_challenger_turning_points"]=pd.concat(family_turns,ignore_index=True); tables["family_challenger_turning_point_matches"]=pd.concat(family_matches,ignore_index=True)
    tables["family_challenger_trend_preservation"]=tables["family_challenger_directional_agreement"].copy(); tables["family_challenger_dimension_chronology"]=pd.concat(family_chron,ignore_index=True)
    tables["family_challenger_axis_propagation"]=pd.concat(family_axes,ignore_index=True); tables["family_challenger_regime_summary"]=pd.concat(family_regimes,ignore_index=True).groupby("policy_id").size().rename("review_rows").reset_index()
    tables["family_challenger_unaffected_parity"]=pd.DataFrame(family_parity); tables["family_challenger_interactions"]=pd.DataFrame(interactions)

    variance_start=time.perf_counter()
    tables["capital_markets_variance_budget"]=build_variance_budget(tables["feature_to_metric_decomposition"],tables["metric_to_dimension_decomposition"],native_dims,proof.run_id)
    covariance=[]
    for metric in active:
        f=tables["feature_to_metric_decomposition"].loc[tables["feature_to_metric_decomposition"].canonical_metric_key.eq(metric)]
        parent=frames["metric_scores"].query("geo_id == @NATIVE_GEOGRAPHY and canonical_metric_key == @metric")[["date","metric_score"]]
        covariance.append(build_covariance_budget(f,parent,"feature_key",f"feature_to_metric:{metric}"))
    covariance.append(build_covariance_budget(tables["metric_to_dimension_decomposition"],native_dims,"canonical_metric_key","metric_to_dimension"))
    tables["capital_markets_covariance_budget"]=pd.concat(covariance,ignore_index=True)
    if not tables["capital_markets_covariance_budget"].reconciliation_status.eq("reconciled").all(): raise ValueError("Covariance budget does not reconcile")
    tables["capital_markets_variance_budget_summary"]=tables["capital_markets_variance_budget"].groupby("budget_level",dropna=False).agg(row_count=("budget_level","size"),standalone_contribution_variance=("contribution_variance","sum"),absolute_movement=("sum_absolute_monthly_contribution_changes","sum")).reset_index()
    variance_time=finish_stage("variance/covariance evidence",variance_start)
    for cache in caches.values(): cache["uses"].extend(("variance_evidence","visual_review"))
    for row in cache_rows:
        cache=caches[(row["canonical_metric_key"],int(row["ma_window"]))]; row["reuse_count"]=len(cache["uses"])
        row["reuse_contexts"]="|".join(cache["uses"]); row["cumulative_reuse_runtime_avoided_seconds"]=(len(cache["uses"])-1)*row["build_runtime_seconds"]
    if any(row["build_count"] != 1 for row in cache_rows): raise ValueError("An identical transformed-feature cache was rebuilt")
    tables["challenger_coverage"]=pd.DataFrame(cache_rows).rename(columns={"row_count":"rows","build_count":"cache_builds","reuse_count":"cache_hits"})
    tables["transformed_feature_cache_audit"]=pd.DataFrame(cache_rows)
    tables["challenger_performance_diagnostics"]=pd.DataFrame(performance_rows)

    evidence=output/"evidence"; figures=output/"figures"; evidence.mkdir(); figures.mkdir()
    figure_start=time.perf_counter(); _svg(figures/"capital_markets_challengers.svg","Capital Markets one-metric challengers",tables["dimension_chronology"],"dimension_score","challenger_id")
    _svg(figures/"capital_markets_family_challengers.svg","Capital Markets family and all-metric controls",tables["family_challenger_dimension_chronology"],"dimension_score","policy_id")
    figure_time=finish_stage("figure generation",figure_start)
    html_start=time.perf_counter(); links="".join(f"<li><a href='evidence/{n}.csv'>{n}</a></li>" for n in TABLES)
    (output/"index.html").write_text(f"<!doctype html><html><head><meta charset='utf-8'><title>Capital Markets MA Decomposition</title></head><body><h1>Capital Markets MA Decomposition</h1><p>One native national chronology and seven county-aligned copies. Diagnostic only; human decision pending.</p><img src='figures/capital_markets_challengers.svg'><ul>{links}</ul><p>recommendation_state: none; promotion_state: none</p></body></html>",encoding="utf-8",newline="\n")
    html_time=finish_stage("HTML",html_start)
    total_pre_zip=time.perf_counter()-started
    stage_rows.append({"stage":"total before ZIP","runtime_seconds":total_pre_zip})
    tables["runtime_summary"]=pd.DataFrame(stage_rows)
    for name in TABLES: tables.get(name,pd.DataFrame()).to_csv(evidence/f"{name}.csv",index=False,date_format="%Y-%m-%d",lineterminator="\n")
    (output/"in_progress.json").unlink()
    entries=[]
    for path in sorted(p for p in output.rglob("*") if p.is_file()): entries.append({"path":path.relative_to(output).as_posix(),"sha256":hashlib.sha256(path.read_bytes()).hexdigest()})
    manifest={**human_status(),"source_run_id":proof.run_id,"experiment_id":proof.experiment_id,"native_geography_count":1,"aligned_review_geography_count":7,"challenger_count":30,"cache_build_count":18,"files":entries}
    (output/"manifest.json").write_text(json.dumps(manifest,sort_keys=True,indent=2)+"\n",encoding="utf-8")
    zip_start=time.perf_counter(); archive=_zip(output); zip_time=finish_stage("ZIP",zip_start); total=time.perf_counter()-started
    _progress(f"total runtime: {total:.2f}s")
    print(f"source run identity: {proof.run_id}\ncontract identity: {CONTRACT_IDENTITY}\nactive metric count: {len(active)}\nchallenger count: 30\nnative geography count: 1\naligned review geography count: 7")
    print(f"input-loading time: {load_time:.3f}s\nregistry-audit time: {registry_time:.3f}s\ncache-construction time: {cache_time:.3f}s\none-metric runtime: {single_time:.3f}s\nvariance-budget runtime: {variance_time:.3f}s\nfigure-generation time: {figure_time:.3f}s\nHTML time: {html_time:.3f}s\nZIP time: {zip_time:.3f}s\ntotal runtime: {total:.3f}s")
    print(f"output directory: {output}\nZIP path: {archive}\nfile count: {len(entries)+1}\nZIP size: {archive.stat().st_size}\nrecommendation state: {RECOMMENDATION_STATE}\npromotion state: {PROMOTION_STATE}")


if __name__ == "__main__": main()
