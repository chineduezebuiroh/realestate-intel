"""Build the authoritative, artifact-backed feature-weight experiment review."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import time
from typing import Mapping, Sequence

import pandas as pd

from regime._02_feature_normalizer import normalize_features
from regime._04_asof_aligner import align_metric_scores_asof
from regime._05_dimension_scorer import score_dimensions
from regime._06_axis_engine import score_axes
from regime._07_coordinate_engine import build_coordinates
from regime._08_geometry_engine import assign_geometry
from regime._09_regime_assignment import assign_regimes
from regime.artifacts import RegimeArtifactStore
from regime.experiments.linked_price_family_features import build_linked_price_family_features
from regime.smoothing_features import build_smoothed_metric_features_wide
from regime.smoothing_policy import SmoothingMetricPolicy
from regime.diagnostics.feature_weight_experiment import (
    ALTERNATIVES, CONTRACT_VERSION, POLICY_ORDER, REVIEW_GEOGRAPHIES,
    TARGET_METRICS, audit_feature_registry, build_evidence,
    summarize_propagation, validate_unaffected_parity, write_review_bundle,
)

REQUIRED_ARTIFACTS = (
    "source_metrics", "features", "normalized_features", "metric_scores",
    "aligned_metric_scores", "dimension_scores", "axis_scores", "coordinates",
    "geometry", "regime_assignments",
)


@dataclass(frozen=True, slots=True)
class AuthoritativeInputs:
    identity: str
    manifest: Mapping[str, object]
    frames: Mapping[str, pd.DataFrame]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_directory", type=Path)
    parser.add_argument("output_directory", type=Path)
    return parser.parse_args(argv)


def _required_columns() -> Mapping[str, set[str]]:
    return {
        "normalized_features": {"geo_id", "date", "canonical_metric_key", "feature_key", "feature_score"},
        "metric_scores": {"geo_id", "date", "canonical_metric_key", "metric_score"},
        "aligned_metric_scores": {"geo_id", "evaluation_date", "canonical_metric_key", "metric_score"},
        "dimension_scores": {"geo_id", "date", "dimension", "dimension_score"},
        "axis_scores": {"geo_id", "date", "axis", "axis_score"},
        "coordinates": {"geo_id", "date", "x_supply", "y_demand"},
        "geometry": {"geo_id", "date", "major_regime", "minor_regime"},
        "regime_assignments": {"geo_id", "date", "major_regime", "minor_regime"},
        "features": {"geo_id", "date", "canonical_metric_key", "feature_key"},
        "source_metrics": {"geo_id", "date", "canonical_metric_key"},
    }


def load_authoritative_inputs(run_directory: Path) -> AuthoritativeInputs:
    run_directory = run_directory.resolve()
    if not run_directory.is_dir():
        raise FileNotFoundError(f"Authoritative run directory does not exist: {run_directory}")
    store = RegimeArtifactStore(run_directory.parent)
    run_id = run_directory.name
    manifest = store.read_manifest(run_id)
    if manifest.get("status") != "complete":
        raise ValueError(f"Authoritative run is not ready: status={manifest.get('status')!r}")
    if manifest.get("run_id") != run_id:
        raise ValueError("Authoritative source identity does not match the run directory")
    verification = store.verify_run(run_id)
    if verification.empty or not verification.exists.all() or not verification.hash_matches.all():
        raise ValueError("Authoritative manifest readiness/hash verification failed")
    frames = {}
    for name in REQUIRED_ARTIFACTS:
        frames[name] = store.read_dataframe(run_id, name, verify_hash=True)
        missing = _required_columns()[name].difference(frames[name].columns)
        if missing:
            raise ValueError(f"Authoritative {name} schema is missing columns: {sorted(missing)}")
    absent = set(REVIEW_GEOGRAPHIES).difference(frames["dimension_scores"].geo_id.unique())
    if absent:
        raise ValueError(f"Authoritative run is missing governed geographies: {sorted(absent)}")
    normalized_metrics = set(frames["normalized_features"].canonical_metric_key)
    missing_metrics = set(TARGET_METRICS).difference(normalized_metrics)
    if missing_metrics:
        raise ValueError(f"Authoritative normalized features are missing targets: {sorted(missing_metrics)}")
    identity_payload = json.dumps({"run_id": run_id, "experiment_id": manifest.get("experiment_id"),
                                   "artifacts": manifest.get("artifacts")}, sort_keys=True).encode()
    identity = f"{run_id}:{hashlib.sha256(identity_payload).hexdigest()}"
    return AuthoritativeInputs(identity, manifest, frames)


def _splice(
    parent: pd.DataFrame,
    replacement: pd.DataFrame,
    column: str,
    value: str,
    *,
    geo_ids: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Replace one governed identity with strict schema and scoped parity."""
    if parent.columns.duplicated().any():
        raise ValueError("Splice parent contains duplicate columns")
    if replacement.columns.duplicated().any():
        raise ValueError("Splice replacement contains duplicate columns")

    parent_columns = list(parent.columns)
    parent_set = set(parent_columns)
    replacement_set = set(replacement.columns)
    if parent_set != replacement_set:
        raise ValueError(
            "Splice schema mismatch; "
            f"parent_only={sorted(parent_set - replacement_set)}, "
            f"replacement_only={sorted(replacement_set - parent_set)}"
        )

    replacement = replacement.loc[:, parent_columns].copy()

    if not replacement[column].eq(value).all():
        raise ValueError(
            f"Splice replacement contains identities outside {column}={value!r}"
        )

    replace_mask = parent[column].eq(value)

    governed_geos: set[str] | None = None
    if geo_ids is not None:
        governed_geos = {str(geo_id) for geo_id in geo_ids}
        replacement_geos = set(replacement["geo_id"].astype(str))

        unexpected_geos = replacement_geos.difference(governed_geos)
        if unexpected_geos:
            raise ValueError(
                "Splice replacement contains geography outside governed scope: "
                f"{sorted(unexpected_geos)}"
            )

        replace_mask &= parent["geo_id"].astype(str).isin(governed_geos)

    # Production artifacts may persist nanosecond timestamps while frames
    # derived from Parquet-backed features retain microseconds.
    for name in parent_columns:
        parent_dtype = parent[name].dtype
        replacement_dtype = replacement[name].dtype

        if (
            pd.api.types.is_datetime64_any_dtype(parent_dtype)
            and pd.api.types.is_datetime64_any_dtype(replacement_dtype)
        ):
            replacement[name] = replacement[name].astype(parent_dtype)
        elif parent_dtype != replacement_dtype:
            raise ValueError(
                "Splice dtype mismatch; "
                f"column={name!r}, parent={parent_dtype}, "
                f"replacement={replacement_dtype}"
            )

    chronology = (
        "evaluation_date"
        if "evaluation_date" in parent_columns
        else "date"
    )
    keys = ["geo_id", chronology, column]

    if parent.duplicated(keys).any():
        raise ValueError("Splice parent contains duplicate governed keys")
    if replacement.duplicated(keys).any():
        raise ValueError("Splice replacement contains duplicate governed keys")

    preserved_out_of_scope = None
    if governed_geos is not None:
        preserved_out_of_scope = (
            parent.loc[
                parent[column].eq(value)
                & ~parent["geo_id"].astype(str).isin(governed_geos)
            ]
            .sort_values(keys, kind="mergesort")
            .reset_index(drop=True)
        )

    mixed = pd.concat(
        [
            parent.loc[~replace_mask],
            replacement,
        ],
        ignore_index=True,
    )

    if mixed.duplicated(keys).any():
        raise ValueError("Splice result contains duplicate governed keys")

    mixed = mixed.sort_values(
        keys,
        kind="mergesort",
    ).reset_index(drop=True)

    if preserved_out_of_scope is not None:
        mixed_out_of_scope = (
            mixed.loc[
                mixed[column].eq(value)
                & ~mixed["geo_id"].astype(str).isin(governed_geos)
            ]
            .sort_values(keys, kind="mergesort")
            .reset_index(drop=True)
        )
        try:
            pd.testing.assert_frame_equal(
                preserved_out_of_scope,
                mixed_out_of_scope,
                check_exact=True,
                check_dtype=True,
            )
        except AssertionError as exc:
            raise ValueError(
                "Splice changed target rows outside governed geography scope"
            ) from exc

    return mixed


def _regime_changes(incumbent: pd.DataFrame, challenger: pd.DataFrame, metric: str, policy: str) -> pd.DataFrame:
    keys = ["geo_id", "date"]
    cols = keys + ["major_regime", "minor_regime"]
    merged = incumbent[cols].merge(challenger[cols], on=keys, suffixes=("_incumbent", "_challenger"), validate="one_to_one")
    merged["metric"] = metric; merged["policy"] = policy
    merged["major_changed"] = merged.major_regime_incumbent.ne(merged.major_regime_challenger)
    merged["minor_changed"] = merged.minor_regime_incumbent.ne(merged.minor_regime_challenger)
    for suffix in ("incumbent", "challenger"):
        merged[f"transition_{suffix}"] = merged.groupby("geo_id")[f"minor_regime_{suffix}"].transform(lambda x: x.ne(x.shift(1)))
    merged["transition_changed"] = merged.transition_incumbent.ne(merged.transition_challenger)
    return merged


def build_ma12_feature_cache(source_metrics: pd.DataFrame, audit: pd.DataFrame) -> Mapping[str, pd.DataFrame]:
    """Reconstruct each governed MA12 family once, then production-normalize it."""
    source = source_metrics[source_metrics.geo_id.isin(REVIEW_GEOGRAPHIES)].copy()
    source["date"] = pd.to_datetime(source["date"])
    cache: dict[str, pd.DataFrame] = {}
    price_metrics = {"median_sale_price", "median_ppsf", "price_to_income", "payment_burden"}
    # Linked affordability metrics require the complete upstream dependency
    # universe, including national mortgage inputs. Scope to review counties
    # only after linked feature construction.
    linked = build_linked_price_family_features(
        source_metrics,
        experiment_id="price_family_ma12_structural_linked",
    ).feature_history
    linked = linked[
        linked["geo_id"].isin(REVIEW_GEOGRAPHIES)
    ].copy()
    for metric in TARGET_METRICS:
        family = audit[audit.metric.eq(metric)].set_index("feature_type")
        key_map = family.feature_key.to_dict()
        if metric in price_metrics:
            raw = linked[linked.canonical_metric_key.eq(metric)][
                ["geo_id", "date", "canonical_metric_key", "feature_component", "raw_feature_value"]
            ].copy()
            raw["feature_key"] = raw.feature_component.map(key_map)
        else:
            observations = source[source.canonical_metric_key.eq(metric)].copy()
            if observations.empty:
                raise ValueError(f"MA12 reconstruction source is missing {metric}")
            policy = SmoothingMetricPolicy(
                experiment_id=f"{metric}_ma12_structural", metric_key=metric,
                policy_role="direct", transform_strategy="ma_structural",
                level_window=12, short_window=12, short_lag_periods=3,
                long_window=12, long_lag_periods=12, recompute_dependents=False,
            )
            wide = build_smoothed_metric_features_wide(observations, policy=policy, value_column="value")
            raw = wide.melt(
                id_vars=["geo_id", "date", "canonical_metric_key"],
                value_vars=["smoothed_level_value", "smoothed_short_value", "smoothed_long_value"],
                var_name="feature_component", value_name="raw_feature_value",
            )
            components = {"smoothed_level_value": "level", "smoothed_short_value": "short",
                          "smoothed_long_value": "long"}
            raw["feature_key"] = raw.feature_component.map(components).map(key_map)
        raw = raw[["geo_id", "date", "canonical_metric_key", "feature_key", "raw_feature_value"]].dropna()
        normalized = normalize_features(raw)
        normalized["feature_definition"] = "ma12_structural"
        cache[metric] = normalized.sort_values(["geo_id", "date", "feature_key"], kind="mergesort").reset_index(drop=True)
        print(f"[feature-weight] MA12 cache miss/build {metric}: rows={len(cache[metric])}")
    return cache


def run_authoritative_experiment(inputs: AuthoritativeInputs) -> tuple[object, Mapping[str, pd.DataFrame]]:
    frames = inputs.frames
    registry = pd.read_csv("config/feature_registry.csv")
    metric_registry = pd.read_csv("config/metric_dimension_registry.csv")
    source_registry = pd.read_csv("config/source_metric_registry.csv")
    axis_registry = pd.read_csv("config/axis_registry.csv")
    audit = audit_feature_registry(registry, metric_registry, source_registry)
    incumbent = frames["normalized_features"][
        frames["normalized_features"].geo_id.isin(REVIEW_GEOGRAPHIES)
        & frames["normalized_features"].feature_key.isin(audit.feature_key)
    ].assign(feature_definition="incumbent")
    cache = build_ma12_feature_cache(frames["source_metrics"], audit)
    governed = pd.concat([incumbent, *cache.values()], ignore_index=True)
    evidence = build_evidence(governed, registry, metric_registry, source_registry)
    policy_rows = evidence.tables["policy_registry"]
    dimension_rows=[]; axis_rows=[]; coordinate_rows=[]; regime_rows=[]; parity_rows=[]
    for metric in TARGET_METRICS:
        dimension = metric_registry.loc[metric_registry.canonical_metric_key.eq(metric), "dimension"].iloc[0]
        target_features_by_definition = {
            "incumbent": frames["normalized_features"][frames["normalized_features"].canonical_metric_key.eq(metric)],
            "ma12_structural": cache[metric],
        }
        family = audit[audit.metric.eq(metric)].set_index("feature_key")
        for policy in POLICY_ORDER[1:]:
            started=time.perf_counter(); p=policy_rows[(policy_rows.metric.eq(metric)) & (policy_rows.policy.eq(policy))].iloc[0]
            weights={row.Index: float(getattr(p, f"{row.feature_type}_weight")) for row in family.itertuples()}
            work=target_features_by_definition[p.feature_definition].copy(); work["feature_weight"]=work.feature_key.map(weights)
            available=work.feature_score.notna(); totals=work.feature_weight.where(available,0).groupby([work.geo_id,work.date]).transform("sum")
            work["contribution"]=work.feature_score*work.feature_weight.div(totals).where(available)
            scored=work.groupby(["geo_id","date","canonical_metric_key"],as_index=False).agg(
                metric_score=("contribution","sum"), feature_count=("feature_score","count"),
                feature_weight_sum=("feature_weight","sum"), min_feature_score=("feature_score","min"), max_feature_score=("feature_score","max"))
            mixed_metrics = _splice(
                frames["metric_scores"],
                scored,
                "canonical_metric_key",
                metric,
                geo_ids=REVIEW_GEOGRAPHIES,
            )
            aligned_target = align_metric_scores_asof(mixed_metrics)
            aligned_target = aligned_target[
                aligned_target["canonical_metric_key"].eq(metric)
                & aligned_target["geo_id"].isin(REVIEW_GEOGRAPHIES)
            ].copy()
            mixed_aligned = _splice(
                frames["aligned_metric_scores"],
                aligned_target,
                "canonical_metric_key",
                metric,
                geo_ids=REVIEW_GEOGRAPHIES,
            )
            dimension_metrics=set(metric_registry.loc[metric_registry.dimension.eq(dimension),"canonical_metric_key"])
            rebuilt_dimension=score_dimensions(mixed_aligned[mixed_aligned.canonical_metric_key.isin(dimension_metrics)])
            target_dimension=rebuilt_dimension[rebuilt_dimension.dimension.eq(dimension)]
            mixed_dimensions = _splice(
                frames["dimension_scores"],
                target_dimension[
                    target_dimension["geo_id"].isin(REVIEW_GEOGRAPHIES)
                ],
                "dimension",
                dimension,
                geo_ids=REVIEW_GEOGRAPHIES,
            )
            impacted_axes=tuple(axis_registry.loc[axis_registry.dimension.eq(dimension),"axis"].drop_duplicates())
            axis_dimensions=set(axis_registry.loc[axis_registry.axis.isin(impacted_axes),"dimension"])
            rebuilt_axes=score_axes(mixed_dimensions[mixed_dimensions.dimension.isin(axis_dimensions)])
            affected_axes=frames["axis_scores"].copy()
            for axis in impacted_axes:
                affected_axes = _splice(
                    affected_axes,
                    rebuilt_axes[
                        rebuilt_axes["axis"].eq(axis)
                        & rebuilt_axes["geo_id"].isin(REVIEW_GEOGRAPHIES)
                    ],
                    "axis",
                    axis,
                    geo_ids=REVIEW_GEOGRAPHIES,
                )
            coordinates=build_coordinates(affected_axes); geometry=assign_geometry(coordinates); regimes=assign_regimes(geometry)
            tag={"metric":metric,"policy":policy}
            incumbent_dimension = frames["dimension_scores"][
                frames["dimension_scores"].dimension.eq(dimension)
                & frames["dimension_scores"]["geo_id"].isin(REVIEW_GEOGRAPHIES)
            ]
            challenger_dimension = mixed_dimensions[
                mixed_dimensions.dimension.eq(dimension)
                & mixed_dimensions["geo_id"].isin(REVIEW_GEOGRAPHIES)
            ]
            dimension_rows.append(
                summarize_propagation(
                    incumbent_dimension,
                    challenger_dimension,
                    ["geo_id", "date", "dimension"],
                    "dimension_score",
                    artifact="dimension",
                ).assign(**tag)
            )

            incumbent_axes = frames["axis_scores"][
                frames["axis_scores"].axis.isin(impacted_axes)
                & frames["axis_scores"]["geo_id"].isin(REVIEW_GEOGRAPHIES)
            ]
            challenger_axes = affected_axes[
                affected_axes.axis.isin(impacted_axes)
                & affected_axes["geo_id"].isin(REVIEW_GEOGRAPHIES)
            ]
            axis_rows.append(
                summarize_propagation(
                    incumbent_axes,
                    challenger_axes,
                    ["geo_id", "date", "axis"],
                    "axis_score",
                    artifact="axis",
                ).assign(**tag)
            )

            incumbent_coordinates = frames["coordinates"][
                frames["coordinates"]["geo_id"].isin(REVIEW_GEOGRAPHIES)
            ]
            challenger_coordinates = coordinates[
                coordinates["geo_id"].isin(REVIEW_GEOGRAPHIES)
            ]
            coordinate_rows.append(
                summarize_propagation(
                    incumbent_coordinates,
                    challenger_coordinates,
                    ["geo_id", "date"],
                    "radius",
                    artifact="coordinates",
                ).assign(**tag)
            )

            incumbent_regimes = frames["regime_assignments"][
                frames["regime_assignments"]["geo_id"].isin(REVIEW_GEOGRAPHIES)
            ]
            challenger_regimes = regimes[
                regimes["geo_id"].isin(REVIEW_GEOGRAPHIES)
            ]
            regime_rows.append(
                _regime_changes(
                    incumbent_regimes,
                    challenger_regimes,
                    metric,
                    policy,
                )
            )
            unaffected_inc=frames["metric_scores"][~frames["metric_scores"].canonical_metric_key.eq(metric)]
            unaffected_ch=mixed_metrics[~mixed_metrics.canonical_metric_key.eq(metric)]
            parity_rows.append(validate_unaffected_parity(unaffected_inc,unaffected_ch,["geo_id","date","canonical_metric_key"],artifact=f"{metric}__{policy}__sibling_metrics").assign(**tag))
            unaffected_aligned_inc=frames["aligned_metric_scores"][~frames["aligned_metric_scores"].canonical_metric_key.eq(metric)]
            unaffected_aligned_ch=mixed_aligned[~mixed_aligned.canonical_metric_key.eq(metric)]
            parity_rows.append(validate_unaffected_parity(unaffected_aligned_inc,unaffected_aligned_ch,
                ["geo_id","evaluation_date","canonical_metric_key"],artifact=f"{metric}__{policy}__aligned_sibling_metrics").assign(**tag))
            unrelated_dimensions_inc=frames["dimension_scores"][~frames["dimension_scores"].dimension.eq(dimension)]
            unrelated_dimensions_ch=mixed_dimensions[~mixed_dimensions.dimension.eq(dimension)]
            parity_rows.append(validate_unaffected_parity(unrelated_dimensions_inc,unrelated_dimensions_ch,
                ["geo_id","date","dimension"],artifact=f"{metric}__{policy}__unrelated_dimensions").assign(**tag))
            supporting_axes_inc=frames["axis_scores"][~frames["axis_scores"].axis.isin(impacted_axes)]
            supporting_axes_ch=affected_axes[~affected_axes.axis.isin(impacted_axes)]
            parity_rows.append(validate_unaffected_parity(supporting_axes_inc,supporting_axes_ch,
                ["geo_id","date","axis"],artifact=f"{metric}__{policy}__supporting_axes").assign(**tag))
            print(f"[feature-weight] challenger {metric}/{policy}: {time.perf_counter()-started:.3f}s")
    extra={"downstream_dimension_propagation":pd.concat(dimension_rows,ignore_index=True),
           "downstream_axis_propagation":pd.concat(axis_rows,ignore_index=True),
           "coordinate_regime_changes":pd.concat(regime_rows,ignore_index=True),
           "coordinate_propagation_summary":pd.concat(coordinate_rows,ignore_index=True),
           "unaffected_parity":pd.concat(parity_rows,ignore_index=True),
           "ma12_rebuilt_features":pd.concat([
               value[["geo_id","date","canonical_metric_key","feature_key","raw_feature_value"]]
               for value in cache.values()], ignore_index=True),
           "ma12_normalized_feature_scores":pd.concat([
               value[["geo_id","date","canonical_metric_key","feature_key","feature_score",
                      "percentile","normalization_method","score_direction"]]
               for value in cache.values()], ignore_index=True),
           "ma12_cache_behavior":pd.DataFrame([
               {"metric":metric,"cache_build_count":1,"policy_consumers":4,"cache_reuse_count":3,
                "feature_definition":"ma12_structural"} for metric in TARGET_METRICS])}
    return evidence, extra


def main(argv: Sequence[str] | None = None) -> None:
    args=parse_args(argv); total=time.perf_counter()
    print(f"[feature-weight] input_run_directory={args.run_directory}")
    print(f"[feature-weight] output_directory={args.output_directory}")
    started=time.perf_counter(); inputs=load_authoritative_inputs(args.run_directory)
    print(f"[feature-weight] authoritative input loading/readiness/identity validation: {time.perf_counter()-started:.3f}s")
    started=time.perf_counter(); evidence, extra=run_authoritative_experiment(inputs)
    print(f"[feature-weight] challenger construction, unaffected-parity validation, and diagnostic calculations: {time.perf_counter()-started:.3f}s")
    started=time.perf_counter(); review,archive,count=write_review_bundle(evidence,args.output_directory,extra,
        {"authoritative_input_identity":inputs.identity,"metric_count":7,"policy_record_count":35,"challenger_count":28,
         "ma12_cache_builds":7,"ma12_cache_reuses":21,"excluded_windows":["MA6","MA9"],
         "recommendation_state":"none","promotion_state":"none"})
    print(f"[feature-weight] HTML assembly + ZIP creation: {time.perf_counter()-started:.3f}s")
    print(f"[feature-weight] review={review} zip={archive} files={count} zip_bytes={archive.stat().st_size}")
    print(f"[feature-weight] metric_count=7 policy_records=35 challenger_count=28 cache_builds=7 cache_reuses=21 excluded=MA6,MA9 contract_identity={CONTRACT_VERSION} recommendation_state=none promotion_state=none")
    print(f"[feature-weight] total runtime: {time.perf_counter()-total:.3f}s")


if __name__ == "__main__": main()
