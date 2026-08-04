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

from regime._04_asof_aligner import align_metric_scores_asof
from regime._05_dimension_scorer import score_dimensions
from regime._06_axis_engine import score_axes
from regime._07_coordinate_engine import build_coordinates
from regime._08_geometry_engine import assign_geometry
from regime._09_regime_assignment import assign_regimes
from regime.artifacts import RegimeArtifactStore
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


def _splice(parent: pd.DataFrame, replacement: pd.DataFrame, column: str, value: str) -> pd.DataFrame:
    return pd.concat([parent[~parent[column].eq(value)], replacement], ignore_index=True).sort_values(
        [c for c in ("geo_id", "date", "evaluation_date", column) if c in parent.columns], kind="mergesort").reset_index(drop=True)


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


def run_authoritative_experiment(inputs: AuthoritativeInputs) -> tuple[object, Mapping[str, pd.DataFrame]]:
    frames = inputs.frames
    registry = pd.read_csv("config/feature_registry.csv")
    metric_registry = pd.read_csv("config/metric_dimension_registry.csv")
    source_registry = pd.read_csv("config/source_metric_registry.csv")
    axis_registry = pd.read_csv("config/axis_registry.csv")
    audit = audit_feature_registry(registry, metric_registry, source_registry)
    governed = frames["normalized_features"][
        frames["normalized_features"].geo_id.isin(REVIEW_GEOGRAPHIES)
        & frames["normalized_features"].feature_key.isin(audit.feature_key)
    ]
    evidence = build_evidence(governed, registry, metric_registry, source_registry)
    policy_rows = evidence.tables["policy_registry"]
    dimension_rows=[]; axis_rows=[]; coordinate_rows=[]; regime_rows=[]; parity_rows=[]
    for metric in TARGET_METRICS:
        dimension = metric_registry.loc[metric_registry.canonical_metric_key.eq(metric), "dimension"].iloc[0]
        target_features = frames["normalized_features"][frames["normalized_features"].canonical_metric_key.eq(metric)]
        family = audit[audit.metric.eq(metric)].set_index("feature_key")
        for policy in POLICY_ORDER[1:]:
            started=time.perf_counter(); p=policy_rows[(policy_rows.metric.eq(metric)) & (policy_rows.policy.eq(policy))].iloc[0]
            weights={row.Index: float(getattr(p, f"{row.feature_type}_weight")) for row in family.itertuples()}
            work=target_features.copy(); work["feature_weight"]=work.feature_key.map(weights)
            available=work.feature_score.notna(); totals=work.feature_weight.where(available,0).groupby([work.geo_id,work.date]).transform("sum")
            work["contribution"]=work.feature_score*work.feature_weight.div(totals).where(available)
            scored=work.groupby(["geo_id","date","canonical_metric_key"],as_index=False).agg(
                metric_score=("contribution","sum"), feature_count=("feature_score","count"),
                feature_weight_sum=("feature_weight","sum"), min_feature_score=("feature_score","min"), max_feature_score=("feature_score","max"))
            mixed_metrics=_splice(frames["metric_scores"],scored,"canonical_metric_key",metric)
            aligned_target=align_metric_scores_asof(mixed_metrics)
            aligned_target=aligned_target[aligned_target.canonical_metric_key.eq(metric)]
            mixed_aligned=_splice(frames["aligned_metric_scores"],aligned_target,"canonical_metric_key",metric)
            dimension_metrics=set(metric_registry.loc[metric_registry.dimension.eq(dimension),"canonical_metric_key"])
            rebuilt_dimension=score_dimensions(mixed_aligned[mixed_aligned.canonical_metric_key.isin(dimension_metrics)])
            target_dimension=rebuilt_dimension[rebuilt_dimension.dimension.eq(dimension)]
            mixed_dimensions=_splice(frames["dimension_scores"],target_dimension,"dimension",dimension)
            impacted_axes=tuple(axis_registry.loc[axis_registry.dimension.eq(dimension),"axis"].drop_duplicates())
            axis_dimensions=set(axis_registry.loc[axis_registry.axis.isin(impacted_axes),"dimension"])
            rebuilt_axes=score_axes(mixed_dimensions[mixed_dimensions.dimension.isin(axis_dimensions)])
            affected_axes=frames["axis_scores"].copy()
            for axis in impacted_axes:
                affected_axes=_splice(affected_axes,rebuilt_axes[rebuilt_axes.axis.eq(axis)],"axis",axis)
            coordinates=build_coordinates(affected_axes); geometry=assign_geometry(coordinates); regimes=assign_regimes(geometry)
            tag={"metric":metric,"policy":policy}
            dimension_rows.append(summarize_propagation(frames["dimension_scores"],mixed_dimensions,["geo_id","date","dimension"],"dimension_score",artifact="dimension").assign(**tag))
            axis_rows.append(summarize_propagation(frames["axis_scores"],affected_axes,["geo_id","date","axis"],"axis_score",artifact="axis").assign(**tag))
            coordinate_rows.append(summarize_propagation(frames["coordinates"],coordinates,["geo_id","date"],"radius",artifact="coordinates").assign(**tag))
            regime_rows.append(_regime_changes(frames["regime_assignments"],regimes,metric,policy))
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
           "unaffected_parity":pd.concat(parity_rows,ignore_index=True)}
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
        {"authoritative_input_identity":inputs.identity,"metric_count":7,"challenger_count":21,
         "recommendation_state":"none","promotion_state":"none"})
    print(f"[feature-weight] HTML assembly + ZIP creation: {time.perf_counter()-started:.3f}s")
    print(f"[feature-weight] review={review} zip={archive} files={count} zip_bytes={archive.stat().st_size}")
    print(f"[feature-weight] metric_count=7 challenger_count=21 contract_identity={CONTRACT_VERSION} recommendation_state=none promotion_state=none")
    print(f"[feature-weight] total runtime: {time.perf_counter()-total:.3f}s")


if __name__ == "__main__": main()
