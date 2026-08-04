"""Authoritative, policy-neutral Capital Markets diagnostic evidence.

The module only decomposes persisted engine outputs.  It deliberately imports the
production registry builders so that a diagnostic cannot acquire its own scoring
policy.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping
import hashlib
import html
import json
import time
import zipfile

import numpy as np
import pandas as pd

from regime._00_config_loader import load_regime_config
from regime._05_dimension_scorer import _build_dimension_weights
from regime._06_axis_engine import _build_axis_weights
from regime.review.calibration.engine_decomposition import (
    RECONCILIATION_TOLERANCE,
    build_dimension_to_axis,
    build_feature_to_metric,
    build_metric_to_dimension,
)

CONTRACT_VERSION = "capital_markets_diagnostic_v1"
DIMENSION = "capital_markets"
REVIEW_GEOGRAPHIES = (
    "district_of_columbia_dc__county", "essex_county_nj__county",
    "montgomery_county_md__county", "prince_george_s_county_md__county",
    "fairfax_county_va__county", "san_francisco_county_ca__county",
    "los_angeles_county_ca__county",
)
TABLE_NAMES = (
    "registry_audit", "feature_to_metric_decomposition",
    "metric_to_dimension_decomposition", "volatility_attribution",
    "sign_flip_attribution", "largest_jump_windows", "coverage_missingness",
    "effective_weight_history", "cancellation", "provenance_reconstruction",
    "supply_axis_propagation", "demand_axis_propagation",
)
RECONSTRUCTION_STATUSES = frozenset({"reconciled", "not_applicable", "not_reconcilable", "failed"})


@dataclass(frozen=True, slots=True)
class CapitalMarketsEvidence:
    contract_version: str
    review_geographies: tuple[str, ...]
    tables: Mapping[str, pd.DataFrame]

    def copied_tables(self) -> dict[str, pd.DataFrame]:
        return {name: frame.copy(deep=True) for name, frame in self.tables.items()}


def _truthy(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})


def build_registry_audit() -> pd.DataFrame:
    """Resolve feature -> canonical metric -> dimension -> both axes, fail closed."""
    config = load_regime_config(validate=True)
    md = config.metric_dimensions.copy()
    capital = md[md["dimension"].eq(DIMENSION)].copy()
    if capital.empty:
        raise ValueError("Authoritative registry has no capital_markets ownership")
    sources = config.source_metrics.rename(columns={"geo_levels": "source_geography"})
    feature = config.features.merge(
        capital, on="metric_key", how="inner", validate="many_to_one", suffixes=("_feature", "_metric")
    ).merge(
        sources[["metric_key", "source_id", "metric_id", "source_geography", "frequency", "seasonality"]],
        on="metric_key", how="left", validate="many_to_one",
    )
    if feature.empty or feature[["canonical_metric_key", "source_id", "source_geography"]].isna().any().any():
        raise ValueError("Capital Markets registry ownership or source lineage is incomplete")
    axes = config.axes[config.axes["dimension"].eq(DIMENSION)].copy()
    axis_weights = dict(zip(axes["axis"], pd.to_numeric(axes["dimension_weight"], errors="raise")))
    if set(axis_weights) != {"supply", "demand"}:
        raise ValueError("Capital Markets must have unambiguous Supply and Demand axis ownership")
    feature["configured_feature_weight"] = pd.to_numeric(feature["feature_weight"], errors="raise")
    feature["configured_metric_to_dimension_weight"] = pd.to_numeric(feature["metric_weight"], errors="raise")
    feature["supply_axis_configured_weight"] = axis_weights["supply"]
    feature["demand_axis_configured_weight"] = axis_weights["demand"]
    feature["source_lineage"] = feature["source_id"] + ":" + feature["metric_id"]
    feature["enabled"] = _truthy(feature["enabled"])
    feature["diagnostic_only"] = _truthy(feature["diagnostic_only"])
    feature["macro_enabled"] = _truthy(feature["macro_enabled"])
    feature["missing_value_behavior"] = "drop unavailable children; renormalize available configured weight"
    feature["clipping_behavior"] = "production metric and dimension parents clip to [-1, 1]"
    feature["alignment_freshness"] = "production as-of alignment; persisted source/evaluation dates and age govern eligibility"
    columns = ["feature_key", "feature_type", "transform", "feature_window", "metric_key",
        "canonical_metric_key", "dimension", "configured_feature_weight",
        "configured_metric_to_dimension_weight", "supply_axis_configured_weight",
        "demand_axis_configured_weight", "enabled", "diagnostic_only", "macro_enabled",
        "source_geography", "source_lineage", "frequency", "seasonality",
        "alignment_freshness", "missing_value_behavior", "clipping_behavior"]
    return feature[columns].sort_values(["canonical_metric_key", "feature_key"], kind="mergesort").reset_index(drop=True)


def _add_lineage(decomp: pd.DataFrame, source: pd.DataFrame) -> pd.DataFrame:
    out = decomp.copy()
    source = source.copy()
    if "evaluation_date" in source:
        source["date"] = source["evaluation_date"]
    keys = [key for key in ("geo_id", "date", "canonical_metric_key", "feature_key") if key in out and key in source]
    optional = [c for c in ("source_date", "evaluation_date", "metric_age_days", "feature_age_days") if c in source]
    if keys and optional:
        out = out.merge(source[keys + optional].drop_duplicates(keys), on=keys, how="left", validate="many_to_one")
    if "evaluation_date" not in out:
        out["evaluation_date"] = out["date"]
    if "source_date" not in out:
        out["source_date"] = pd.NaT
    age = "metric_age_days" if "metric_age_days" in out else "feature_age_days" if "feature_age_days" in out else None
    out["age_days"] = out[age] if age else pd.NA
    return out


def _volatility(rows: pd.DataFrame, identity: str, contribution: str, parent: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    work = rows.copy().sort_values(["grain", "geo_id", identity, "date"], kind="mergesort")
    work["movement"] = work.groupby(["grain", "geo_id", identity], dropna=False)[contribution].diff()
    work["parent_movement"] = work.groupby(["grain", "geo_id"], dropna=False)["parent_score"].diff()
    work["sign_flip"] = work.groupby(["grain", "geo_id", identity], dropna=False)[contribution].transform(
        lambda s: s.mul(s.shift()).lt(0))
    work["absolute_movement"] = work["movement"].abs()
    total = work.groupby(["grain", "geo_id"])["absolute_movement"].transform("sum").replace(0, np.nan)
    work["absolute_movement_share"] = work["absolute_movement"] / total
    group = ["grain", "geo_id", identity]
    stats = work.groupby(group, dropna=False).agg(
        standard_deviation=(contribution, "std"),
        median_absolute_mom_change=("absolute_movement", "median"),
        p90_absolute_mom_change=("absolute_movement", lambda s: s.quantile(.90)),
        p99_absolute_mom_change=("absolute_movement", lambda s: s.quantile(.99)),
        sign_flip_count=("sign_flip", "sum"), observation_count=(contribution, "count"),
        contribution_to_total_absolute_movement=("absolute_movement_share", "sum"),
    ).reset_index()
    stats["sign_flip_rate"] = stats["sign_flip_count"] / (stats["observation_count"] - 1).clip(lower=1)
    flips = work[work["sign_flip"]][group + ["date", contribution, "movement", "parent_movement"]].copy()
    threshold = work.groupby(["grain", "geo_id"])["parent_movement"].transform(lambda s: s.abs().quantile(.90))
    jumps = work[work["parent_movement"].abs().ge(threshold) & work["parent_movement"].notna()][
        group + ["date", contribution, "movement", "parent_movement", "absolute_movement_share"]].copy()
    return stats, flips, jumps


def _cancellation(m2d: pd.DataFrame) -> pd.DataFrame:
    work = m2d.sort_values(["grain", "geo_id", "canonical_metric_key", "date"]).copy()
    work["contribution_movement"] = work.groupby(["grain", "geo_id", "canonical_metric_key"])["weighted_contribution"].diff()
    records = []
    for keys, group in work.groupby(["grain", "geo_id", "date"], sort=True):
        values = group.dropna(subset=["contribution_movement"])
        signed = values["contribution_movement"].sum(); gross = values["contribution_movement"].abs().sum()
        pos = sorted(values.loc[values.contribution_movement.gt(0), "canonical_metric_key"])
        neg = sorted(values.loc[values.contribution_movement.lt(0), "canonical_metric_key"])
        records.append({"grain": keys[0], "geo_id": keys[1], "date": keys[2],
            "signed_contribution_sum": signed, "gross_absolute_contribution_sum": gross,
            "cancellation_ratio": 0.0 if gross == 0 else 1.0 - abs(signed) / gross,
            "opposite_direction_pairs": "|".join(f"{a}::{b}" for a in pos for b in neg),
            "stable_due_to_cancellation": bool(gross > 0 and abs(signed) <= .25 * gross)})
    return pd.DataFrame(records)


def _axis_table(dimensions: pd.DataFrame, axes: pd.DataFrame, axis: str, grain: str) -> pd.DataFrame:
    rows, _ = build_dimension_to_axis(dimensions, axes)
    rows = rows[rows["axis"].eq(axis)].copy()
    rows["grain"] = grain
    rows["capital_markets_contribution"] = rows["weighted_contribution"].where(rows["dimension"].eq(DIMENSION))
    rows["other_dimension_contribution"] = rows["weighted_contribution"].where(~rows["dimension"].eq(DIMENSION))
    rows["capital_markets_effective_weight_one"] = rows["dimension"].eq(DIMENSION) & rows["effective_weight"].eq(1.0)
    return rows


def build_capital_markets_evidence(*, normalized_features: pd.DataFrame, metric_scores: pd.DataFrame,
        aligned_metric_scores: pd.DataFrame, dimension_scores: pd.DataFrame, axis_scores: pd.DataFrame,
        native_geo_ids: tuple[str, ...], review_geographies: tuple[str, ...] = REVIEW_GEOGRAPHIES) -> CapitalMarketsEvidence:
    """Build evidence from persisted inputs without changing any score or policy."""
    overall_started = time.perf_counter()
    step = time.perf_counter(); audit = build_registry_audit()
    print(f"[capital-markets] registry audit: {time.perf_counter()-step:.3f}s")
    canonical = set(audit.loc[audit.enabled & ~audit.diagnostic_only & audit.macro_enabled, "canonical_metric_key"])
    if not canonical:
        raise ValueError("No authoritative Capital Markets canonical metrics")
    all_geos = set(native_geo_ids) | set(review_geographies)
    grain_of = {geo: "native_source" if geo in native_geo_ids else "county_aligned" for geo in all_geos}
    scoped_dims = dimension_scores[dimension_scores.geo_id.isin(all_geos)].copy()
    capital_parent = scoped_dims[scoped_dims.dimension.eq(DIMENSION)]
    if capital_parent.empty:
        raise ValueError("No persisted Capital Markets parent evidence in governed scope")
    step = time.perf_counter()
    f2m, _ = build_feature_to_metric(
        normalized_features[normalized_features.geo_id.isin(all_geos) & normalized_features.canonical_metric_key.isin(canonical)],
        metric_scores[metric_scores.geo_id.isin(all_geos) & metric_scores.canonical_metric_key.isin(canonical)])
    f2m = _add_lineage(f2m, normalized_features); f2m["grain"] = f2m.geo_id.map(grain_of)
    m2d, _ = build_metric_to_dimension(
        aligned_metric_scores[aligned_metric_scores.geo_id.isin(all_geos)], scoped_dims)
    m2d = m2d[m2d.dimension.eq(DIMENSION)].copy(); m2d["grain"] = m2d.geo_id.map(grain_of)
    print(f"[capital-markets] decomposition construction: {time.perf_counter()-step:.3f}s")
    m2d["metric_age"] = m2d.get("metric_age_days", pd.NA)
    effective = m2d[["grain", "geo_id", "date", "canonical_metric_key", "configured_weight",
        "effective_weight", "available_child_count", "available_weight_sum", "available"]].copy()
    # Dimension parents do not exist on zero-child dates.  Retain those governed
    # evaluation dates explicitly rather than making missingness disappear.
    weights = _build_dimension_weights().query("dimension == @DIMENSION")[["canonical_metric_key", "metric_weight"]]
    evaluation = scoped_dims[["geo_id", "date"]].drop_duplicates()
    grid = evaluation.merge(pd.DataFrame({"canonical_metric_key": sorted(canonical)}), how="cross").merge(
        weights, on="canonical_metric_key", how="left", validate="many_to_one")
    missing_grid = grid.merge(
        effective[["geo_id", "date", "canonical_metric_key"]],
        on=["geo_id", "date", "canonical_metric_key"], how="left", indicator=True,
    ).query("_merge == 'left_only'").drop(columns="_merge")
    if not missing_grid.empty:
        missing_grid = missing_grid.rename(columns={"metric_weight": "configured_weight"})
        missing_grid["grain"] = missing_grid.geo_id.map(grain_of)
        missing_grid["effective_weight"] = np.nan; missing_grid["available_child_count"] = 0
        missing_grid["available_weight_sum"] = 0.0; missing_grid["available"] = False
        effective = pd.concat([effective, missing_grid[effective.columns]], ignore_index=True)
    effective["one_child_period"] = effective.available_child_count.eq(1)
    effective["zero_child_period"] = effective.available_child_count.eq(0)
    effective["effective_weight_one"] = effective.effective_weight.eq(1.0)
    effective["material_weight_shift"] = (effective.effective_weight - effective.configured_weight).abs().ge(.05)
    coverage = effective.groupby(["grain", "geo_id", "canonical_metric_key"], dropna=False).agg(
        first_valid_metric_date=("date", lambda s: s[effective.loc[s.index, "available"]].min()),
        first_valid_capital_markets_date=("date", lambda s: s[effective.loc[s.index, "available_child_count"].gt(0)].min()), one_child_periods=("one_child_period", "sum"),
        zero_child_periods=("zero_child_period", "sum"), effective_weight_one_periods=("effective_weight_one", "sum"),
        material_weight_shift_periods=("material_weight_shift", "sum")).reset_index()
    source_dates = f2m.groupby(["grain", "geo_id", "canonical_metric_key"]).agg(
        first_valid_source_date=("source_date", "min"), first_valid_feature_date=("date", "min")).reset_index()
    coverage = coverage.merge(source_dates, on=["grain", "geo_id", "canonical_metric_key"], how="left")
    step = time.perf_counter()
    v_metric, flips, jumps = _volatility(m2d, "canonical_metric_key", "weighted_contribution", capital_parent)
    feature_for_vol = f2m.rename(columns={"metric_score": "parent_score"})
    v_feature, feature_flips, feature_jumps = _volatility(feature_for_vol, "feature_key", "weighted_contribution", metric_scores)
    v_metric["level"] = "metric"; v_feature["level"] = "feature"
    flips["level"] = "metric"; feature_flips["level"] = "feature"
    jumps["level"] = "metric"; feature_jumps["level"] = "feature"
    print(f"[capital-markets] volatility attribution: {time.perf_counter()-step:.3f}s")
    step = time.perf_counter()
    provenance_cols = ["grain", "geo_id", "date", "parent_score", "summed_contributions", "absolute_residual", "reconciliation_status", "reason_code"]
    provenance = m2d[provenance_cols].drop_duplicates(["grain", "geo_id", "date"])
    provenance["producing_configuration_may_differ"] = provenance.reconciliation_status.eq("failed")
    if not set(provenance.reconciliation_status).issubset(RECONSTRUCTION_STATUSES):
        raise ValueError("Uncontrolled provenance reconstruction status")
    print(f"[capital-markets] provenance reconciliation: {time.perf_counter()-step:.3f}s")
    supply = _axis_table(scoped_dims, axis_scores[axis_scores.geo_id.isin(all_geos)], "supply", "mixed")
    demand = _axis_table(scoped_dims, axis_scores[axis_scores.geo_id.isin(all_geos)], "demand", "mixed")
    tables = {
        "registry_audit": audit, "feature_to_metric_decomposition": f2m,
        "metric_to_dimension_decomposition": m2d,
        "volatility_attribution": pd.concat([v_metric, v_feature], ignore_index=True, sort=False),
        "sign_flip_attribution": pd.concat([flips, feature_flips], ignore_index=True, sort=False),
        "largest_jump_windows": pd.concat([jumps, feature_jumps], ignore_index=True, sort=False),
        "coverage_missingness": coverage, "effective_weight_history": effective,
        "cancellation": _cancellation(m2d), "provenance_reconstruction": provenance,
        "supply_axis_propagation": supply, "demand_axis_propagation": demand,
    }
    print(f"[capital-markets] focused diagnostic evidence generation: {time.perf_counter()-overall_started:.3f}s")
    return CapitalMarketsEvidence(CONTRACT_VERSION, tuple(review_geographies), MappingProxyType(tables))


def write_review_bundle(evidence: CapitalMarketsEvidence, output: Path) -> tuple[Path, Path, int]:
    """Write deterministic CSV/HTML/ZIP review artifacts (no production promotion)."""
    started = time.perf_counter(); output.mkdir(parents=True, exist_ok=True)
    csv_dir = output / "evidence"; csv_dir.mkdir(exist_ok=True)
    links = []
    for name in TABLE_NAMES:
        path = csv_dir / f"{name}.csv"
        evidence.tables[name].to_csv(path, index=False, lineterminator="\n", date_format="%Y-%m-%d")
        links.append(f'<li><a href="evidence/{path.name}">{html.escape(name)}</a></li>')
    step = time.perf_counter(); figure_dir = output / "figures"; figure_dir.mkdir(exist_ok=True)
    figure_specs = (
        ("capital_markets_chronology", "metric_to_dimension_decomposition", "parent_score"),
        ("metric_weighted_contributions", "metric_to_dimension_decomposition", "weighted_contribution"),
        ("feature_weighted_contributions", "feature_to_metric_decomposition", "weighted_contribution"),
        ("effective_metric_weights", "effective_weight_history", "effective_weight"),
        ("largest_jump_decomposition", "largest_jump_windows", "movement"),
        ("cancellation_ratio", "cancellation", "cancellation_ratio"),
        ("supply_axis_capital_markets", "supply_axis_propagation", "capital_markets_contribution"),
        ("demand_axis_capital_markets", "demand_axis_propagation", "capital_markets_contribution"),
        ("persisted_vs_reconstructed", "provenance_reconstruction", "summed_contributions"),
    )
    figures = []
    for figure_name, table_name, value_name in figure_specs:
        frame = evidence.tables[table_name]
        values = pd.to_numeric(frame.get(value_name, pd.Series(dtype=float)), errors="coerce").dropna()
        dates = pd.to_datetime(frame.loc[values.index, "date"], errors="coerce") if "date" in frame else pd.Series(dtype="datetime64[ns]")
        points = []
        if len(values):
            low, high = min(0.0, values.min()), max(0.0, values.max()); span = high - low or 1.0
            start, end = dates.min(), dates.max(); date_span = max((end - start).total_seconds(), 1)
            points = [(50 + (date-start).total_seconds()/date_span*700, 260-(value-low)/span*220)
                      for date, value in zip(dates, values) if pd.notna(date)]
        path_data = " ".join(("M" if i == 0 else "L") + f" {x:.2f} {y:.2f}" for i, (x, y) in enumerate(points))
        svg = ("<svg xmlns='http://www.w3.org/2000/svg' width='800' height='300' role='img'>"
               f"<title>{html.escape(figure_name.replace('_', ' '))}</title>"
               "<rect width='800' height='300' fill='white'/><line x1='50' y1='260' x2='750' y2='260' stroke='black'/>"
               "<line x1='50' y1='40' x2='50' y2='260' stroke='black'/>"
               f"<path d='{path_data}' fill='none' stroke='#4c78a8' stroke-width='2'/></svg>")
        figure = figure_dir / f"{figure_name}.svg"; figure.write_text(svg, encoding="utf-8", newline="\n")
        figures.append(f'<figure><img src="figures/{figure.name}" alt="{html.escape(figure_name)}"><figcaption>{html.escape(figure_name.replace("_", " "))}</figcaption></figure>')
    print(f"[capital-markets] figure generation: {time.perf_counter()-step:.3f}s")
    registry = evidence.tables["registry_audit"]
    metrics = ", ".join(sorted(registry.loc[registry.enabled & ~registry.diagnostic_only, "canonical_metric_key"].unique()))
    sections = ["Executive Summary", "Registry and Source Lineage", "Capital Markets Chronology",
        "Feature-to-Metric Decomposition", "Metric-to-Capital-Markets Decomposition", "Volatility Attribution",
        "Missingness and Effective-Weight History", "Cancellation Diagnostics", "Supply-Axis Propagation",
        "Demand-Axis Propagation", "Historical Reconstruction and Provenance", "Supporting Tables and Artifact Links",
        "Human Decision Status"]
    body = [f"<h2>{i}. {html.escape(title)}</h2>" for i, title in enumerate(sections, 1)]
    body[0] += f"<p>Diagnostic-only incumbent evidence. Authoritative metrics: {html.escape(metrics)}.</p>"
    body[2] += figures[0]
    body[3] += figures[2]
    body[4] += figures[1]
    body[5] += figures[4]
    body[6] += figures[3]
    body[7] += figures[5]
    body[8] += figures[6]
    body[9] += figures[7]
    body[10] += figures[8]
    body[-2] += "<ul>" + "".join(links) + "</ul>"
    body[-1] += "<p>Pending human review. No policy change and no production promotion.</p>"
    page = "<!doctype html><html><head><meta charset='utf-8'><title>Capital Markets Diagnostic</title>" \
        "<style>body{font-family:sans-serif;max-width:1100px;margin:auto}h2{border-bottom:1px solid #333}</style></head><body>" \
        + "".join(body) + "</body></html>"
    review = output / "index.html"; review.write_text(page, encoding="utf-8", newline="\n")
    manifest_entries = []
    for path in sorted(p for p in output.rglob("*") if p.is_file() and p.name not in {"manifest.json"}):
        manifest_entries.append({"path": path.relative_to(output).as_posix(), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    manifest = {"contract_version": evidence.contract_version, "promotion_state": "none", "files": manifest_entries}
    (output / "manifest.json").write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    zip_path = output.with_suffix(".zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(p for p in output.rglob("*") if p.is_file()):
            info = zipfile.ZipInfo(path.relative_to(output).as_posix(), (1980, 1, 1, 0, 0, 0)); info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())
    print(f"[capital-markets] HTML assembly + ZIP creation: {time.perf_counter()-started:.3f}s; files={len(manifest_entries)+1}")
    return review, zip_path, len(manifest_entries) + 1
