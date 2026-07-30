"""Deterministic, advisory-only rendering of supplied inventory evidence."""

from __future__ import annotations

import hashlib
import html
import json
import re
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd

from .campaign import CalibrationCampaign
from .inventory_campaign import FEATURE_COMPONENTS, PhaseAEvidence
from .inventory_candidate_scoring import (
    SCORING_CONTRACT_VERSION,
    InventoryCandidateScoringResult,
    InventoryScoringPolicy,
    load_inventory_scoring_policy,
)


BUNDLE_CONTRACT_VERSION = "inventory_review_bundle_v1"
GENERATOR_VERSION = "1.0"
EVIDENCE_TABLES = (
    "inventory_campaign_geography_scope",
    "inventory_candidate_feature_coverage",
    "inventory_candidate_calendar_month_behavior",
    "inventory_candidate_feature_statistics",
    "inventory_candidate_baseline_feature_comparison",
    "inventory_candidate_target_replacement",
    "inventory_candidate_non_target_parity",
)
FIGURE_DIRECTORIES = (
    "score_summary", "time_series_overlays", "transition_windows",
    "calendar_month_profiles", "volatility", "sign_flip", "trend_preservation",
)


@dataclass(frozen=True, slots=True)
class InventoryReviewBundleResult:
    bundle_directory: Path
    zip_path: Path
    manifest: dict[str, object]
    generated_files: tuple[Path, ...]


def _evidence_tables(evidence: PhaseAEvidence) -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}
    for result in evidence.evidence_results.values():
        for name, frame in result.tables.items():
            if name in tables:
                raise ValueError(f"Duplicate Phase A evidence table: {name}")
            tables[name] = frame.copy(deep=True)
    return tables


def _slug(value: object) -> str:
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(value)).strip("._")
    if not slug:
        raise ValueError(f"Value cannot form a safe artifact name: {value!r}")
    return slug


def _canvas(title: str, width: int = 1200, height: int = 600):
    from PIL import Image, ImageDraw
    image = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(image)
    draw.text((30, 18), title, fill="black")
    return image, draw


def _save_image(image, path: Path) -> None:
    image.save(path, format="PNG", optimize=False)


def _bars(path: Path, title: str, groups: list[str], series: list[tuple[str, list[float]]], *, maximum: float | None = None, stacked: bool = False) -> None:
    image, draw = _canvas(title)
    colors = ("#4c78a8", "#f58518", "#54a24b", "#e45756", "#b279a2")
    left, top, right, bottom = 90, 70, 1170, 510
    draw.line((left, top, left, bottom, right, bottom), fill="black", width=2)
    values = [value for _, row in series for value in row if pd.notna(value)]
    ceiling = maximum or max(values or [1.0]) * 1.1 or 1.0
    group_width = (right - left) / len(groups)
    for gi, group in enumerate(groups):
        draw.text((left + gi * group_width + 3, bottom + 12), group.replace("inventory_", ""), fill="black")
        running = 0.0
        for si, (label, row) in enumerate(series):
            value = float(row[gi]) if pd.notna(row[gi]) else 0.0
            if stacked:
                x0, x1 = left + gi * group_width + group_width * .2, left + (gi + 1) * group_width - group_width * .2
                y1 = bottom - running / ceiling * (bottom - top); running += value
                y0 = bottom - running / ceiling * (bottom - top)
            else:
                width = group_width * .75 / len(series)
                x0 = left + gi * group_width + group_width * .1 + si * width; x1 = x0 + width * .9
                y0, y1 = bottom - value / ceiling * (bottom - top), bottom
            draw.rectangle((x0, y0, x1, y1), fill=colors[si % len(colors)], outline="white")
    for si, (label, _) in enumerate(series):
        y = 45 + si * 18; draw.rectangle((900, y, 912, y + 12), fill=colors[si % len(colors)]); draw.text((918, y), label.replace("_", " "), fill="black")
    _save_image(image, path)


def _lines(path: Path, title: str, lines: list[tuple[str, list[object], list[float]]], ylabel: str) -> None:
    image, draw = _canvas(title)
    colors = ("black", "#4c78a8", "#f58518", "#54a24b", "#e45756")
    left, top, right, bottom = 90, 70, 1170, 510
    draw.line((left, top, left, bottom, right, bottom), fill="black", width=2); draw.text((5, 280), ylabel, fill="black")
    values = [float(v) for _, _, row in lines for v in row if pd.notna(v)]
    low, high = min(values), max(values); span = high - low or 1.0
    for index, (label, xs, ys) in enumerate(lines):
        points = []
        for pos, value in enumerate(ys):
            if pd.isna(value): continue
            x = left + pos / max(len(ys) - 1, 1) * (right - left); y = bottom - (float(value) - low) / span * (bottom - top)
            points.append((x, y))
        if len(points) > 1: draw.line(points, fill=colors[index % len(colors)], width=3 if index == 0 else 2)
        draw.text((900, 45 + index * 18), label, fill=colors[index % len(colors)])
    _save_image(image, path)

def _ordered(frame: pd.DataFrame, candidates: tuple[str, ...]) -> pd.DataFrame:
    work = frame.copy()
    if "candidate_policy_id" in work:
        work["candidate_policy_id"] = pd.Categorical(
            work["candidate_policy_id"], candidates, ordered=True
        )
    columns = [column for column in ("candidate_policy_id", "feature_component", "calendar_month") if column in work]
    return work.sort_values(columns, kind="mergesort").reset_index(drop=True) if columns else work


def _validate(
    campaign: CalibrationCampaign,
    evidence: PhaseAEvidence,
    scoring: InventoryCandidateScoringResult,
    policy: InventoryScoringPolicy,
    tables: Mapping[str, pd.DataFrame],
) -> None:
    if evidence.campaign.to_dict() != campaign.to_dict():
        raise ValueError("Campaign and Phase A evidence identity mismatch")
    candidates = campaign.candidate_policy_ids
    required = set(EVIDENCE_TABLES) | {
        "inventory_candidate_feature_series", "inventory_transition_review_windows"
    }
    missing = sorted(required.difference(tables))
    if missing:
        raise ValueError(f"Missing required Phase A review tables: {missing}")
    for name, frame in scoring.tables.items():
        if frame.empty:
            raise ValueError(f"Missing required scoring rows: {name}")
    for name in EVIDENCE_TABLES:
        frame = tables[name]
        if name == "inventory_campaign_geography_scope":
            included = frame[frame["included"].eq(True)]
            if included.empty or set(included["geo_level"]) != {"county"}:
                raise ValueError("Review evidence geography scope must include counties only")
            continue
        if "candidate_policy_id" not in frame:
            raise ValueError(f"{name} is missing candidate_policy_id")
        if set(frame["candidate_policy_id"].dropna()) != set(candidates):
            raise ValueError(f"{name} candidate identity mismatch")
    calendar = tables["inventory_candidate_calendar_month_behavior"]
    for candidate in candidates:
        for component in FEATURE_COMPONENTS:
            part = calendar[
                calendar["candidate_policy_id"].eq(candidate)
                & calendar["feature_component"].eq(component)
            ]
            months = pd.to_numeric(part.get("calendar_month"), errors="coerce")
            if len(part) != 12 or set(months) != set(range(1, 13)):
                raise ValueError(f"Invalid calendar months for {candidate}/{component}")
    for name in ("inventory_candidate_feature_statistics", "inventory_candidate_baseline_feature_comparison"):
        if tables[name].duplicated(["candidate_policy_id", "feature_component"]).any():
            raise ValueError(f"Duplicate artifact keys in {name}")
        if len(tables[name]) != len(candidates) * len(FEATURE_COMPONENTS):
            raise ValueError(f"Incomplete candidate/component grain in {name}")
    weighted = scoring.inventory_candidate_weighted_scores
    ranking = scoring.inventory_candidate_ranking
    metric_order = policy.metrics["metric_key"].tolist()
    if weighted.groupby("candidate_policy_id", observed=True)["metric_key"].apply(list).tolist() != [metric_order] * len(candidates):
        raise ValueError("Weighted scores do not use canonical candidate/metric order")
    for row in ranking[ranking["eligible"]].itertuples(index=False):
        actual = weighted.loc[
            weighted["candidate_policy_id"].eq(row.candidate_policy_id), "weighted_score"
        ].sum()
        if not np.isclose(actual, row.total_score, atol=1e-12, rtol=0):
            raise ValueError(f"Weighted contributions do not reconcile for {row.candidate_policy_id}")
    recommendation = scoring.inventory_campaign_recommendation
    if len(recommendation) != 1:
        raise ValueError("Recommendation artifact must contain exactly one row")
    recommended = ranking.loc[ranking["recommendation_state"].eq("recommended")]
    value = recommendation.iloc[0].get("recommended_candidate_policy_id")
    expected = recommended.iloc[0]["candidate_policy_id"] if len(recommended) == 1 else None
    if value != expected:
        raise ValueError("Scoring recommendation mismatch")
    series = tables["inventory_candidate_feature_series"]
    required_series = {"series_id", "is_baseline", "geo_id", "date", "feature_component", "raw_feature_value"}
    if not required_series.issubset(series.columns):
        raise ValueError(f"Series evidence is missing columns: {sorted(required_series.difference(series.columns))}")
    if set(series["series_id"]) != {"baseline", *candidates}:
        raise ValueError("Series evidence identity mismatch")
    if series.duplicated(["series_id", "geo_id", "date", "feature_component"]).any():
        raise ValueError("Duplicate artifact keys in inventory_candidate_feature_series")


def _score_figures(directory: Path, scoring: InventoryCandidateScoringResult,
                   policy: InventoryScoringPolicy, candidates: tuple[str, ...]) -> list[Path]:
    weighted = scoring.inventory_candidate_weighted_scores
    metrics = policy.metrics["metric_key"].tolist()
    normalized = weighted.pivot(index="candidate_policy_id", columns="metric_key", values="normalized_score").reindex(candidates)
    first = directory / "normalized_metric_comparison.png"
    _bars(first, "Normalized scoring metrics (0.5 warmup is neutral when tied)", list(candidates), [(m, normalized[m].tolist()) for m in metrics], maximum=1.0)
    contributions = weighted.pivot(index="candidate_policy_id", columns="metric_key", values="weighted_score").reindex(candidates)
    second = directory / "weighted_score_decomposition.png"
    _bars(second, "Weighted score decomposition — advisory winner, not promotion", list(candidates), [(m, contributions[m].tolist()) for m in metrics], stacked=True)
    ranking = scoring.inventory_candidate_ranking.sort_values("rank", na_position="last")
    third = directory / "candidate_ranking_summary.png"; image, draw = _canvas("Candidate ranking — recommendation is not promotion", height=360)
    headers = ("rank", "candidate", "eligible", "total", "state", "tie break")
    draw.text((30, 65), " | ".join(headers), fill="black")
    for number, row in enumerate(ranking.itertuples(index=False)):
        text = f"{row.rank} | {row.candidate_policy_id} | {row.eligible} | {row.total_score:.6f} | {row.recommendation_state} | {row.tie_break_reason}"
        draw.text((30, 95 + number * 45), text, fill="black")
    _save_image(image, third)
    return [first, second, third]


def _evidence_figures(root: Path, tables: Mapping[str, pd.DataFrame], candidates: tuple[str, ...]) -> list[Path]:
    output = []
    calendar = tables["inventory_candidate_calendar_month_behavior"]
    for component in FEATURE_COMPONENTS:
        lines = []
        for candidate in candidates:
            part = calendar[calendar["candidate_policy_id"].eq(candidate) & calendar["feature_component"].eq(component)].sort_values("calendar_month")
            lines.append((candidate, part["calendar_month"].tolist(), part["mean_absolute_monthly_change"].tolist()))
        path = root / "calendar_month_profiles" / f"{component}.png"; _lines(path, f"Calendar-month profile — {component} (months 1 through 12)", lines, "mean absolute monthly change"); output.append(path)
    series = tables["inventory_candidate_feature_series"].copy(); series["date"] = pd.to_datetime(series["date"])
    for (geo, component), group in series.groupby(["geo_id", "feature_component"], sort=True):
        lines = []
        for series_id in ("baseline", *candidates):
            part = group[group["series_id"].eq(series_id)].sort_values("date"); lines.append((series_id, part["date"].tolist(), part["raw_feature_value"].tolist()))
        start, end = group["date"].min().date(), group["date"].max().date(); path = root / "time_series_overlays" / f"{_slug(geo)}__{component}.png"
        _lines(path, f"{geo} — {component} ({start} to {end})", lines, "authoritative feature value"); output.append(path)
    windows = tables["inventory_transition_review_windows"]
    for row in windows.sort_values(["geo_id", "feature_component"]).itertuples(index=False):
        group = series[series["geo_id"].eq(row.geo_id) & series["feature_component"].eq(row.feature_component) & series["date"].between(pd.Timestamp(row.window_start), pd.Timestamp(row.window_end))]
        lines = []
        for series_id in ("baseline", *candidates):
            part = group[group["series_id"].eq(series_id)].sort_values("date"); lines.append((series_id, part["date"].tolist(), part["raw_feature_value"].tolist()))
        path = root / "transition_windows" / f"{_slug(row.geo_id)}__{row.feature_component}.png"; _lines(path, f"Deterministic transition window — {row.geo_id} / {row.feature_component}", lines, "authoritative feature value"); output.append(path)
    specs = (("volatility", "inventory_candidate_feature_statistics", "standard_deviation", "Standard deviation (lower dispersion)"), ("sign_flip", "inventory_candidate_feature_statistics", "sign_flip_rate", "Sign-flip rate (lower is better)"), ("trend_preservation", "inventory_candidate_baseline_feature_comparison", "correlation", "Baseline correlation (higher is better)"))
    for folder, table_name, value, title in specs:
        frame = tables[table_name]; series_values = []
        for component in FEATURE_COMPONENTS:
            part = frame[frame["feature_component"].eq(component)].set_index("candidate_policy_id").reindex(candidates); series_values.append((component, part[value].tolist()))
        path = root / folder / f"{folder}_comparison.png"; _bars(path, title, list(candidates), series_values); output.append(path)
    return output

def _hash(path: Path) -> str:
    digest = hashlib.sha256(); digest.update(path.read_bytes()); return digest.hexdigest()


def build_inventory_review_bundle(
    *, campaign: CalibrationCampaign, phase_a_evidence: PhaseAEvidence,
    scoring_result: InventoryCandidateScoringResult, output_root: str | Path,
    scoring_policy: InventoryScoringPolicy | None = None,
    source_lineage: Mapping[str, object] | None = None, overwrite: bool = False,
) -> InventoryReviewBundleResult:
    """Render supplied immutable evidence; never orchestrate upstream computation."""
    policy = scoring_policy or load_inventory_scoring_policy()
    tables = _evidence_tables(phase_a_evidence)
    _validate(campaign, phase_a_evidence, scoring_result, policy, tables)
    bundle = Path(output_root) / "inventory_calibration" / _slug(campaign.campaign_id) / _slug(campaign.campaign_version)
    archive = bundle.parent / f"{bundle.name}.zip"
    if bundle.exists() or archive.exists():
        if not overwrite:
            raise FileExistsError(f"Review bundle output already exists: {bundle}")
        shutil.rmtree(bundle, ignore_errors=True); archive.unlink(missing_ok=True)
    for name in ("tables", "metadata", *(f"figures/{item}" for item in FIGURE_DIRECTORIES)):
        (bundle / name).mkdir(parents=True, exist_ok=True)
    candidates = campaign.candidate_policy_ids
    for name, frame in {**scoring_result.tables, **{key: tables[key] for key in EVIDENCE_TABLES}}.items():
        _ordered(frame, candidates).to_csv(bundle / "tables" / f"{name}.csv", index=False, lineterminator="\n")
    (bundle / "metadata" / "campaign.json").write_text(json.dumps(campaign.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    policy.metrics.to_csv(bundle / "metadata" / "scoring_policy.csv", index=False, lineterminator="\n")
    lineage = {"baseline_run_id": campaign.baseline_run_id, "incumbent_run_id": campaign.incumbent_run_id, **dict(source_lineage or {})}
    (bundle / "metadata" / "source_lineage.json").write_text(json.dumps(lineage, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    figures = _score_figures(bundle / "figures" / "score_summary", scoring_result, policy, candidates)
    figures += _evidence_figures(bundle / "figures", tables, candidates)
    ranking = scoring_result.inventory_candidate_ranking
    recommendation = scoring_result.inventory_campaign_recommendation.iloc[0]
    figure_links = "\n".join(f'<li><a href="{path.relative_to(bundle).as_posix()}">{html.escape(path.stem.replace("_", " "))}</a> — authoritative evidence visualization.</li>' for path in figures)
    failed = scoring_result.inventory_candidate_eligibility.query("not gate_pass")
    failed_html = "<p>None.</p>" if failed.empty else failed.to_html(index=False, escape=True)
    page = f"""<!doctype html><html><head><meta charset=\"utf-8\"><title>Inventory calibration review</title>
<style>body{{font:14px sans-serif;max-width:1200px;margin:2rem auto;color:#222}}table{{border-collapse:collapse}}th,td{{border:1px solid #bbb;padding:.35rem}}.notice{{background:#fff3cd;padding:1rem;font-weight:bold}}</style></head><body>
<h1>Inventory calibration human-review bundle</h1><div class=\"notice\">Objective recommendation available; visual review bundle available; human decision pending; promotion not performed.</div>
<h2>Campaign identity</h2><p><b>{html.escape(campaign.campaign_id)} / {html.escape(campaign.campaign_version)}</b><br>Baseline: {html.escape(campaign.baseline_run_id)} ({html.escape(campaign.baseline_policy_id)}); incumbent: {html.escape(campaign.incumbent_run_id)} ({html.escape(campaign.incumbent_policy_id)})<br>Target: {campaign.target_metric} / {campaign.target_dimension} / {campaign.target_axis}<br>Candidates (canonical order): {', '.join(candidates)}</p>
<h2>Advisory recommendation</h2><p>{html.escape(str(recommendation.get('recommended_candidate_policy_id')))} — recommended_for_human_review only. Eligible candidates: {int(ranking['eligible'].sum())}.</p>
<h2>Normalized metric scores and weighted decomposition</h2>{scoring_result.inventory_candidate_weighted_scores.to_html(index=False, escape=True)}
<h2>Final ranking</h2>{ranking.to_html(index=False, escape=True)}
<h2>Failed gates</h2>{failed_html}<h2>Primary figures</h2><ul>{figure_links}</ul>
<p>Calendar profiles reveal recurring month behavior; full overlays reveal smoothing, lag, turning points, and oscillation; deterministic transition windows focus the largest baseline change; statistic charts reproduce scorer inputs.</p></body></html>"""
    (bundle / "review_summary.html").write_text(page, encoding="utf-8")
    (bundle / "README.md").write_text(
        "# Inventory calibration human-review bundle\n\nOpen `review_summary.html` locally. "
        "Tables are immutable copies of Phase A and Slice 3 inputs; figures only pivot and render those values. "
        "The manifest reconciles file hashes and lineage. The recommendation is advisory, human review is pending, and no promotion occurred.\n",
        encoding="utf-8",
    )
    files = sorted(path for path in bundle.rglob("*") if path.is_file())
    manifest: dict[str, object] = {
        "bundle_contract_version": BUNDLE_CONTRACT_VERSION,
        "campaign": {key: campaign.to_dict()[key] for key in ("campaign_id", "campaign_version", "target_metric", "target_dimension", "target_axis")},
        "scoring_contract_version": policy.contract_version,
        "baseline_run_id": campaign.baseline_run_id, "incumbent_run_id": campaign.incumbent_run_id,
        "baseline_policy_id": campaign.baseline_policy_id, "incumbent_policy_id": campaign.incumbent_policy_id,
        "candidate_policy_ids": list(candidates),
        "regime_scope": campaign.metadata.get("geography_scope", {}).get("regime_scope", "macro"),
        "included_geo_levels": campaign.metadata.get("geography_scope", {}).get("included_geo_levels", list(campaign.allowed_geo_levels)),
        "zip_future_status": campaign.metadata.get("geography_scope", {}).get(
            "zip_future_status", "reserved_for_future_local_regime"),
        "city_status": campaign.metadata.get("geography_scope", {}).get(
            "city_status", "out_of_scope_no_current_regime_role"),
        "geography_identity": {
            key: campaign.metadata.get("geography_scope", {}).get(key)
            for key in ("authoritative_geography_manifest_path", "authoritative_geography_manifest_hash",
                        "authoritative_identity_column", "identity_crosswalk_path", "identity_crosswalk_hash",
                        "identity_resolution_mode")
        },
        "recommended_candidate_policy_id": recommendation.get("recommended_candidate_policy_id"),
        "recommendation_status": "recommended_for_human_review",
        "eligible_candidate_count": int(ranking["eligible"].sum()),
        "source_artifacts": lineage,
        "generation": {"module": __name__, "version": GENERATOR_VERSION},
        "flags": {"promotion_performed": False, "challengers_rebuilt_by_renderer": False, "normalization_rerun_by_renderer": False, "scoring_rerun_by_renderer": False},
        "files": [{"relative_path": path.relative_to(bundle).as_posix(), "size_bytes": path.stat().st_size, "sha256": _hash(path)} for path in files],
        "manifest_self_hash_excluded": True,
    }
    (bundle / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    files = sorted(path for path in bundle.rglob("*") if path.is_file())
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zipped:
        for path in files:
            info = zipfile.ZipInfo(path.relative_to(bundle.parent).as_posix(), date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED; info.external_attr = 0o644 << 16
            zipped.writestr(info, path.read_bytes(), compresslevel=9)
    with zipfile.ZipFile(archive) as zipped:
        expected = {path.relative_to(bundle.parent).as_posix() for path in files}
        if set(zipped.namelist()) != expected:
            raise ValueError("ZIP is missing required review files")
    return InventoryReviewBundleResult(bundle, archive, manifest, tuple(files))
