"""Correlation-only diagnostic for the two authoritative permit metrics."""
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

from regime.review.calibration.inventory_review_bundle import CHILD_COMPONENT_STYLE_REGISTRY

CONTRACT_VERSION = "permit_redundancy_diagnostic_v1"
METRICS = ("permit_activity", "permit_intensity")
REVIEW_GEOGRAPHIES = (
    "district_of_columbia_dc__county", "essex_county_nj__county",
    "montgomery_county_md__county", "prince_george_s_county_md__county",
    "fairfax_county_va__county", "san_francisco_county_ca__county",
    "los_angeles_county_ca__county",
)


@dataclass(frozen=True, slots=True)
class PermitRedundancyEvidence:
    contract_version: str
    review_geographies: tuple[str, ...]
    tables: Mapping[str, pd.DataFrame]


def _paired_chronology(aligned: pd.DataFrame, geographies: tuple[str, ...]) -> pd.DataFrame:
    required = {"geo_id", "canonical_metric_key", "metric_score"}
    date_column = "evaluation_date" if "evaluation_date" in aligned else "date"
    required.add(date_column)
    missing = required.difference(aligned.columns)
    if missing:
        raise ValueError(f"Aligned metric evidence is missing columns: {sorted(missing)}")
    absent = set(geographies).difference(aligned["geo_id"].unique())
    if absent:
        raise ValueError(f"Aligned evidence is missing governed geographies: {sorted(absent)}")
    work = aligned[
        aligned.geo_id.isin(geographies) & aligned.canonical_metric_key.isin(METRICS)
    ][["geo_id", date_column, "canonical_metric_key", "metric_score"]].copy()
    work = work.rename(columns={date_column: "date"})
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    if work.date.isna().any():
        raise ValueError("Aligned permit evidence contains invalid evaluation dates")
    keys = ["geo_id", "date", "canonical_metric_key"]
    if work.duplicated(keys).any():
        raise ValueError("Aligned permit evidence contains duplicate metric observations")
    scores = pd.to_numeric(work.metric_score, errors="coerce")
    invalid = work.metric_score.notna() & ~np.isfinite(scores)
    if invalid.any():
        raise ValueError("Aligned permit evidence contains non-finite scores")
    work["metric_score"] = scores
    # The inner pair and dropna are intentional: never fill or interpolate either series.
    paired = work.pivot(index=["geo_id", "date"], columns="canonical_metric_key", values="metric_score").reset_index()
    for metric in METRICS:
        if metric not in paired:
            paired[metric] = np.nan
    return paired.dropna(subset=list(METRICS)).sort_values(["geo_id", "date"], kind="mergesort").reset_index(drop=True)


def _correlation_row(frame: pd.DataFrame, geography: str) -> dict[str, object]:
    correlation = frame[METRICS[0]].corr(frame[METRICS[1]], method="pearson") if len(frame) >= 2 else np.nan
    magnitude = abs(correlation) if pd.notna(correlation) else np.nan
    interpretation = ("undefined (fewer than two paired observations)" if pd.isna(magnitude) else
        "very strong linear association" if magnitude >= .9 else
        "strong linear association" if magnitude >= .7 else
        "moderate linear association" if magnitude >= .5 else "weak linear association")
    return {"geo_id": geography, "pearson_correlation": correlation,
        "correlation_interpretation": interpretation,
        "observation_count": len(frame),
        "first_overlapping_observation": frame.date.min() if len(frame) else pd.NaT,
        "last_overlapping_observation": frame.date.max() if len(frame) else pd.NaT}


def build_permit_redundancy_evidence(aligned_metric_scores: pd.DataFrame,
        review_geographies: tuple[str, ...] = REVIEW_GEOGRAPHIES) -> PermitRedundancyEvidence:
    if tuple(review_geographies) != REVIEW_GEOGRAPHIES:
        raise ValueError("Permit redundancy diagnostic must use the governed seven-geography shortlist")
    paired = _paired_chronology(aligned_metric_scores, review_geographies)
    per_geo = pd.DataFrame([
        _correlation_row(paired[paired.geo_id.eq(geo)], geo) for geo in review_geographies
    ])
    overall = pd.DataFrame([_correlation_row(paired, "combined_governed_chronology")])
    tables = MappingProxyType({"summary": overall, "per_geography_correlations": per_geo,
                              "paired_chronology": paired})
    return PermitRedundancyEvidence(CONTRACT_VERSION, tuple(review_geographies), tables)


def _segments(frame: pd.DataFrame, metric: str) -> list[list[tuple[pd.Timestamp, float]]]:
    segments: list[list[tuple[pd.Timestamp, float]]] = []
    previous = None
    for row in frame[["date", metric]].itertuples(index=False):
        date = pd.Timestamp(row.date)
        if previous is None or (date.year * 12 + date.month) - (previous.year * 12 + previous.month) > 1:
            segments.append([])
        segments[-1].append((date, float(getattr(row, metric)))); previous = date
    return segments


def _render_figure(frame: pd.DataFrame, geography: str, output: Path) -> None:
    width, height = 900, 360; left, right, top, bottom = 70, 860, 40, 300
    values = frame[list(METRICS)].to_numpy().ravel()
    low, high = min(-1.0, float(values.min())), max(1.0, float(values.max()))
    start, end = frame.date.min(), frame.date.max(); seconds = max((end - start).total_seconds(), 1.0)
    def point(date: pd.Timestamp, value: float) -> tuple[float, float]:
        return (left + (date-start).total_seconds()/seconds*(right-left),
                bottom - (value-low)/(high-low)*(bottom-top))
    paths = []
    for metric in METRICS:
        color = CHILD_COMPONENT_STYLE_REGISTRY[metric]
        for segment in _segments(frame, metric):
            commands = " ".join(("M" if i == 0 else "L") + f" {x:.2f} {y:.2f}"
                                for i, (x, y) in enumerate(point(*item) for item in segment))
            paths.append(f"<path d='{commands}' fill='none' stroke='{color}' stroke-width='2'/>")
    zero = point(start, 0)[1]
    svg = (f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' role='img'>"
        f"<title>{html.escape(geography)} permit metric chronology</title><rect width='100%' height='100%' fill='white'/>"
        f"<line x1='{left}' y1='{zero:.2f}' x2='{right}' y2='{zero:.2f}' stroke='#999'/>"
        f"<line x1='{left}' y1='{top}' x2='{left}' y2='{bottom}' stroke='black'/><line x1='{left}' y1='{bottom}' x2='{right}' y2='{bottom}' stroke='black'/>"
        + "".join(paths)
        + f"<text x='90' y='25' fill='{CHILD_COMPONENT_STYLE_REGISTRY[METRICS[0]]}'>permit_activity</text>"
        + f"<text x='260' y='25' fill='{CHILD_COMPONENT_STYLE_REGISTRY[METRICS[1]]}'>permit_intensity</text></svg>")
    output.write_text(svg, encoding="utf-8", newline="\n")


def write_permit_redundancy_bundle(evidence: PermitRedundancyEvidence, output: Path) -> tuple[Path, Path, int]:
    started = time.perf_counter(); output.mkdir(parents=True, exist_ok=True)
    evidence.tables["summary"].to_csv(output / "summary.csv", index=False, lineterminator="\n", date_format="%Y-%m-%d")
    evidence.tables["per_geography_correlations"].to_csv(
        output / "per_geography_correlations.csv", index=False, lineterminator="\n", date_format="%Y-%m-%d")
    figures = output / "figures"; figures.mkdir(exist_ok=True)
    tags = []
    paired = evidence.tables["paired_chronology"]
    for geo in evidence.review_geographies:
        path = figures / f"{geo}.svg"; _render_figure(paired[paired.geo_id.eq(geo)], geo, path)
        tags.append(f"<figure><img src='figures/{path.name}' alt='{html.escape(geo)} permit overlay'><figcaption>{html.escape(geo)}</figcaption></figure>")
    table = evidence.tables["per_geography_correlations"].to_html(index=False, border=0, float_format=lambda x: f"{x:.6f}")
    overall = evidence.tables["summary"].iloc[0]
    review = output / "index.html"
    review.write_text("<!doctype html><html><head><meta charset='utf-8'><title>Permit Redundancy Diagnostic</title>"
        "<style>body{font-family:sans-serif;max-width:1000px;margin:auto}img{max-width:100%}th,td{padding:.3rem}</style></head><body>"
        "<h1>Permit Redundancy Diagnostic</h1><p>Correlation-only diagnostic; no recommendation or promotion.</p>"
        f"<p>Combined governed Pearson correlation: <strong>{overall.pearson_correlation:.6f}</strong> "
        f"across {int(overall.observation_count)} paired observations: {html.escape(overall.correlation_interpretation)}. "
        "This is descriptive only; correlation alone does not establish redundancy.</p>"
        f"<h2>Correlations</h2>{table}<h2>Visual overlays</h2>{''.join(tags)}"
        "<p><a href='summary.csv'>summary.csv</a> · <a href='per_geography_correlations.csv'>per_geography_correlations.csv</a></p>"
        "<h2>Human interpretation</h2><p>Pending. No automatic recommendation and no production promotion.</p></body></html>",
        encoding="utf-8", newline="\n")
    files = sorted(path for path in output.rglob("*") if path.is_file() and path.name != "manifest.json")
    manifest = {"contract_version": evidence.contract_version, "promotion_state": "none", "files": [
        {"path": path.relative_to(output).as_posix(), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        for path in files]}
    (output / "manifest.json").write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    archive_path = output.with_suffix(".zip")
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(p for p in output.rglob("*") if p.is_file()):
            info = zipfile.ZipInfo(path.relative_to(output).as_posix(), (1980, 1, 1, 0, 0, 0)); info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())
    count = len(files) + 1
    print(f"[permit-redundancy] bundle generation: {time.perf_counter()-started:.3f}s; files={count}; zip_bytes={archive_path.stat().st_size}")
    return review, archive_path, count
