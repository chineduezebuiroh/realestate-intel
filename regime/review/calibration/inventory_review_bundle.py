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
from .system_evidence import (
    CalibrationSystemEvidence,
    NORMALIZED_METRIC_SECTION,
    SYSTEM_SECTIONS,
    validate_system_evidence,
)
from .engine_decomposition import (
    DECOMPOSITION_SECTIONS, EngineDecompositionEvidence, validate_engine_decomposition,
)


BUNDLE_CONTRACT_VERSION = "calibration_review_bundle_v7"
GENERATOR_VERSION = "8.0"
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


def _timeline_domain(lines: list[tuple[str, list[object], list[float]]]) -> tuple[pd.Timestamp, pd.Timestamp]:
    dates = [pd.Timestamp(value) for _, xs, _ in lines for value in xs]
    if not dates:
        raise ValueError("Timeline requires at least one timestamp")
    return min(dates), max(dates)


def _timeline_x(value: object, start: pd.Timestamp, end: pd.Timestamp, left: int = 90, right: int = 1170) -> float:
    span = max((end - start).total_seconds(), 1.0)
    return left + (pd.Timestamp(value) - start).total_seconds() / span * (right - left)


def _monthly_gap(previous: object, current: object) -> bool:
    """Campaign-governed monthly gap rule; never infer cadence from a series."""
    previous, current = pd.Timestamp(previous), pd.Timestamp(current)
    return (current.year * 12 + current.month) - (previous.year * 12 + previous.month) > 1


def _lines(path: Path, title: str, lines: list[tuple[str, list[object], list[float]]], ylabel: str) -> None:
    image, draw = _canvas(title)
    colors = ("black", "#4c78a8", "#f58518", "#54a24b", "#e45756")
    left, top, right, bottom = 90, 70, 1170, 510
    draw.line((left, top, left, bottom, right, bottom), fill="black", width=2); draw.text((5, 280), ylabel, fill="black")
    values = [float(v) for _, _, row in lines for v in row if pd.notna(v)]
    start_date, end_date = _timeline_domain(lines)
    low, high = min([0.0, *values]), max([0.0, *values]); span = high - low or 1.0
    zero_y = bottom - (0.0 - low) / span * (bottom - top)
    draw.line((left, zero_y, right, zero_y), fill="#999999", width=1)
    for tick in range(5):
        fraction = tick / 4
        tick_date = start_date + (end_date - start_date) * fraction
        x = left + fraction * (right - left)
        draw.line((x, bottom, x, bottom + 5), fill="black")
        draw.text((x - 28, bottom + 9), tick_date.strftime("%Y-%m"), fill="black")
    for index, (label, xs, ys) in enumerate(lines):
        observations = sorted(zip((pd.Timestamp(x) for x in xs), ys), key=lambda item: item[0])
        segments: list[list[tuple[float, float]]] = [[]]
        previous = None
        for date, value in observations:
            if pd.isna(value): continue
            if previous is not None and _monthly_gap(previous, date):
                segments.append([])
            x = _timeline_x(date, start_date, end_date, left, right); y = bottom - (float(value) - low) / span * (bottom - top)
            segments[-1].append((x, y)); previous = date
        for points in segments:
            if len(points) > 1: draw.line(points, fill=colors[index % len(colors)], width=3 if index == 0 else 2)
            elif points: draw.ellipse((points[0][0]-2, points[0][1]-2, points[0][0]+2, points[0][1]+2), fill=colors[index % len(colors)])
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


SERIES_COLORS = ("#111111", "#4c78a8", "#f58518", "#54a24b", "#e45756")


def _label(series_id: str, incumbent_policy_id: str) -> str:
    if series_id == "baseline":
        return f"Incumbent — {incumbent_policy_id}"
    return f"Challenger — {series_id}"


def _chronology_plot(path: Path, frame: pd.DataFrame, *, geo_id: str, value: str,
                     title: str, ylabel: str, incumbent: str, note: str,
                     center_date: object | None = None) -> None:
    image, draw = _canvas(f"{title} — {geo_id}", height=650)
    left, top, right, bottom = 110, 90, 1160, 520
    draw.line((left, top, left, bottom, right, bottom), fill="black", width=2)
    draw.text((left, bottom + 25), "Observation date", fill="black")
    draw.text((8, 280), ylabel, fill="black")
    values = pd.to_numeric(frame[value], errors="coerce"); low, high = values.min(), values.max()
    span = float(high - low) or 1.0
    all_dates = pd.to_datetime(frame["date"]); date_low, date_high = all_dates.min(), all_dates.max()
    date_span = max((date_high - date_low).total_seconds(), 1)
    for index, (series_id, part) in enumerate(frame.groupby("series_id", sort=False)):
        part = part.sort_values("date", kind="mergesort")
        points = []
        for date, number in zip(pd.to_datetime(part["date"]), pd.to_numeric(part[value], errors="coerce")):
            if pd.notna(number):
                x = left + (date - date_low).total_seconds() / date_span * (right - left)
                y = bottom - (float(number) - float(low)) / span * (bottom - top)
                points.append((x, y))
        if len(points) > 1:
            draw.line(points, fill=SERIES_COLORS[index % len(SERIES_COLORS)], width=4 if series_id == "baseline" else 2)
        draw.text((760, 42 + index * 14), _label(str(series_id), incumbent), fill=SERIES_COLORS[index % len(SERIES_COLORS)])
    if center_date is not None:
        center = pd.Timestamp(center_date)
        x = left + (center - date_low).total_seconds() / date_span * (right - left)
        draw.line((x, top, x, bottom), fill="#8b0000", width=3)
        draw.text((max(left, x - 70), top + 5), f"Center event: {center.date().isoformat()}", fill="#8b0000")
    draw.text((left, bottom + 45), f"{date_low.date()} to {date_high.date()} | range {float(low):.3f} to {float(high):.3f}", fill="#444")
    draw.text((20, 600), note, fill="#444")
    _save_image(image, path)


def _coordinate_plot(path: Path, frame: pd.DataFrame, *, geo_id: str, incumbent: str) -> None:
    image, draw = _canvas(f"Supply–Demand Coordinate Trajectory — {geo_id}", width=900, height=800)
    left, top, right, bottom = 110, 100, 850, 680
    xs = pd.to_numeric(frame["x_supply"], errors="coerce"); ys = pd.to_numeric(frame["y_demand"], errors="coerce")
    xmin, xmax, ymin, ymax = float(xs.min()), float(xs.max()), float(ys.min()), float(ys.max())
    xspan, yspan = xmax - xmin or 1.0, ymax - ymin or 1.0
    sx = lambda v: left + (float(v) - xmin) / xspan * (right - left)
    sy = lambda v: bottom - (float(v) - ymin) / yspan * (bottom - top)
    draw.rectangle((left, top, right, bottom), outline="black", width=2)
    if xmin <= 0 <= xmax: draw.line((sx(0), top, sx(0), bottom), fill="#777", width=1)
    if ymin <= 0 <= ymax: draw.line((left, sy(0), right, sy(0)), fill="#777", width=1)
    for index, (series_id, part) in enumerate(frame.groupby("series_id", sort=False)):
        part = part.sort_values("date", kind="mergesort").reset_index(drop=True)
        # A deterministic maximum of 36 evenly spaced anchors prevents dense
        # histories from hiding material trajectory separation.
        anchors = np.unique(np.linspace(0, max(len(part) - 1, 0), min(len(part), 36), dtype=int))
        shown = part.iloc[anchors]
        x = pd.to_numeric(shown["x_supply"], errors="coerce"); y = pd.to_numeric(shown["y_demand"], errors="coerce")
        color = SERIES_COLORS[index % len(SERIES_COLORS)]
        points = [(sx(a), sy(b)) for a, b in zip(x, y) if pd.notna(a) and pd.notna(b)]
        if len(points) > 1: draw.line(points, fill=color, width=4 if series_id == "baseline" else 2)
        if len(shown):
            draw.ellipse((sx(x.iloc[0])-5, sy(y.iloc[0])-5, sx(x.iloc[0])+5, sy(y.iloc[0])+5), outline=color, width=2)
            draw.polygon(((sx(x.iloc[-1])+7, sy(y.iloc[-1])), (sx(x.iloc[-1])-5, sy(y.iloc[-1])-6), (sx(x.iloc[-1])-5, sy(y.iloc[-1])+6)), fill=color)
        draw.text((500, 44 + index * 13), _label(str(series_id), incumbent), fill=color)
    draw.text((left, 705), "Supply coordinate (x_supply; higher = greater supply pressure)", fill="black")
    draw.text((8, 380), "Demand coordinate (y_demand; higher = stronger demand)", fill="black")
    draw.text((20, 755), "Source: immutable coordinate_trajectories.csv. Up to 36 evenly spaced full-history anchors; circle=start, triangle=end; zero lines mark quadrant boundaries.", fill="#444")
    _save_image(image, path)


def _regime_plot(path: Path, frame: pd.DataFrame, *, geo_id: str, incumbent: str) -> None:
    image, draw = _canvas(f"Assigned Major Regime Over Time — {geo_id}", height=650)
    ordered_series = list(dict.fromkeys(frame["series_id"].astype(str)))
    categories = sorted(frame["major_regime"].fillna("unassigned").astype(str).unique())
    palette = ("#4c78a8", "#f58518", "#54a24b", "#e45756", "#b279a2", "#72b7b2", "#ff9da6", "#9d755d")
    colors = {category: palette[i % len(palette)] for i, category in enumerate(categories)}
    left, right = 310, 1160
    dates_all = pd.to_datetime(frame["date"]); dmin, dmax = dates_all.min(), dates_all.max(); dspan=max((dmax-dmin).total_seconds(),1)
    for row, series_id in enumerate(ordered_series):
        part = frame[frame["series_id"].astype(str).eq(series_id)].sort_values("date")
        dates = pd.to_datetime(part["date"])
        y = 105 + row * 65
        draw.text((15, y - 5), _label(series_id, incumbent), fill="black")
        for date, category in zip(dates, part["major_regime"].fillna("unassigned").astype(str)):
            x = left + (date-dmin).total_seconds()/dspan*(right-left)
            draw.rectangle((x-4,y-12,x+4,y+12), fill=colors[category])
        changed = part["major_regime"].astype(str).ne(part["major_regime"].astype(str).shift())
        for date in dates[changed]:
            x=left+(date-dmin).total_seconds()/dspan*(right-left); draw.line((x,y-18,x,y+18),fill="black",width=2)
    legend_y = 105 + len(ordered_series)*65
    for i, category in enumerate(categories):
        x=left+(i%4)*205; y=legend_y+(i//4)*22; draw.rectangle((x,y,x+12,y+12),fill=colors[category]); draw.text((x+18,y),category,fill="black")
    draw.text((left, 570), f"Assignment date: {dmin.date()} to {dmax.date()}", fill="black")
    draw.text((20, 610), "Source: immutable regime_chronology.csv major_regime. Color=categorical identity; black ticks=transitions. Minor regime and quadrant remain in CSV.", fill="#444")
    _save_image(image, path)


def _system_figures(bundle: Path, tables: Mapping[str, pd.DataFrame], campaign: CalibrationCampaign) -> dict[str, list[Path]]:
    outputs: dict[str, list[Path]] = {name: [] for name in SYSTEM_SECTIONS}
    specs = {
        "dimension_chronology": ("dimension_score", "Supply Dimension Score", "Supply dimension score (engine units)"),
        "axis_chronology": ("axis_score", "Supply Axis Score", "Supply axis score (engine units)"),
        "cancellation_diagnostics": ("dimension_cancellation_ratio", "Supply-Axis Contribution Cancellation", "Cancellation ratio (0=no offsetting; 1=full offsetting)"),
    }
    for section, (value, title, ylabel) in specs.items():
        frame = tables[section]
        for geo_id, geo in frame.groupby("geo_id", sort=True):
            path = bundle / "system_evidence" / section / f"{_slug(geo_id)}.png"
            _chronology_plot(path, geo, geo_id=str(geo_id), value=value, title=title, ylabel=ylabel,
                             incumbent=campaign.incumbent_policy_id,
                             note=f"Source: immutable {section}.csv. Solid black is incumbent; dashed colored lines are challengers.")
            outputs[section].append(path)
    for geo_id, geo in tables["coordinate_trajectories"].groupby("geo_id", sort=True):
        path = bundle / "system_evidence/coordinate_trajectories" / f"{_slug(geo_id)}.png"
        _coordinate_plot(path, geo, geo_id=str(geo_id), incumbent=campaign.incumbent_policy_id); outputs["coordinate_trajectories"].append(path)
    for geo_id, geo in tables["regime_chronology"].groupby("geo_id", sort=True):
        path = bundle / "system_evidence/regime_chronology" / f"{_slug(geo_id)}.png"
        _regime_plot(path, geo, geo_id=str(geo_id), incumbent=campaign.incumbent_policy_id); outputs["regime_chronology"].append(path)
    for geo_id, geo in tables["transition_windows"].groupby("geo_id", sort=True):
        center = geo["window_center_date"].iloc[0]
        path = bundle / "system_evidence/transition_windows" / f"{_slug(geo_id)}.png"
        _chronology_plot(path, geo, geo_id=str(geo_id), value="axis_score", title="Incumbent Supply-Axis Transition Window",
                         ylabel="Supply axis score (engine units)", incumbent=campaign.incumbent_policy_id, center_date=center,
                         note="Source: immutable transition_windows.csv. Center is the largest absolute incumbent Supply-axis month-over-month change; three observations each side where available.")
        outputs["transition_windows"].append(path)
    return outputs


def build_inventory_review_bundle(
    *, campaign: CalibrationCampaign, phase_a_evidence: PhaseAEvidence,
    scoring_result: InventoryCandidateScoringResult, output_root: str | Path,
    system_evidence: CalibrationSystemEvidence,
    decomposition_evidence: EngineDecompositionEvidence | None = None,
    scoring_policy: InventoryScoringPolicy | None = None,
    source_lineage: Mapping[str, object] | None = None, overwrite: bool = False,
) -> InventoryReviewBundleResult:
    """Render supplied immutable evidence; never orchestrate upstream computation."""
    policy = scoring_policy or load_inventory_scoring_policy()
    tables = _evidence_tables(phase_a_evidence)
    _validate(campaign, phase_a_evidence, scoring_result, policy, tables)
    validate_system_evidence(system_evidence)
    decomposition_evidence = decomposition_evidence or phase_a_evidence.decomposition_evidence
    if decomposition_evidence is None:
        raise ValueError("Immutable Engine Decomposition evidence is required")
    validate_engine_decomposition(decomposition_evidence)
    if (decomposition_evidence.campaign_id, decomposition_evidence.campaign_version,
            decomposition_evidence.candidate_policy_ids, decomposition_evidence.primary_decomposition_axes,
            decomposition_evidence.supporting_coordinate_axes) != (
            campaign.campaign_id, campaign.campaign_version, campaign.candidate_policy_ids,
            campaign.primary_decomposition_axes, campaign.supporting_coordinate_axes):
        raise ValueError("Engine Decomposition evidence does not reconcile to the campaign")
    expected_system_identity = (
        system_evidence.campaign_id, system_evidence.campaign_version,
        system_evidence.candidate_policy_ids, system_evidence.incumbent_policy_id,
        system_evidence.baseline_policy_id, system_evidence.target_metric,
        system_evidence.target_dimension, system_evidence.target_axis,
    )
    campaign_identity = (
        campaign.campaign_id, campaign.campaign_version,
        campaign.candidate_policy_ids, campaign.incumbent_policy_id,
        campaign.baseline_policy_id, campaign.target_metric,
        campaign.target_dimension, campaign.target_axis,
    )
    if expected_system_identity != campaign_identity:
        raise ValueError("System evidence does not reconcile to the scoring campaign")
    bundle = Path(output_root) / "inventory_calibration" / _slug(campaign.campaign_id) / _slug(campaign.campaign_version)
    archive = bundle.parent / f"{bundle.name}.zip"
    if bundle.exists() or archive.exists():
        if not overwrite:
            raise FileExistsError(f"Review bundle output already exists: {bundle}")
        shutil.rmtree(bundle, ignore_errors=True); archive.unlink(missing_ok=True)
    for name in (
        "tables", "metadata", "technical_evidence/metric_score_decomposition",
        "technical_evidence/metric_chronology",
        "technical_evidence/calendar_month_profiles",
        "technical_evidence/metric_transition_windows",
        "technical_evidence/statistic_comparisons",
        *(f"system_evidence/{item}" for item in SYSTEM_SECTIONS),
        *(f"engine_decomposition/{item}" for item in DECOMPOSITION_SECTIONS),
        *(f"figures/{item}" for item in FIGURE_DIRECTORIES),
    ):
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
    system_tables = system_evidence.copied_tables()
    decomposition_tables = decomposition_evidence.copied_tables()
    review_geographies = set(system_tables[SYSTEM_SECTIONS[0]]["geo_id"].astype(str))
    decomposition_figures: dict[str, list[Path]] = {name: [] for name in (
        "feature_to_metric", "metric_to_dimension", "dimension_to_axis")}
    for section, frame in decomposition_tables.items():
        frame.to_csv(bundle / "engine_decomposition" / section / f"{section}.csv",
                     index=False, lineterminator="\n")
        if section not in decomposition_figures:
            continue
        frame = frame[frame["geo_id"].astype(str).isin(review_geographies)]
        label_column = {"feature_to_metric": "feature_key", "metric_to_dimension": "canonical_metric_key",
                        "dimension_to_axis": "dimension"}[section]
        parent_identity_column = {"feature_to_metric": "canonical_metric_key", "metric_to_dimension": "dimension",
                         "dimension_to_axis": "axis"}[section]
        for (geo_id, parent_key), geo in frame.groupby(["geo_id", parent_identity_column], sort=True):
            lines = []
            for (series_id, label), part in geo.groupby(["series_id", label_column], sort=True):
                part = part.sort_values("date", kind="mergesort")
                lines.append((f"{series_id}: {label}", part["date"].tolist(), part["weighted_contribution"].astype(float).tolist()))
            for series_id, part in geo.groupby("series_id", sort=True):
                conflicts = part.groupby("date")["parent_score"].nunique(dropna=False)
                if (conflicts > 1).any():
                    raise ValueError(f"Conflicting supplied parent scores for {section}/{geo_id}/{parent_key}")
                parent = part.groupby("date", as_index=False, sort=True)["parent_score"].first()
                lines.append((f"{series_id}: parent", parent["date"].tolist(), parent["parent_score"].astype(float).tolist()))
            path = bundle / "engine_decomposition" / section / f"{_slug(geo_id)}__{_slug(parent_key)}__{section}.png"
            _lines(path, f"{section.replace('_', ' ').title()} — {geo_id} — {parent_key}", lines, "Engine score / weighted contribution")
            decomposition_figures[section].append(path)
    for section in SYSTEM_SECTIONS:
        frame = system_tables[section].sort_values(
            [c for c in ("geo_id", "series_id", "date", "window_id") if c in system_tables[section]],
            kind="mergesort",
        )
        csv_path = bundle / "system_evidence" / section / f"{section}.csv"
        frame.to_csv(csv_path, index=False, lineterminator="\n")
    normalized = system_tables.get(NORMALIZED_METRIC_SECTION)
    if normalized is not None:
        normalized_path = bundle / "technical_evidence/metric_chronology/normalized_metric_score_chronology.csv"
        normalized.sort_values(["geo_id", "series_id", "date"], kind="mergesort").to_csv(
            normalized_path, index=False, lineterminator="\n")
        for geo_id, geo in normalized.groupby("geo_id", sort=True):
            path = normalized_path.parent / f"{_slug(geo_id)}__normalized_metric_score.png"
            _chronology_plot(path, geo, geo_id=str(geo_id), value="metric_score",
                             title="Active Inventory — Normalized Metric Score",
                             ylabel="Normalized active_inventory metric score (engine score units)",
                             incumbent=campaign.incumbent_policy_id,
                             note="Source: immutable aligned metric-score evidence. This is the normalized metric stage, not the raw feature, Supply dimension, or Supply axis.")
    system_figures = _system_figures(bundle, system_tables, campaign)
    selection = {
        "representative_geography_rule": system_evidence.representative_geography_rule,
        "selected_geographies": sorted(set(system_tables[SYSTEM_SECTIONS[0]]["geo_id"].astype(str))),
        "transition_window_rule": system_evidence.transition_window_rule,
    }
    (bundle / "metadata" / "system_evidence_selection.json").write_text(
        json.dumps(selection, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    ranking = scoring_result.inventory_candidate_ranking
    recommendation = scoring_result.inventory_campaign_recommendation.iloc[0]
    failed = scoring_result.inventory_candidate_eligibility.query("not gate_pass")
    failed_html = "<p>None.</p>" if failed.empty else failed.to_html(index=False, escape=True)
    selected_geographies = selection["selected_geographies"]

    def image_tag(path: Path, caption: str) -> str:
        relative = path.relative_to(bundle).as_posix()
        return f'<figure><a href="{relative}"><img src="{relative}" alt="{html.escape(caption)}"></a><figcaption>{html.escape(caption)}</figcaption></figure>'

    def system_panels(section: str, caption: str) -> str:
        return "".join(image_tag(path, f"{caption} — {path.stem}") for path in system_figures[section])

    raw_panels = "".join(
        image_tag(bundle / "figures/time_series_overlays" / f"{_slug(geo)}__level.png",
                  f"Active Inventory — Raw Observations / level feature — {geo}")
        for geo in selected_geographies
        if (bundle / "figures/time_series_overlays" / f"{_slug(geo)}__level.png").is_file()
    )
    normalized_panels = ""
    if normalized is not None:
        normalized_panels = "".join(
            image_tag(bundle / "technical_evidence/metric_chronology" / f"{_slug(geo)}__normalized_metric_score.png",
                      f"Active Inventory — Normalized Metric Score — {geo}")
            for geo in selected_geographies
        )
    decomposition = image_tag(bundle / "figures/score_summary/weighted_score_decomposition.png",
                              "Candidate Calibration-Score Decomposition — weighted contributions sum to each candidate total")
    def decomposition_panels(section: str, parent: str, heading: str) -> str:
        selected = [path for path in decomposition_figures[section] if f"__{_slug(parent)}__" in path.name]
        return (f"<h3>{heading}</h3><p>Each chart has one parent object and sign-preserving child contributions with its persisted parent overlay.</p>" +
                "".join(image_tag(path, f"{heading} — {path.stem}") for path in selected) +
                f'<p><a href="engine_decomposition/{section}/{section}.csv">Direct detailed CSV</a></p>')
    focused = "".join((
        decomposition_panels("feature_to_metric", "active_inventory", "3.1 Active Inventory — Feature-to-Metric"),
        decomposition_panels("feature_to_metric", "permit_activity", "3.2 Building Permits (BPS / permit_activity) — Feature-to-Metric"),
        decomposition_panels("metric_to_dimension", "supply", "3.3 Supply — Metric-to-Dimension"),
        decomposition_panels("dimension_to_axis", "supply", "3.4 Supply Axis — Dimension-to-Axis"),
    ))
    focused_names = {path for section, parent in (("feature_to_metric", "active_inventory"), ("feature_to_metric", "permit_activity"),
                    ("metric_to_dimension", "supply"), ("dimension_to_axis", "supply"))
                    for path in decomposition_figures[section] if f"__{_slug(parent)}__" in path.name}
    supporting = "".join(image_tag(path, f"Additional parent-specific decomposition — {path.stem}")
                         for paths in decomposition_figures.values() for path in paths if path not in focused_names)
    reconciliation = decomposition_tables["reconciliation_summary"]
    recon_summary = reconciliation.groupby(
        ["layer", "reconciliation_status", "reason_code"], as_index=False, dropna=False
    ).agg(
        row_count=("parent_key", "size"),
        max_reconciled_residual=("absolute_residual", lambda values: values.max(skipna=True)),
    )
    transition_centers = ", ".join(
        f"{geo}: {pd.Timestamp(part['window_center_date'].iloc[0]).date().isoformat()}"
        for geo, part in system_tables["transition_windows"].groupby("geo_id", sort=True)
    )
    page = f"""<!doctype html><html><head><meta charset=\"utf-8\"><title>Inventory calibration review</title>
<style>body{{font:15px system-ui,sans-serif;max-width:1280px;margin:2rem auto;color:#20242a;line-height:1.5}}nav a{{margin-right:1rem}}table{{border-collapse:collapse;font-size:12px;display:block;overflow:auto}}th,td{{border:1px solid #bbb;padding:.35rem}}.notice{{background:#fff3cd;border-left:5px solid #d39e00;padding:1rem;font-weight:bold}}.context{{background:#eef5fb;padding:.8rem}}figure{{margin:1.2rem 0 2rem}}img{{max-width:100%;height:auto;border:1px solid #ccd}}figcaption{{font-size:13px;color:#4b5563}}code{{background:#eee;padding:.1rem .25rem}}</style></head><body>
<h1>Calibration Review</h1><nav><a href="#summary">Summary</a><a href="#technical">Technical</a><a href="#engine">Engine Decomposition</a><a href="#system">System</a><a href="#supporting">Artifacts</a><a href="#decision">Decision</a></nav>
<div class=\"notice\">Objective recommendation available; human decision pending; promotion not performed. All figures consume immutable evidence and are advisory-only.</div>
<h2 id="summary">1. Executive Summary</h2><p><b>{html.escape(campaign.campaign_id)} / {html.escape(campaign.campaign_version)}</b><br>Incumbent: {html.escape(campaign.incumbent_policy_id)}; target chain: <code>{campaign.target_metric}</code> → {campaign.target_dimension.title()} dimension → {campaign.target_axis.title()} axis → coordinates → regimes.<br>Challengers, in canonical order: {', '.join(candidates)}.</p>
<p>Advisory recommendation: <b>{html.escape(str(recommendation.get('recommended_candidate_policy_id')))}</b> for human review only. Smoothing can overlap at the metric layer yet diverge downstream because persisted metric, dimension, axis, coordinate, and categorical-regime artifacts represent successive engine stages. Similarity is not itself evidence of superiority.</p>
<h3>Candidate ranking</h3>{ranking.to_html(index=False, escape=True)}
<h2 id="technical">2. Technical Evidence</h2><p class="context">This section separates the already-produced active-inventory feature chronology from its normalized metric score and from the weighted campaign scoring components. Review suppression and reduction alongside trend-shape preservation and warmup coverage; a smoother line alone is not a decision rule.</p>
<h3>2.1 Raw Metric Chronology</h3><p>The level feature shows the supplied active-inventory observations after each policy's already-materialized smoothing treatment; values are not normalized metric scores.</p>{raw_panels}
<h3>2.2 Normalized Metric-Score Chronology</h3><p>The engine-produced <code>metric_score</code> for <code>active_inventory</code>. This is distinct from raw feature values and downstream Supply scores.</p>{normalized_panels or '<p>Normalized metric-score chronology was not supplied in this evidence package.</p>'}
<h3>2.3 Candidate Calibration-Score Decomposition</h3><p>Stacked bars use the persisted weighted contributions; no engine contribution is recalculated.</p>{decomposition}{scoring_result.inventory_candidate_weighted_scores.to_html(index=False, escape=True)}
<h2 id="engine">3. Engine Decomposition</h2><p><b>Strict decomposition:</b> {', '.join(axis.title() + ' axis' for axis in campaign.primary_decomposition_axes)}.<br><b>Supporting coordinate/regime axes:</b> {', '.join(axis.title() + ' axis' for axis in campaign.supporting_coordinate_axes if axis not in campaign.primary_decomposition_axes) or 'None'}.</p>{decomposition_tables['axis_scope_lineage'].to_html(index=False, escape=True)}{focused}
<h3>3.5 Coverage and Start-Date Explanation</h3>{decomposition_tables['chronology_coverage'].to_html(index=False, escape=True)}
<h3>3.6 Reconciliation Summary</h3>{recon_summary.to_html(index=False, escape=True)}<p><a href="engine_decomposition/reconciliation_summary/reconciliation_summary.csv">Detailed reconciliation evidence</a></p>
<h3>3.7 Additional Supporting Decompositions</h3>{supporting or '<p>No additional parent objects.</p>'}
<h2 id="system">4. System Evidence</h2><p class="context">Follow each selected geography through Supply dimension → Supply axis → coordinates → regimes → transitions → cancellation.</p>
<h3>4.1 Supply Dimension Chronology</h3>{system_panels('dimension_chronology', 'Supply Dimension Score')}
<h3>4.2 Supply Axis Chronology</h3>{system_panels('axis_chronology', 'Supply Axis Score')}
<h3>4.3 Supply–Demand Coordinate Trajectory</h3><p>The supplied coordinate evidence plots <code>x_supply</code> horizontally and <code>y_demand</code> vertically. Up to 36 evenly spaced full-history anchors are selected only for display; start/end markers preserve direction and zero lines expose quadrant boundaries. Coordinates are copied from immutable engine artifacts and are not recomputed by this renderer.</p>{system_panels('coordinate_trajectories', 'Supply–Demand Coordinate Trajectory')}
<h3>4.4 Regime Chronology</h3><p>Colors identify the persisted categorical <code>major_regime</code>; transition markers show dates where that identity changes. The exact CSV retains <code>minor_regime</code> and <code>quadrant</code> for every observation. No categorical assignment is recomputed while rendering.</p>{system_panels('regime_chronology', 'Assigned Major Regime Over Time')}
<h3>4.5 Transition Windows</h3><p><b>Persisted selection rule:</b> {html.escape(system_evidence.transition_window_rule)}.<br><b>Center event:</b> the largest absolute month-over-month incumbent Supply-axis change for each selected geography.<br><b>Selected center dates:</b> {html.escape(transition_centers)}.<br><b>Context width:</b> up to three persisted observations on each side of the center. Review timing, amplitude, lag, and shape; this visualization does not issue a promotion judgment.</p>{system_panels('transition_windows', 'Incumbent Supply-Axis Transition Window')}
<h3>4.6 Cancellation Diagnostics</h3><p><b>Target axis:</b> {html.escape(campaign.target_axis.title())}.<br><b>Contributing dimensions:</b> Supply and Capital Markets.<br><b>Cancellation measure:</b> <code>1 − abs(axis_score_change_1m) / gross_dimension_contribution_change_1m</code>.<br><b>Gross contribution change:</b> sum of the absolute weighted dimension-contribution changes.<br><b>Range:</b> [0, 1], undefined when gross contribution change is zero.<br><b>Interpretation of 0:</b> no offsetting dimension movement.<br><b>Interpretation of 1:</b> complete offsetting dimension movement. No new materiality threshold is introduced.</p>{system_panels('cancellation_diagnostics', 'Supply-Axis Contribution Cancellation')}
<h2 id="supporting">5. Supporting Tables and Artifact Links</h2><ul><li><a href="tables/">Exact Technical Evidence CSVs</a></li><li><a href="engine_decomposition/">Exact Engine Decomposition evidence</a></li>{''.join(f'<li><a href="system_evidence/{s}/{s}.csv">{s.replace("_", " ").title()} CSV</a></li>' for s in SYSTEM_SECTIONS)}<li><a href="metadata/system_evidence_selection.json">System-evidence selection metadata</a></li><li><a href="metadata/source_lineage.json">Source lineage</a></li><li><a href="manifest.json">Bundle hash manifest</a></li></ul>
<h3>Failed gates</h3>{failed_html}
<h2 id="decision">6. Human Decision Status</h2><p>Pending. The recommendation remains advisory-only and no promotion occurred.</p></body></html>"""
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
        "review_sections": {"technical_evidence": True, "engine_decomposition": list(DECOMPOSITION_SECTIONS), "system_evidence": list(SYSTEM_SECTIONS)},
        "system_evidence_selection": selection,
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
