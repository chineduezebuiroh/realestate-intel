"""Standalone, artifact-only county macro-regime snapshot renderer."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from html import escape
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

REQUIRED_ARTIFACTS = (
    "regime_assignments.parquet", "coordinates.parquet", "axis_scores.parquet",
    "dimension_scores.parquet", "metric_scores.parquet",
)
REQUIRED_AXES = {"demand", "supply"}
MAJOR_REGIMES = ("expansion", "hypersupply", "recession", "recovery")
REGIME_COLORS = {"expansion": "#12b76a", "hypersupply": "#f79009", "recession": "#d92d20", "recovery": "#2e90fa"}
REGIME_RGB = {"expansion": "18,183,106", "hypersupply": "247,144,9",
              "recession": "217,45,32", "recovery": "46,144,250"}
MICRO_REGIME_OPACITY = {"early": .18, "mid": .115, "late": .065}
# Exact major-sector boundaries governed by regime/_08_geometry_engine.py.
MAJOR_BOUNDARY_DEGREES = (45.0, 135.0, 225.0, 315.0)
RADIAL_REFERENCES = (.25, .50)
PLANE_EXTENT = .60
NATIONAL_GEO_ID = "united_states__nation"
VISUALIZATION_VERSION = "v0.3.0"
SCHEMA_VERSION = "2.0"


@dataclass
class Snapshot:
    current: dict
    drivers: dict[str, pd.DataFrame]
    metric_drivers: dict[str, pd.DataFrame]
    path: pd.DataFrame
    history: pd.DataFrame
    transitions: pd.DataFrame
    explanation: str
    interpretation: dict[str, str]
    freshness: dict[str, dict]


def _truthy(values: pd.Series) -> pd.Series:
    return values.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})


def load_axis_memberships(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Axis registry does not exist: {path}")
    rows = pd.read_csv(path)
    required = {"axis", "dimension", "dimension_weight", "enabled"}
    if not required.issubset(rows):
        raise ValueError(f"Axis registry is missing columns: {sorted(required - set(rows))}")
    rows = rows[_truthy(rows.enabled)].copy()
    rows["axis"] = rows.axis.astype(str).str.strip().str.lower()
    rows["dimension"] = rows.dimension.astype(str).str.strip().str.lower()
    rows["dimension_weight"] = pd.to_numeric(rows.dimension_weight, errors="coerce")
    if set(rows.axis) != REQUIRED_AXES or rows.empty:
        raise ValueError("Active axis registry must resolve exactly Demand and Supply")
    if rows.duplicated(["axis", "dimension"]).any() or rows.dimension_weight.isna().any() or (rows.dimension_weight <= 0).any():
        raise ValueError("Active axis registry has duplicate, non-numeric, or non-positive memberships")
    if not rows.groupby("axis").dimension_weight.sum().map(lambda value: abs(value - 1) <= .001).all():
        raise ValueError("Active dimension weights must sum to 1.0 for each axis")
    return rows[["axis", "dimension", "dimension_weight"]].sort_values(["axis", "dimension"]).reset_index(drop=True)


def load_metric_memberships(path: Path, active_dimensions: set[str]) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Metric registry does not exist: {path}")
    rows = pd.read_csv(path)
    required = {"canonical_metric_key", "dimension", "metric_weight",
                "enabled", "diagnostic_only", "macro_enabled"}
    if not required.issubset(rows):
        raise ValueError(f"Metric registry is missing columns: {sorted(required - set(rows))}")
    rows = rows[_truthy(rows.enabled) & ~_truthy(rows.diagnostic_only) & _truthy(rows.macro_enabled)].copy()
    rows["dimension"] = rows.dimension.astype(str).str.strip().str.lower()
    rows["canonical_metric_key"] = rows.canonical_metric_key.astype(str).str.strip()
    rows["metric_weight"] = pd.to_numeric(rows.metric_weight, errors="coerce")
    # These optional columns are retained in the production registry only as
    # retired schema surface. Active metadata would silently revive a
    # superseded scoring hierarchy, so fail closed exactly as production does.
    legacy_columns = [column for column in ("demand_block", "block_weight") if column in rows]
    if legacy_columns:
        has_legacy_metadata = pd.Series(False, index=rows.index)
        for column in legacy_columns:
            has_legacy_metadata |= rows[column].fillna("").astype(str).str.strip().ne("")
        if has_legacy_metadata.any():
            raise ValueError("Active production metrics must not define superseded Demand block metadata")
    metadata = ["dimension", "canonical_metric_key", "metric_weight"]
    conflicts = rows[metadata].drop_duplicates().groupby(
        ["dimension", "canonical_metric_key"], dropna=False).size()
    if (conflicts > 1).any() or rows.metric_weight.isna().any() or (rows.metric_weight < 0).any():
        raise ValueError("Active metric registry has conflicting, non-numeric, or negative weights")
    rows = rows[rows.dimension.isin(active_dimensions)]
    rows = rows.drop_duplicates(["dimension", "canonical_metric_key"]).copy()
    if set(rows.dimension) != active_dimensions or (rows.groupby("dimension").metric_weight.sum() <= 0).any():
        raise ValueError("Every active dimension must have positive governed metric membership")
    return rows[metadata].sort_values(["dimension", "canonical_metric_key"]).reset_index(drop=True)


def _read_artifacts(run_dir: Path) -> dict[str, pd.DataFrame]:
    if not run_dir.is_dir():
        raise FileNotFoundError(f"Run directory does not exist: {run_dir}")
    missing = [name for name in REQUIRED_ARTIFACTS if not (run_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Required run artifacts are missing: {', '.join(missing)}")
    tables = {name.removesuffix(".parquet"): pd.read_parquet(run_dir / name) for name in REQUIRED_ARTIFACTS}
    for name, table in tables.items():
        if not {"geo_id", "date"}.issubset(table):
            raise ValueError(f"{name}.parquet is missing geo_id/date")
        table["date"] = pd.to_datetime(table.date)
    return tables


def _sentence(axis: str, rows: pd.DataFrame) -> str:
    positive = rows[rows.weighted_contribution > 0].sort_values("weighted_contribution", ascending=False)
    negative = rows[rows.weighted_contribution < 0].sort_values("weighted_contribution")
    parts = [f"{axis.title()} pressure is {'positive' if rows.weighted_contribution.sum() >= 0 else 'negative'}"]
    if not positive.empty:
        parts.append(f"with {positive.iloc[0].display_name} the strongest positive contribution")
    if not negative.empty:
        parts.append(f"and {negative.iloc[0].display_name} the strongest negative contribution")
    return ", ".join(parts) + "."


def _metric_drivers(metrics: pd.DataFrame, memberships: pd.DataFrame, geo_id: str, latest: pd.Timestamp,
                    expected_dimensions: pd.DataFrame) -> dict[str, pd.DataFrame]:
    required = {"canonical_metric_key", "metric_score"}
    if not required.issubset(metrics):
        raise ValueError(f"metric_scores.parquet is missing columns: {sorted(required - set(metrics))}")
    wanted = set(memberships.canonical_metric_key)
    candidates = metrics[metrics.canonical_metric_key.isin(wanted) & metrics.geo_id.isin({geo_id, NATIONAL_GEO_ID}) & metrics.date.le(latest)].copy()
    # County evidence wins; national evidence supplies governed Capital Markets series.
    candidates["geo_priority"] = candidates.geo_id.eq(geo_id).astype(int)
    candidates = candidates.sort_values(["canonical_metric_key", "date", "geo_priority"])
    latest_metrics = (
        candidates
        .groupby("canonical_metric_key", as_index=False)
        .tail(1)
        .copy()
    )
    if latest_metrics.duplicated("canonical_metric_key").any():
        raise ValueError("Metric evidence is not unique after production as-of alignment")
    latest_metrics["metric_score"] = pd.to_numeric(latest_metrics.metric_score, errors="coerce")
    latest_metrics["metric_age_days"] = (latest - latest_metrics.date).dt.days
    result = {}
    for dimension, governed in memberships.groupby("dimension", sort=True):
        rows = governed.merge(latest_metrics[["canonical_metric_key", "metric_score", "metric_age_days"]],
                              on="canonical_metric_key", how="left", validate="one_to_one")
        available = rows.metric_score.notna()
        if rows.loc[available, "metric_weight"].sum() <= 0:
            raise ValueError(f"No persisted metric evidence can reconstruct active dimension: {dimension}")
        rows = rows[available].copy()
        rows["effective_metric_weight"] = rows.metric_weight / rows.metric_weight.sum()
        rows["weighted_metric_contribution"] = rows.metric_score * rows.effective_metric_weight
        expected = float(expected_dimensions.set_index("dimension").loc[dimension, "dimension_score"])
        if not math.isclose(rows.weighted_metric_contribution.sum(), expected, abs_tol=1e-6):
            raise ValueError(f"Persisted metric evidence does not reconcile production dimension: {dimension}")
        rows["display_name"] = rows.canonical_metric_key.str.replace("_", " ").str.title()
        result[dimension] = rows.sort_values("weighted_metric_contribution", key=lambda s: s.abs(), ascending=False, kind="mergesort").reset_index(drop=True)
    return result


def _cadences_from_registries(metric_registry_path: Path, source_registry_path: Path | None,
                              memberships: pd.DataFrame) -> pd.DataFrame:
    result = memberships[["canonical_metric_key"]].drop_duplicates().copy()
    result["frequency"] = "unknown"
    if source_registry_path is None or not source_registry_path.is_file():
        return result
    source = pd.read_csv(source_registry_path)
    if not {"metric_key", "frequency"}.issubset(source):
        raise ValueError("Source metric registry is missing metric_key/frequency")
    governed = pd.read_csv(metric_registry_path)
    aliases = governed[["metric_key", "canonical_metric_key"]].drop_duplicates()
    frequencies = aliases.merge(source[["metric_key", "frequency"]], on="metric_key", how="left")
    conflicts = frequencies.dropna(subset=["frequency"]).groupby("canonical_metric_key").frequency.nunique()
    if (conflicts > 1).any():
        raise ValueError("Governed canonical metrics have conflicting cadence metadata")
    frequency_map = frequencies.dropna(subset=["frequency"]).drop_duplicates("canonical_metric_key")
    result = result.drop(columns="frequency").merge(
        frequency_map[["canonical_metric_key", "frequency"]], on="canonical_metric_key", how="left")
    result["frequency"] = result.frequency.fillna("unknown").astype(str).str.strip().str.lower()
    return result


def _freshness(metric_drivers: dict[str, pd.DataFrame], latest: pd.Timestamp) -> dict[str, dict]:
    evidence = pd.concat(metric_drivers.values(), ignore_index=True).drop_duplicates("canonical_metric_key")
    monthly = evidence[evidence.frequency.isin({"monthly", "month"})]
    annual = evidence[evidence.frequency.isin({"annual", "yearly", "year"})]
    known = {"monthly", "month", "annual", "yearly", "year", "quarterly", "quarter"}
    unknown = evidence[~evidence.frequency.isin(known)]
    def item(rows: pd.DataFrame, cadence: str) -> dict:
        if rows.empty:
            return {"status": "not_applicable", "cadence": cadence, "latest_evidence_date": None,
                    "latest_vintage": None, "max_age_days": None, "metric_count": 0}
        date = pd.to_datetime(rows.evidence_date).max()
        return {"status": "available", "cadence": cadence, "latest_evidence_date": date.date().isoformat(),
                "latest_vintage": int(date.year) if "annual" in cadence else None,
                "max_age_days": int(rows.metric_age_days.max()), "metric_count": int(len(rows))}
    output = {"monthly_indicators": item(monthly, "monthly_indicators"),
              "annual_structural_axis_evidence": item(annual, "annual_structural_axis_evidence")}
    output["unknown"] = {"status": "not_classified" if len(unknown) else "none", "metric_count": int(len(unknown)),
                         "metrics": sorted(unknown.canonical_metric_key.astype(str).tolist())}
    output["latest_evaluation_month"] = latest.date().isoformat()
    return output


def _axis_phrase(value: float, noun: str) -> str:
    direction = "positive" if value > 0 else "negative" if value < 0 else "neutral"
    return f"{noun} is {direction} ({value:+.2f})"


def _interpretation(current: dict, drivers: dict[str, pd.DataFrame], path: pd.DataFrame) -> dict[str, str]:
    condition = (f"{_axis_phrase(current['demand_score'], 'Demand')} while "
                 f"{_axis_phrase(current['supply_score'], 'Supply').lower()}, placing the market in "
                 f"{str(current['minor_regime']).replace('_', ' ').title()}.")
    all_rows = pd.concat([rows.assign(axis=axis) for axis, rows in drivers.items()], ignore_index=True)
    support = all_rows[all_rows.weighted_contribution > 0].sort_values("weighted_contribution", ascending=False)
    drag = all_rows[all_rows.weighted_contribution < 0].sort_values("weighted_contribution")
    parts = []
    if not support.empty:
        row = support.iloc[0]; parts.append(f"The largest positive contribution is {row.display_name} ({row.weighted_contribution:+.3f})")
    if not drag.empty:
        row = drag.iloc[0]; parts.append(f"the largest offset is {row.display_name} ({row.weighted_contribution:+.3f})")
    primary = (", while ".join(parts) + ".") if parts else "Dimension contributions are balanced at the current evaluation month."
    if len(path) < 2:
        movement = "Recent movement is unavailable from the persisted trajectory."
    else:
        first, last = path.iloc[0], path.iloc[-1]
        dd = float(last.demand_strength_score - first.demand_strength_score)
        ds = float(last.supply_pressure_score - first.supply_pressure_score)
        demand = "strengthened" if dd > 0 else "weakened" if dd < 0 else "was unchanged"
        supply = "increased" if ds > 0 else "eased" if ds < 0 else "was unchanged"
        movement = f"Across the displayed 12-month path, Demand {demand} ({dd:+.3f}) and Supply pressure {supply} ({ds:+.3f})."
    return {"current_condition": condition, "primary_drivers": primary, "recent_movement": movement,
            "materiality_note": "Contributions are reported with magnitude; 'largest' is relative and is not a production materiality classification."}


def resolve_snapshot(run_dir: Path, geo_id: str, axis_registry_path: Path, metric_registry_path: Path, source_registry_path: Path | None = None) -> Snapshot:
    tables = _read_artifacts(run_dir)
    memberships = load_axis_memberships(axis_registry_path)
    metric_memberships = load_metric_memberships(metric_registry_path, set(memberships.dimension))
    filtered = {name: table[table.geo_id.eq(geo_id)].copy() for name, table in tables.items() if name != "metric_scores"}
    if filtered["regime_assignments"].empty:
        raise ValueError(f"Geography has no regime chronology: {geo_id}")
    common_dates = set(filtered["regime_assignments"].date)
    for name in ("coordinates", "axis_scores"):
        common_dates &= set(filtered[name].date)
    if not common_dates:
        raise ValueError("No common production regime, coordinate, and axis-score date")
    latest = max(common_dates)
    regime_rows = filtered["regime_assignments"].query("date == @latest")
    coordinate_rows = filtered["coordinates"].query("date == @latest")
    latest_axes = filtered["axis_scores"].query("date == @latest").copy()
    latest_axes["axis"] = latest_axes.axis.astype(str).str.lower()
    latest_axes = latest_axes[latest_axes.axis.isin(REQUIRED_AXES)]
    if len(regime_rows) != 1 or len(coordinate_rows) != 1 or set(latest_axes.axis) != REQUIRED_AXES or latest_axes.duplicated("axis").any():
        raise ValueError("Latest production state is not unique and complete")
    dims = filtered["dimension_scores"].query("date == @latest").copy()
    dims["dimension"] = dims.dimension.astype(str).str.strip().str.lower()
    active = dims[dims.dimension.isin(set(memberships.dimension))]
    if active.duplicated("dimension").any() or set(active.dimension) != set(memberships.dimension) or active.dimension_score.isna().any():
        raise ValueError("Weighted active-axis dimensions are missing at the latest date")
    drivers = {}
    for axis in sorted(REQUIRED_AXES):
        rows = memberships[memberships.axis.eq(axis)].merge(active[["dimension", "dimension_score"]], on="dimension", validate="one_to_one")
        rows["weighted_contribution"] = rows.dimension_score.astype(float) * rows.dimension_weight
        rows["display_name"] = rows.dimension.str.replace("_", " ").str.title()
        drivers[axis] = rows
    metrics = _metric_drivers(tables["metric_scores"], metric_memberships, geo_id, latest, active)
    cadences = _cadences_from_registries(metric_registry_path, source_registry_path, metric_memberships)
    for dimension, rows in metrics.items():
        rows["evidence_date"] = latest - pd.to_timedelta(rows.metric_age_days, unit="D")
        metrics[dimension] = rows.merge(cadences, on="canonical_metric_key", how="left", validate="many_to_one")
    regime, coordinate = regime_rows.iloc[0], coordinate_rows.iloc[0]
    scores = latest_axes.set_index("axis").axis_score
    current = {"as_of_date": latest, "major_regime": regime.major_regime, "minor_regime": regime.minor_regime,
               "demand_score": float(scores.demand), "supply_score": float(scores.supply),
               "regime_strength": float(regime.regime_strength), "max_axis_age_days": int(coordinate.max_axis_age_days)}
    chronology = filtered["regime_assignments"].sort_values("date").query("date <= @latest")
    cutoff = latest - pd.DateOffset(years=5) + pd.offsets.MonthEnd(0)
    history = chronology.query("date >= @cutoff").copy()
    transitions = history[history.major_regime.ne(history.major_regime.shift())].copy()
    explanation = " ".join(_sentence(axis, drivers[axis]) for axis in ("demand", "supply"))
    path = chronology.tail(12).copy()
    interpretation = _interpretation(current, drivers, path)
    freshness = _freshness(metrics, latest)
    return Snapshot(current, drivers, metrics, path, history, transitions, explanation, interpretation, freshness)


def _plane(snapshot: Snapshot) -> go.Figure:
    path = snapshot.path
    fig = go.Figure()
    # These angular bands mirror the governed minor-regime geometry. Persisted
    # labels remain authoritative for every plotted observation.
    sector_specs = (
        (-45, -15, "hypersupply", "late"), (-15, 15, "hypersupply", "mid"),
        (15, 45, "hypersupply", "early"), (45, 75, "expansion", "late"),
        (75, 105, "expansion", "mid"), (105, 135, "expansion", "early"),
        (135, 165, "recovery", "late"), (165, 195, "recovery", "mid"),
        (195, 225, "recovery", "early"), (225, 255, "recession", "late"),
        (255, 285, "recession", "mid"), (285, 315, "recession", "early"),
    )
    for start, end, regime, phase in sector_specs:
        angles = [math.radians(start + (end - start) * i / 24) for i in range(25)]
        fig.add_trace(go.Scatter(x=[0] + [PLANE_EXTENT * math.cos(a) for a in angles] + [0],
            y=[0] + [PLANE_EXTENT * math.sin(a) for a in angles] + [0], fill="toself", mode="lines",
            line={"width": 0}, fillcolor=f"rgba({REGIME_RGB[regime]},{MICRO_REGIME_OPACITY[phase]})",
            hoverinfo="skip", showlegend=False, name=f"{phase}-{regime}-shade"))
    for angle in MAJOR_BOUNDARY_DEGREES:
        rad = math.radians(angle)
        fig.add_shape(type="line", x0=-PLANE_EXTENT * math.cos(rad), y0=-PLANE_EXTENT * math.sin(rad),
                      x1=PLANE_EXTENT * math.cos(rad), y1=PLANE_EXTENT * math.sin(rad),
                      line={"color": "#98a2b3", "width": 1.5, "dash": "dash"}, name="major-regime-boundary")
    for radius in RADIAL_REFERENCES:
        fig.add_shape(type="circle", x0=-radius, y0=-radius, x1=radius, y1=radius,
                      line={"color": "#d0d5dd", "width": 1, "dash": "dot"}, name="radial-reference")
    for text_value, x, y in (("HYPERSUPPLY", .42, 0), ("EXPANSION", 0, .42), ("RECOVERY", -.42, 0), ("RECESSION", 0, -.42)):
        fig.add_annotation(x=x, y=y, text=text_value, showarrow=False, font={"size": 11, "color": "#667085"})
    fig.add_trace(go.Scatter(x=path.supply_pressure_score, y=path.demand_strength_score, mode="lines+markers",
        marker={"size": [5 + 5 * i / max(1, len(path)-1) for i in range(len(path))], "color": list(range(len(path))), "colorscale": "Blues", "showscale": False},
        customdata=path[["date", "major_regime", "minor_regime"]],
        hovertemplate="%{customdata[0]|%b %Y}<br>Supply %{x:+.3f}<br>Demand %{y:+.3f}<br>%{customdata[1]} · %{customdata[2]}<extra></extra>"))
    fig.add_trace(go.Scatter(x=[snapshot.current["supply_score"]], y=[snapshot.current["demand_score"]], mode="markers",
        marker={"size": 15, "color": "#b42318", "line": {"color": "#ffffff", "width": 2}},
        customdata=[[snapshot.current["as_of_date"], snapshot.current["major_regime"], snapshot.current["minor_regime"]]],
        hovertemplate="%{customdata[0]|%b %Y}<br>Supply %{x:+.3f}<br>Demand %{y:+.3f}<br>%{customdata[1]} · %{customdata[2]}<extra></extra>",
        name="current-state-marker"))
    fig.update_layout(template="plotly_white", height=500, margin={"l": 55, "r": 25, "t": 20, "b": 50}, showlegend=False,
        xaxis={"title": "Supply pressure", "range": [-PLANE_EXTENT, PLANE_EXTENT], "zeroline": True,
               "zerolinecolor": "rgba(152,162,179,.42)", "zerolinewidth": .75, "constrain": "domain"},
        yaxis={"title": "Demand strength", "range": [-PLANE_EXTENT, PLANE_EXTENT], "zeroline": True,
               "zerolinecolor": "rgba(152,162,179,.42)", "zerolinewidth": .75, "scaleanchor": "x", "scaleratio": 1})
    return fig


def _drivers(rows: pd.DataFrame, title: str) -> go.Figure:
    rows = rows.sort_values("weighted_contribution")
    fig = go.Figure(go.Bar(x=rows.weighted_contribution, y=rows.display_name, orientation="h",
        marker_color=["#b42318" if x < 0 else "#175cd3" for x in rows.weighted_contribution], customdata=rows[["dimension_score", "dimension_weight"]],
        hovertemplate="%{y}<br>Raw score %{customdata[0]:+.3f}<br>Weight %{customdata[1]:.3f}<br>Contribution %{x:+.3f}<extra></extra>"))
    fig.update_layout(template="plotly_white", title={"text": title, "font": {"size": 16}}, height=230, margin={"l": 120, "r": 20, "t": 45, "b": 35},
        xaxis={"title": "Weighted contribution", "zeroline": True, "zerolinecolor": "#667085"}, yaxis={"title": ""}, showlegend=False)
    return fig


def _metric_chart(rows: pd.DataFrame) -> go.Figure:
    plotted = rows.iloc[::-1]
    custom = plotted[["metric_score", "metric_weight", "effective_metric_weight", "metric_age_days"]]
    fig = go.Figure(go.Bar(x=plotted.weighted_metric_contribution, y=plotted.display_name, orientation="h",
        marker_color=["#b42318" if x < 0 else "#175cd3" for x in plotted.weighted_metric_contribution], customdata=custom,
        hovertemplate="%{y}<br>Metric score %{customdata[0]:+.3f}<br>Configured weight %{customdata[1]:.4f}<br>Effective weight %{customdata[2]:.4f}<br>Contribution %{x:+.3f}<br>Age %{customdata[3]} days<extra></extra>"))
    fig.update_layout(template="plotly_white", height=max(210, 42 * len(rows) + 80), margin={"l": 155, "r": 20, "t": 15, "b": 45},
        xaxis={"title": "Weighted metric contribution", "zeroline": True, "zerolinecolor": "#667085"}, yaxis={"title": ""}, showlegend=False)
    return fig


def _history(snapshot: Snapshot) -> go.Figure:
    fig = go.Figure()
    custom = snapshot.history[["major_regime", "minor_regime"]]
    for column, name, color in (("demand_strength_score", "Demand", "#175cd3"), ("supply_pressure_score", "Supply", "#b54708")):
        fig.add_trace(go.Scatter(x=snapshot.history.date, y=snapshot.history[column], name=name, mode="lines", line={"color": color}, customdata=custom,
            hovertemplate="%{x|%b %Y}<br>" + name + " %{y:+.3f}<br>%{customdata[0]} · %{customdata[1]}<extra></extra>"))
    fig.update_layout(template="plotly_white", height=340, margin={"l": 55, "r": 25, "t": 20, "b": 45}, hovermode="x unified",
        xaxis={"range": [snapshot.history.date.min(), snapshot.history.date.max()]}, yaxis={"title": "Axis score", "zeroline": True, "zerolinecolor": "#344054", "zerolinewidth": 1.5}, legend={"orientation": "h", "y": 1.08})
    return fig


def _regime_strip(snapshot: Snapshot) -> go.Figure:
    history = snapshot.history
    fig = go.Figure()
    custom = history[["major_regime", "minor_regime"]]
    for regime in MAJOR_REGIMES:
        mask = history.major_regime.eq(regime)
        fig.add_trace(go.Bar(x=history.date[mask], y=[1] * int(mask.sum()), width=31 * 24 * 60 * 60 * 1000,
            name=regime.title(), marker_color=REGIME_COLORS[regime], customdata=custom[mask],
            hovertemplate="%{x|%b %Y}<br>%{customdata[0]} · %{customdata[1]}<extra></extra>"))
    for date in snapshot.transitions.date.iloc[1:]:
        fig.add_vline(x=date.timestamp() * 1000, line_color="#344054", line_width=1)
    fig.update_layout(template="plotly_white", barmode="overlay", height=180, margin={"l": 55, "r": 25, "t": 48, "b": 40},
        xaxis={"range": [history.date.min(), history.date.max()]}, yaxis={"visible": False, "range": [0, 1]},
        legend={"orientation": "h", "y": 1.18, "yanchor": "bottom", "x": 0, "xanchor": "left"},
        bargap=0, showlegend=True)
    return fig


def _freshness_text(freshness: dict[str, dict]) -> tuple[str, str]:
    monthly = freshness["monthly_indicators"]
    annual = freshness["annual_structural_axis_evidence"]
    monthly_text = (f"Monthly indicators: current through {pd.Timestamp(monthly['latest_evidence_date']):%b %Y}"
                    if monthly["status"] == "available" else "Monthly indicators: cadence unavailable")
    annual_text = (f"Annual/structural axis evidence: latest governed vintage {annual['latest_vintage']}"
                   if annual["status"] == "available" else "Annual/structural axis evidence: none active")
    return monthly_text, annual_text


def _presentation_summary(snapshot: Snapshot) -> dict[str, str | float]:
    """Resolve deterministic, presentation-only executive copy."""
    current = snapshot.current
    all_drivers = pd.concat(
        [rows.assign(axis=axis) for axis, rows in snapshot.drivers.items()],
        ignore_index=True,
    )
    # Capital Markets intentionally belongs to both axes; keep axis context
    # rather than collapsing its two governed contributions.
    positive = all_drivers.sort_values(
        ["weighted_contribution", "display_name"], ascending=[False, True], kind="mergesort"
    )
    negative = all_drivers.sort_values(
        ["weighted_contribution", "display_name"], ascending=[True, True], kind="mergesort"
    )
    support = positive[positive.weighted_contribution > 0].head(2)
    headwind = negative[negative.weighted_contribution < 0].head(1)

    first, last = snapshot.path.iloc[0], snapshot.path.iloc[-1]
    demand_change = float(last.demand_strength_score - first.demand_strength_score)
    supply_change = float(last.supply_pressure_score - first.supply_pressure_score)

    def direction(value: float, positive_word: str, negative_word: str) -> str:
        return positive_word if value > 0 else negative_word if value < 0 else "Unchanged"

    support_text = "; ".join(
        f"{row.display_name} ({row.weighted_contribution:+.3f}, {row.axis.title()})"
        for row in support.itertuples()
    ) or "No positive dimension contribution"
    headwind_text = (
        f"{headwind.iloc[0].display_name} ({headwind.iloc[0].weighted_contribution:+.3f}, "
        f"{headwind.iloc[0].axis.title()})"
        if not headwind.empty else "No negative dimension contribution"
    )
    largest_axis = "Demand" if abs(demand_change) >= abs(supply_change) else "Supply"
    largest_change = demand_change if largest_axis == "Demand" else supply_change
    regime = str(current["minor_regime"]).replace("_", " ").title()
    interpretation = (
        f"{regime}, with Demand at {current['demand_score']:+.2f} and Supply pressure at "
        f"{current['supply_score']:+.2f}. Over the displayed year, {largest_axis} had the larger "
        f"absolute movement ({largest_change:+.3f})."
    )

    def contribution(dimension: str, axis: str) -> str:
        rows = snapshot.drivers[axis]
        match = rows[rows.dimension.eq(dimension)]
        return (f"{float(match.iloc[0].weighted_contribution):+.3f} on {axis.title()}"
                if not match.empty else "Not an active member")

    return {
        "interpretation": interpretation,
        "demand_change": demand_change,
        "supply_change": supply_change,
        "demand_direction": direction(demand_change, "Strengthened", "Weakened"),
        "supply_direction": direction(supply_change, "Increased", "Eased"),
        "largest_change": f"{largest_axis} {largest_change:+.3f}",
        "support": support_text,
        "headwind": headwind_text,
        "capital_markets_demand": contribution("capital_markets", "demand"),
        "capital_markets_supply": contribution("capital_markets", "supply"),
        "affordability": contribution("affordability", "demand"),
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def render_snapshot(run_dir: Path, geo_id: str, market_name: str, output_dir: Path,
                    axis_registry_path: Path, metric_registry_path: Path,
                    source_registry_path: Path | None = None, *, county_href: str | None = None) -> tuple[Path, Path, Snapshot]:
    if output_dir.resolve() == run_dir.resolve() or run_dir.resolve() in output_dir.resolve().parents:
        raise ValueError("Visualization outputs must not be written into an immutable production run")
    snapshot = resolve_snapshot(run_dir, geo_id, axis_registry_path, metric_registry_path, source_registry_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    config = {"displayModeBar": False, "responsive": True}
    plane = pio.to_html(_plane(snapshot), full_html=False, include_plotlyjs="inline", config=config, div_id=f"{geo_id}-regime-plane-chart")
    dimensions = [pio.to_html(_drivers(snapshot.drivers[a], f"{a.title()} drivers"), full_html=False,
                              include_plotlyjs=False, config=config, div_id=f"{geo_id}-{a}-dimension-chart") for a in ("demand", "supply")]
    summaries = [_sentence(a, snapshot.drivers[a]) for a in ("demand", "supply")]
    drilldowns = []
    for axis in ("demand", "supply"):
        blocks = []
        ordered = snapshot.drivers[axis].sort_values("weighted_contribution", key=lambda x: x.abs(), ascending=False)
        for dimension in ordered.dimension:
            rows = snapshot.metric_drivers[dimension]
            chart = pio.to_html(_metric_chart(rows), full_html=False, include_plotlyjs=False,
                                config=config, div_id=f"{geo_id}-{axis}-{dimension}-metric-chart")
            label = escape(dimension.replace("_", " ").title())
            blocks.append(f'<details data-axis="{axis}" data-dimension="{dimension}"><summary>{label} metric evidence</summary>{chart}</details>')
        drilldowns.append(f'<div class="axis-evidence"><h3>{axis.title()} axis</h3>{"".join(blocks)}</div>')
    history = pio.to_html(_history(snapshot), full_html=False, include_plotlyjs=False, config=config, div_id=f"{geo_id}-history-chart")
    strip = pio.to_html(_regime_strip(snapshot), full_html=False, include_plotlyjs=False, config=config, div_id=f"{geo_id}-regime-history-chart")
    current, interpretation = snapshot.current, snapshot.interpretation
    executive = _presentation_summary(snapshot)
    monthly_text, annual_text = _freshness_text(snapshot.freshness)
    index_link = f'<a class="county-link" href="{escape(county_href)}">← All counties</a>' if county_href else ""
    axis_hash = _sha256(axis_registry_path)
    metric_hash = _sha256(metric_registry_path)
    source_hash = (_sha256(source_registry_path)
                   if source_registry_path and source_registry_path.is_file() else None)
    html = f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{escape(market_name)} County Macro Regime</title>
<style>
:root{{--ink:#101828;--muted:#667085;--soft:#f2f4f7;--line:#eaecf0;--canvas:#f8fafc;--blue:#175cd3;--red:#b42318;--green:#067647}}*{{box-sizing:border-box}}html{{scroll-behavior:smooth;scroll-padding-top:72px}}body{{font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:var(--ink);background:var(--canvas);margin:0;line-height:1.55}}main{{max-width:1400px;margin:auto;padding:32px 32px 80px}}.sticky-nav{{position:sticky;top:0;z-index:10;background:rgba(255,255,255,.97);border-bottom:1px solid var(--line);padding:11px max(20px,calc((100vw - 1400px)/2));display:flex;gap:24px;overflow-x:auto;white-space:nowrap}}.sticky-nav a{{color:#344054;text-decoration:none;font-size:13px;font-weight:680;letter-spacing:.01em}}.sticky-nav a:hover{{color:var(--blue)}}.county-link{{margin-left:auto}}section{{margin:72px 0}}.hero{{background:#fff;border:1px solid var(--line);border-radius:16px;padding:34px;margin-top:16px;box-shadow:0 1px 2px rgba(16,24,40,.04)}}.hero-head,.section-head{{display:flex;justify-content:space-between;gap:24px;align-items:start}}.eyebrow{{color:var(--blue);font-size:12px;font-weight:750;letter-spacing:.09em;text-transform:uppercase;margin:0 0 6px}}.meta,.section-intro{{color:var(--muted)}}h1{{font-size:30px;line-height:1.2;letter-spacing:-.02em;margin:0}}h2{{font-size:28px;line-height:1.25;letter-spacing:-.02em;margin:0 0 8px}}h3{{font-size:17px;margin:0 0 8px}}p{{margin:8px 0}}.section-intro{{max-width:720px;margin:0}}.brief-grid{{display:grid;grid-template-columns:minmax(280px,.8fr) minmax(0,1.5fr);gap:36px;margin-top:30px}}.regime-label{{font-size:44px;line-height:1.02;font-weight:780;letter-spacing:-.035em;margin:10px 0 16px}}.brief-copy{{font-size:18px;max-width:760px;margin:0 0 22px}}.signal-grid{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}}.signal{{background:var(--soft);border-radius:10px;padding:14px 16px;min-height:92px}}.signal span{{display:block;color:var(--muted);font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.04em}}.signal strong{{display:block;font-size:18px;margin-top:4px}}.signal small{{color:#475467}}.brief-factors{{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}}.factor{{border:1px solid var(--line);border-radius:10px;padding:14px 16px}}.factor b{{display:block;font-size:13px;margin-bottom:3px}}.freshness-summary{{border-top:1px solid var(--line);margin-top:18px;padding-top:14px;color:#475467;font-size:14px}}.freshness-summary span+span{{margin-left:14px}}.position-grid{{display:grid;grid-template-columns:minmax(0,1.45fr) minmax(300px,.75fr);gap:24px;align-items:center;margin-top:24px}}.surface{{background:#fff;border:1px solid var(--line);border-radius:12px;padding:18px}}.coordinate-grid{{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin:18px 0}}.coordinate{{border-left:3px solid #d0d5dd;padding-left:12px}}.coordinate span{{display:block;color:var(--muted);font-size:13px}}.coordinate b{{font-size:24px}}.drivers{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-top:24px}}.driver-card{{background:#fff;border:1px solid var(--line);border-radius:12px;padding:20px}}.driver-summary{{color:#475467;min-height:48px}}.callout-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:22px 0}}.callout{{border-top:3px solid #98a2b3;background:#fff;padding:14px;border-radius:0 0 8px 8px}}.callout span{{display:block;color:var(--muted);font-size:12px;font-weight:700;text-transform:uppercase}}.trajectory-card{{background:#fff;border:1px solid var(--line);border-radius:12px;padding:20px;margin-top:18px}}.chart-title{{margin:18px 0 0}}.evidence-grid{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-top:22px}}details{{background:#fff;border:1px solid var(--line);border-radius:9px;margin:10px 0;padding:13px 15px}}summary{{cursor:pointer;font-weight:680;color:#344054}}details[open] summary{{margin-bottom:14px}}.audit-grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:22px}}.method-grid{{display:grid;grid-template-columns:repeat(2,1fr);gap:8px 24px;font-size:14px}}.method-grid dt{{color:var(--muted)}}.method-grid dd{{margin:0 0 8px;overflow-wrap:anywhere}}code.hash{{display:block;overflow-wrap:anywhere;font-size:11px;color:#475467}}.legacy-anchor{{position:absolute;visibility:hidden}}
@media(max-width:1080px){{.brief-grid,.position-grid{{grid-template-columns:1fr}}.signal-grid{{grid-template-columns:repeat(3,1fr)}}.callout-grid{{grid-template-columns:repeat(2,1fr)}}}}
@media(max-width:820px){{html{{scroll-padding-top:58px}}main{{padding:20px 16px 60px}}section{{margin:52px 0}}.hero{{padding:24px}}.hero-head,.section-head{{display:block}}.hero-head .meta{{margin-top:8px}}.drivers,.evidence-grid,.audit-grid{{grid-template-columns:1fr}}.regime-label{{font-size:36px}}.county-link{{margin-left:0}}.sticky-nav{{gap:16px}}}}
@media(max-width:560px){{.signal-grid,.brief-factors,.callout-grid,.coordinate-grid,.method-grid{{grid-template-columns:1fr}}.signal{{min-height:0}}.freshness-summary span{{display:block}}.freshness-summary span+span{{margin:3px 0 0}}h1{{font-size:26px}}h2{{font-size:24px}}.hero{{padding:20px}}}}
</style></head><body>
<nav class="sticky-nav" aria-label="Dashboard sections"><a href="#executive-brief">Brief</a><a href="#current-position">Position</a><a href="#market-drivers">Drivers</a><a href="#market-trajectory">Trajectory</a><a href="#supporting-evidence">Evidence</a><a href="#audit">Audit</a>{index_link}</nav><main>
<section id="executive-brief" class="hero"><div class="hero-head"><div><p class="eyebrow">Executive Brief</p><h1>{escape(market_name)}</h1></div><p class="meta">Evidence through {current['as_of_date']:%B %Y}</p></div><div class="brief-grid"><div><p class="meta">Current market regime</p><p class="regime-label">{escape(str(current['minor_regime']).replace('_', ' ').upper())}</p><div class="coordinate-grid"><div class="coordinate"><span>Demand</span><b>{current['demand_score']:+.2f}</b></div><div class="coordinate"><span>Supply</span><b>{current['supply_score']:+.2f}</b></div></div></div><div><p class="brief-copy">{escape(str(executive['interpretation']))}</p><div class="signal-grid"><div class="signal"><span>Demand direction</span><strong>{executive['demand_direction']}</strong><small>{executive['demand_change']:+.3f} over displayed year</small></div><div class="signal"><span>Supply direction</span><strong>{executive['supply_direction']}</strong><small>{executive['supply_change']:+.3f} over displayed year</small></div><div class="signal"><span>Largest axis change</span><strong>{executive['largest_change']}</strong><small>Absolute movement</small></div></div><div class="brief-factors"><div class="factor"><b>Major supporting factors</b>{escape(str(executive['support']))}</div><div class="factor"><b>Primary headwind</b>{escape(str(executive['headwind']))}</div></div><div class="freshness-summary"><span>{escape(monthly_text)}</span><span>{escape(annual_text)}</span></div></div></div></section>
<section id="current-position"><span id="market-regime" class="legacy-anchor"></span><span id="market-interpretation" class="legacy-anchor"></span><div class="section-head"><div><p class="eyebrow">Current Position</p><h2>What is the market doing today?</h2><p class="section-intro">The governed plane places Supply on the horizontal axis and Demand on the vertical axis. Persisted assignments—not display geometry—remain authoritative.</p></div></div><div class="position-grid"><div class="surface" aria-label="Twelve-month governed regime plane">{plane}</div><aside class="surface"><p class="eyebrow">Current reading</p><h3>{escape(str(current['minor_regime']).replace('_', ' ').title())}</h3><div class="coordinate-grid"><div class="coordinate"><span>Demand</span><b>{current['demand_score']:+.2f}</b></div><div class="coordinate"><span>Supply</span><b>{current['supply_score']:+.2f}</b></div></div><p>{escape(interpretation['current_condition'])}</p><p class="meta">{escape(interpretation['recent_movement'])}</p></aside></div></section>
<section id="market-drivers"><span id="regime-drivers" class="legacy-anchor"></span><div class="section-head"><div><p class="eyebrow">Drivers</p><h2>What's driving the market?</h2><p class="section-intro">Governed dimension contributions explain the current Demand and Supply axis scores. Signs and magnitudes remain explicit.</p></div></div><div class="drivers"><article class="driver-card"><h3>Demand-side contributions</h3><p class="driver-summary">{escape(summaries[0])}</p>{dimensions[0]}</article><article class="driver-card"><h3>Supply-side contributions</h3><p class="driver-summary">{escape(summaries[1])}</p>{dimensions[1]}</article></div></section>
<section id="market-trajectory"><div class="section-head"><div><p class="eyebrow">Trajectory</p><h2>What changed?</h2><p class="section-intro">Five years of persisted axis history show how the market reached its current position; transition markers identify changes in major regime.</p></div></div><div class="callout-grid"><div class="callout"><span>Demand movement</span><b>{executive['demand_direction']} {executive['demand_change']:+.3f}</b></div><div class="callout"><span>Supply movement</span><b>{executive['supply_direction']} {executive['supply_change']:+.3f}</b></div><div class="callout"><span>Capital Markets</span><b>{executive['capital_markets_demand']}; {executive['capital_markets_supply']}</b></div><div class="callout"><span>Affordability</span><b>{executive['affordability']}</b></div></div><article class="trajectory-card"><h3>Demand and Supply — five years</h3>{history}<h3 class="chart-title">Regime transitions</h3>{strip}</article></section>
<section id="supporting-evidence"><span id="evidence-detail" class="legacy-anchor"></span><div class="section-head"><div><p class="eyebrow">Evidence</p><h2>What supports this conclusion?</h2><p class="section-intro">Open a dimension to inspect its persisted metric scores, governed weights, contributions, and evidence age. Detail stays collapsed by default.</p></div></div><div class="evidence-grid">{''.join(drilldowns)}</div></section>
<section id="audit"><span id="data-methodology" class="legacy-anchor"></span><div class="section-head"><div><p class="eyebrow">Audit</p><h2>How was this determined?</h2><p class="section-intro">Freshness, methodology, and immutable input identities are available without competing with the decision view.</p></div></div><div class="audit-grid"><details><summary>Freshness and methodology</summary><p>{escape(monthly_text)}.</p><p>{escape(annual_text)}.</p><p>Unclassified cadence metrics: {snapshot.freshness['unknown']['metric_count']}.</p><p><strong>Oldest contributing active-axis input:</strong> {current['max_axis_age_days']} days.</p><p class="meta">Artifact-only rendering. Persisted assignments are authoritative; display geometry does not reclassify points.</p></details><details><summary>Run provenance and registry identities</summary><dl class="method-grid"><div><dt>Run ID</dt><dd>{escape(run_dir.name)}</dd><dt>Geography ID</dt><dd>{escape(geo_id)}</dd><dt>Market label</dt><dd>{escape(market_name)}</dd></div><div><dt>Evaluation month</dt><dd>{current['as_of_date']:%Y-%m-%d}</dd><dt>Visualization / schema</dt><dd>{VISUALIZATION_VERSION} / {SCHEMA_VERSION}</dd><dt>Source run</dt><dd>{escape(str(run_dir))}</dd></div></dl><p><b>Axis registry</b><code class="hash">{axis_hash}</code></p><p><b>Metric registry</b><code class="hash">{metric_hash}</code></p>{f'<p><b>Source registry</b><code class="hash">{source_hash}</code></p>' if source_hash else ''}</details></div></section>
</main></body></html>'''
    html_path, json_path = output_dir / f"{geo_id}.html", output_dir / f"{geo_id}_snapshot.json"
    html_path.write_text(html, encoding="utf-8", newline="\n")
    base = ["canonical_metric_key", "metric_score", "metric_weight", "effective_metric_weight",
            "weighted_metric_contribution", "metric_age_days", "evidence_date", "frequency"]
    compact = {d: rows[base].to_dict("records") for d, rows in snapshot.metric_drivers.items()}
    legacy = {k: (v.date().isoformat() if isinstance(v, pd.Timestamp) else v) for k, v in current.items()}
    payload = {"schema_version": SCHEMA_VERSION, "visualization_version": VISUALIZATION_VERSION,
               "run_id": run_dir.name, "geo_id": geo_id, "market_name": market_name, **legacy, "latest_state": legacy,
               "demand_drivers": snapshot.drivers["demand"][["dimension", "dimension_score", "dimension_weight", "weighted_contribution"]].to_dict("records"),
               "supply_drivers": snapshot.drivers["supply"][["dimension", "dimension_score", "dimension_weight", "weighted_contribution"]].to_dict("records"),
               "metric_drivers": compact, "explanation": snapshot.explanation, "interpretation": interpretation,
               "executive_summary": executive,
               "cadence_freshness": snapshot.freshness,
               "trajectory": {"plane_months": len(snapshot.path), "history_months": len(snapshot.history),
                              "history_start": snapshot.history.date.min().date().isoformat(), "history_end": snapshot.history.date.max().date().isoformat()},
               "provenance": {"source_run_id": run_dir.name, "source_run_path": str(run_dir),
                              "axis_registry": str(axis_registry_path), "axis_registry_sha256": axis_hash,
                              "metric_registry": str(metric_registry_path), "metric_registry_sha256": metric_hash,
                              "source_registry": str(source_registry_path) if source_registry_path else None,
                              "source_registry_sha256": source_hash}}
    json_path.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8", newline="\n")
    return html_path, json_path, snapshot


def load_county_manifest(path: Path) -> list[dict[str, str]]:
    rows = pd.read_csv(path, dtype=str).fillna("")
    if {"geo_slug", "geo_name", "level"}.issubset(rows):
        rows = rows.rename(columns={"geo_slug": "geo_id", "geo_name": "market_name"})
    elif not {"geo_id", "market_name", "level"}.issubset(rows):
        raise ValueError("County manifest requires geo_slug/geo_name/level or geo_id/market_name/level")
    if not rows.level.str.strip().str.lower().eq("county").all():
        raise ValueError("County publication manifest must contain county rows only")
    if rows.geo_id.str.strip().eq("").any() or rows.geo_id.duplicated().any():
        raise ValueError("County publication manifest has empty or duplicate geography IDs")
    return rows[["geo_id", "market_name"]].sort_values(["market_name", "geo_id"], kind="mergesort").to_dict("records")


def render_county_site(run_dir: Path, counties: list[dict[str, str]], output_dir: Path,
                       axis_registry_path: Path, metric_registry_path: Path,
                       source_registry_path: Path | None = None) -> tuple[Path, Path]:
    if not counties:
        raise ValueError("County publication requires at least one governed county")
    output_dir.mkdir(parents=True, exist_ok=True); county_dir = output_dir / "counties"; county_dir.mkdir(exist_ok=True)
    summaries, files = [], []
    for county in sorted(counties, key=lambda row: (row["market_name"], row["geo_id"])):
        html_path, json_path, snapshot = render_snapshot(run_dir, county["geo_id"], county["market_name"], county_dir,
            axis_registry_path, metric_registry_path, source_registry_path, county_href="../index.html")
        monthly_text, annual_text = _freshness_text(snapshot.freshness)
        summaries.append({"geo_id": county["geo_id"], "market_name": county["market_name"], **snapshot.current,
                          "freshness": f"{monthly_text}; {annual_text}", "href": f"counties/{county['geo_id']}.html"})
        files.extend([html_path, json_path])
    cards = "".join(f'<tr><td><a href="{escape(r["href"])}">{escape(r["market_name"])}</a></td><td>{escape(str(r["minor_regime"]).replace("_", " ").title())}</td><td>{r["demand_score"]:+.2f}</td><td>{r["supply_score"]:+.2f}</td><td>{r["regime_strength"]:.2f}</td><td>{r["as_of_date"]:%b %Y}</td><td>{escape(r["freshness"])}</td></tr>' for r in summaries)
    index_path = output_dir / "index.html"
    index_path.write_text(f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>County Macro Regimes</title><style>body{{font-family:Arial,sans-serif;max-width:1200px;margin:auto;padding:32px;color:#101828}}table{{width:100%;border-collapse:collapse}}th,td{{padding:12px;text-align:left;border-bottom:1px solid #eaecf0}}th{{color:#667085}}a{{color:#175cd3}}@media(max-width:760px){{body{{padding:14px}}.table{{overflow:auto}}table{{min-width:900px}}}}</style></head><body><h1>County Macro Regimes</h1><p>Published from immutable run {escape(run_dir.name)} · {VISUALIZATION_VERSION}</p><div class="table"><table><thead><tr><th>County</th><th>Regime</th><th>Demand</th><th>Supply</th><th>Strength</th><th>As of</th><th>Freshness</th></tr></thead><tbody>{cards}</tbody></table></div></body></html>''', encoding="utf-8", newline="\n")
    files.append(index_path)
    outputs = [{"path": p.relative_to(output_dir).as_posix(), "sha256": _sha256(p), "size_bytes": p.stat().st_size} for p in sorted(files, key=lambda x: x.relative_to(output_dir).as_posix())]
    manifest = {"schema_version": SCHEMA_VERSION, "visualization_version": VISUALIZATION_VERSION,
                "source_run_id": run_dir.name, "source_run_path": str(run_dir),
                "generated_counties": [{"geo_id": x["geo_id"], "market_name": x["market_name"]} for x in summaries],
                "registry_identities": {"axis_registry": {"path": str(axis_registry_path), "sha256": _sha256(axis_registry_path)},
                  "metric_registry": {"path": str(metric_registry_path), "sha256": _sha256(metric_registry_path)},
                  "source_registry": {"path": str(source_registry_path), "sha256": _sha256(source_registry_path)} if source_registry_path else None}, "outputs": outputs}
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n")
    return index_path, manifest_path
