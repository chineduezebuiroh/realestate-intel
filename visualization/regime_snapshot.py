"""Standalone, artifact-only county macro-regime snapshot renderer."""

from __future__ import annotations

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
REGIME_FILLS = {"expansion": "rgba(18,183,106,.09)", "hypersupply": "rgba(247,144,9,.09)",
                "recession": "rgba(217,45,32,.09)", "recovery": "rgba(46,144,250,.09)"}
# Exact major-sector boundaries governed by regime/_08_geometry_engine.py.
MAJOR_BOUNDARY_DEGREES = (45.0, 135.0, 225.0, 315.0)
RADIAL_REFERENCES = (.25, .50)
PLANE_EXTENT = .60
NATIONAL_GEO_ID = "united_states__nation"


@dataclass
class Snapshot:
    current: dict
    drivers: dict[str, pd.DataFrame]
    metric_drivers: dict[str, pd.DataFrame]
    path: pd.DataFrame
    history: pd.DataFrame
    transitions: pd.DataFrame
    explanation: str


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
    required = {"canonical_metric_key", "dimension", "metric_weight", "enabled", "diagnostic_only", "macro_enabled"}
    if not required.issubset(rows):
        raise ValueError(f"Metric registry is missing columns: {sorted(required - set(rows))}")
    rows = rows[_truthy(rows.enabled) & ~_truthy(rows.diagnostic_only) & _truthy(rows.macro_enabled)].copy()
    rows["dimension"] = rows.dimension.astype(str).str.strip().str.lower()
    rows["canonical_metric_key"] = rows.canonical_metric_key.astype(str).str.strip()
    rows["metric_weight"] = pd.to_numeric(rows.metric_weight, errors="coerce")
    rows = rows[rows.dimension.isin(active_dimensions)]
    conflicts = rows.groupby(["dimension", "canonical_metric_key"]).metric_weight.nunique()
    if (conflicts > 1).any() or rows.metric_weight.isna().any() or (rows.metric_weight < 0).any():
        raise ValueError("Active metric registry has conflicting, non-numeric, or negative weights")
    rows = rows.drop_duplicates(["dimension", "canonical_metric_key"])
    if set(rows.dimension) != active_dimensions or (rows.groupby("dimension").metric_weight.sum() <= 0).any():
        raise ValueError("Every active dimension must have positive governed metric membership")
    return rows[["dimension", "canonical_metric_key", "metric_weight"]].sort_values(["dimension", "canonical_metric_key"]).reset_index(drop=True)


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
    latest_metrics = candidates.groupby("canonical_metric_key", as_index=False).tail(1)
    if latest_metrics.duplicated("canonical_metric_key").any():
        raise ValueError("Metric evidence is not unique after production as-of alignment")
    latest_metrics["metric_score"] = pd.to_numeric(latest_metrics.metric_score, errors="coerce")
    latest_metrics["metric_age_days"] = (latest - latest_metrics.date).dt.days
    result = {}
    for dimension, governed in memberships.groupby("dimension", sort=True):
        rows = governed.merge(latest_metrics[["canonical_metric_key", "metric_score", "metric_age_days"]],
                              on="canonical_metric_key", how="left", validate="one_to_one")
        available = rows.metric_score.notna()
        available_weight = rows.loc[available, "metric_weight"].sum()
        if available_weight <= 0:
            raise ValueError(f"No persisted metric evidence can reconstruct active dimension: {dimension}")
        rows = rows[available].copy()
        rows["effective_metric_weight"] = rows.metric_weight / available_weight
        rows["weighted_metric_contribution"] = rows.metric_score * rows.effective_metric_weight
        expected = float(expected_dimensions.set_index("dimension").loc[dimension, "dimension_score"])
        if not math.isclose(rows.weighted_metric_contribution.sum(), expected, abs_tol=1e-6):
            raise ValueError(f"Persisted metric evidence does not reconcile production dimension: {dimension}")
        rows["display_name"] = rows.canonical_metric_key.str.replace("_", " ").str.title()
        result[dimension] = rows.sort_values("weighted_metric_contribution", key=lambda s: s.abs(), ascending=False, kind="mergesort").reset_index(drop=True)
    return result


def resolve_snapshot(run_dir: Path, geo_id: str, axis_registry_path: Path, metric_registry_path: Path) -> Snapshot:
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
    return Snapshot(current, drivers, metrics, chronology.tail(12).copy(), history, transitions, explanation)


def _plane(snapshot: Snapshot) -> go.Figure:
    path = snapshot.path
    fig = go.Figure()
    sector_specs = ((-45, 45, "hypersupply"), (45, 135, "expansion"), (135, 225, "recovery"), (225, 315, "recession"))
    for start, end, regime in sector_specs:
        angles = [math.radians(start + (end - start) * i / 24) for i in range(25)]
        fig.add_trace(go.Scatter(x=[0] + [PLANE_EXTENT * math.cos(a) for a in angles] + [0],
            y=[0] + [PLANE_EXTENT * math.sin(a) for a in angles] + [0], fill="toself", mode="lines",
            line={"width": 0}, fillcolor=REGIME_FILLS[regime], hoverinfo="skip", showlegend=False))
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
        marker={"size": 7, "color": list(range(len(path))), "colorscale": "Blues", "showscale": False},
        customdata=path[["date", "major_regime", "minor_regime"]],
        hovertemplate="%{customdata[0]|%b %Y}<br>Supply %{x:+.3f}<br>Demand %{y:+.3f}<br>%{customdata[1]} · %{customdata[2]}<extra></extra>"))
    fig.add_trace(go.Scatter(x=[snapshot.current["supply_score"]], y=[snapshot.current["demand_score"]], mode="markers+text",
        marker={"size": 15, "color": "#b42318"}, text=[str(snapshot.current["major_regime"]).upper()], textposition="top center", hoverinfo="skip"))
    fig.update_layout(template="plotly_white", height=500, margin={"l": 55, "r": 25, "t": 20, "b": 50}, showlegend=False,
        xaxis={"title": "Supply pressure", "range": [-PLANE_EXTENT, PLANE_EXTENT], "zeroline": True, "zerolinecolor": "#667085", "constrain": "domain"},
        yaxis={"title": "Demand strength", "range": [-PLANE_EXTENT, PLANE_EXTENT], "zeroline": True, "zerolinecolor": "#667085", "scaleanchor": "x", "scaleratio": 1})
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
    fig.update_layout(template="plotly_white", barmode="overlay", height=155, margin={"l": 55, "r": 25, "t": 10, "b": 40},
        xaxis={"range": [history.date.min(), history.date.max()]}, yaxis={"visible": False, "range": [0, 1]},
        legend={"orientation": "h", "y": 1.25}, bargap=0, showlegend=True)
    return fig


def render_snapshot(run_dir: Path, geo_id: str, market_name: str, output_dir: Path,
                    axis_registry_path: Path, metric_registry_path: Path) -> tuple[Path, Path, Snapshot]:
    snapshot = resolve_snapshot(run_dir, geo_id, axis_registry_path, metric_registry_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    config = {"displayModeBar": False}
    plane = pio.to_html(_plane(snapshot), full_html=False, include_plotlyjs="inline", config=config)
    dimensions = [pio.to_html(_drivers(snapshot.drivers[a], f"{a.title()} drivers"), full_html=False, include_plotlyjs=False, config=config) for a in ("demand", "supply")]
    drilldowns = []
    for axis in ("demand", "supply"):
        blocks = []
        for dimension in snapshot.drivers[axis].dimension:
            chart = pio.to_html(_metric_chart(snapshot.metric_drivers[dimension]), full_html=False, include_plotlyjs=False, config=config)
            blocks.append(f'<details data-axis="{axis}" data-dimension="{dimension}"><summary>{escape(dimension.replace("_", " ").title())} metric drivers</summary>{chart}</details>')
        drilldowns.append(f'<div><h3>{axis.title()} metric drilldowns</h3>{"".join(blocks)}</div>')
    history = pio.to_html(_history(snapshot), full_html=False, include_plotlyjs=False, config=config)
    strip = pio.to_html(_regime_strip(snapshot), full_html=False, include_plotlyjs=False, config=config)
    current = snapshot.current
    html = f'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>{escape(market_name)} County Macro Regime</title>
<style>body{{font-family:Arial,sans-serif;color:#101828;background:#f9fafb;margin:0}}main{{max-width:980px;margin:auto;padding:32px}}section{{background:white;border:1px solid #eaecf0;padding:24px;margin:16px 0}}h1,h2,p{{margin-top:0}}.eyebrow,.fresh{{color:#667085}}.state{{font-size:26px;font-weight:700}}.scores{{display:flex;gap:32px;font-size:18px}}.drivers,.drilldowns{{display:grid;grid-template-columns:1fr 1fr;gap:12px}}details{{border:1px solid #eaecf0;border-radius:6px;margin:8px 0;padding:10px}}summary{{cursor:pointer;font-weight:600}}footer{{font-size:11px;color:#98a2b3}}@media(max-width:700px){{.drivers,.drilldowns{{grid-template-columns:1fr}}.scores{{gap:14px;flex-wrap:wrap}}main{{padding:12px}}}}</style></head><body><main>
<section id="current-state"><p class="eyebrow">County Macro Regime</p><h1>{escape(market_name)}</h1><p>As of {current['as_of_date']:%B %Y}</p><p class="state">{escape(str(current['minor_regime']).replace('_', ' ').upper())}</p><div class="scores"><span>Demand <b>{current['demand_score']:+.2f}</b></span><span>Supply <b>{current['supply_score']:+.2f}</b></span><span>Strength <b>{current['regime_strength']:.2f}</b></span></div><p class="fresh">Oldest contributing axis input: {current['max_axis_age_days']} days</p></section>
<section id="regime-plane"><h2>Regime plane</h2><p class="eyebrow">Governed major-regime sectors, fixed ±{PLANE_EXTENT:.2f} scale, and trailing 12-month trajectory.</p>{plane}</section>
<section id="why-this-regime"><h2>Why this regime?</h2><p>{escape(snapshot.explanation)}</p></section>
<section id="dimension-drivers"><h2>Dimension drivers</h2><div class="drivers">{''.join(dimensions)}</div><div class="drilldowns">{''.join(drilldowns)}</div></section>
<section id="historical-chronology"><h2>Historical chronology</h2><p class="eyebrow">Latest five years of monthly production axis scores and assignments.</p>{history}</section>
<section id="major-regime-chronology"><h2>Major regime chronology</h2><p class="eyebrow">Persisted monthly assignments; dark markers indicate a major-regime transition.</p>{strip}</section>
<footer>Visualization MVP v0.1.1 · Published run: {escape(run_dir.name)}</footer></main></body></html>'''
    html_path, json_path = output_dir / f"{geo_id}.html", output_dir / f"{geo_id}_snapshot.json"
    html_path.write_text(html, encoding="utf-8")
    compact_metrics = {dimension: rows[["canonical_metric_key", "metric_score", "metric_weight", "effective_metric_weight", "weighted_metric_contribution", "metric_age_days"]].to_dict("records") for dimension, rows in snapshot.metric_drivers.items()}
    payload = {"run_id": run_dir.name, "geo_id": geo_id, "market_name": market_name,
               **{key: (value.date().isoformat() if isinstance(value, pd.Timestamp) else value) for key, value in current.items()},
               "demand_drivers": snapshot.drivers["demand"][["dimension", "dimension_score", "dimension_weight", "weighted_contribution"]].to_dict("records"),
               "supply_drivers": snapshot.drivers["supply"][["dimension", "dimension_score", "dimension_weight", "weighted_contribution"]].to_dict("records"),
               "metric_drivers": compact_metrics, "explanation": snapshot.explanation}
    json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return html_path, json_path, snapshot
