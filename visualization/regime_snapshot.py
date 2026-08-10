"""Standalone county macro-regime snapshot renderer."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

REQUIRED_ARTIFACTS = (
    "regime_assignments.parquet", "coordinates.parquet",
    "axis_scores.parquet", "dimension_scores.parquet",
)
REQUIRED_AXES = {"demand", "supply"}


@dataclass
class Snapshot:
    current: dict
    drivers: dict[str, pd.DataFrame]
    path: pd.DataFrame
    history: pd.DataFrame
    explanation: str


def _truthy(values: pd.Series) -> pd.Series:
    return values.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})


def load_axis_memberships(registry_path: Path) -> pd.DataFrame:
    if not registry_path.is_file():
        raise FileNotFoundError(f"Axis registry does not exist: {registry_path}")
    rows = pd.read_csv(registry_path)
    required = {"axis", "dimension", "dimension_weight", "enabled"}
    if not required.issubset(rows.columns):
        raise ValueError(f"Axis registry is missing columns: {sorted(required - set(rows.columns))}")
    rows = rows[_truthy(rows.enabled)].copy()
    rows["axis"] = rows.axis.astype(str).str.strip().str.lower()
    rows["dimension"] = rows.dimension.astype(str).str.strip().str.lower()
    rows["dimension_weight"] = pd.to_numeric(rows.dimension_weight, errors="coerce")
    if set(rows.axis) != REQUIRED_AXES or rows.empty:
        raise ValueError("Active axis registry must resolve exactly Demand and Supply")
    if rows.duplicated(["axis", "dimension"]).any() or rows.dimension_weight.isna().any() or (rows.dimension_weight <= 0).any():
        raise ValueError("Active axis registry has duplicate, non-numeric, or non-positive memberships")
    sums = rows.groupby("axis").dimension_weight.sum()
    if not sums.map(lambda value: abs(value - 1.0) <= .001).all():
        raise ValueError("Active dimension weights must sum to 1.0 for each axis")
    return rows[["axis", "dimension", "dimension_weight"]].sort_values(["axis", "dimension"]).reset_index(drop=True)


def _read_artifacts(run_dir: Path) -> dict[str, pd.DataFrame]:
    if not run_dir.is_dir():
        raise FileNotFoundError(f"Run directory does not exist: {run_dir}")
    missing = [name for name in REQUIRED_ARTIFACTS if not (run_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Required run artifacts are missing: {', '.join(missing)}")
    tables = {name.removesuffix(".parquet"): pd.read_parquet(run_dir / name) for name in REQUIRED_ARTIFACTS}
    for name, table in tables.items():
        required = {"geo_id", "date"}
        if not required.issubset(table.columns):
            raise ValueError(f"{name}.parquet is missing columns: {sorted(required - set(table.columns))}")
        table["date"] = pd.to_datetime(table.date)
    return tables


def _sentence(axis: str, rows: pd.DataFrame) -> str:
    positive = rows[rows.weighted_contribution > 0].sort_values("weighted_contribution", ascending=False)
    negative = rows[rows.weighted_contribution < 0].sort_values("weighted_contribution")
    score_word = "positive" if rows.weighted_contribution.sum() >= 0 else "negative"
    parts = [f"{axis.title()} pressure is {score_word}"]
    if not positive.empty:
        parts.append(f"with {positive.iloc[0].display_name} the strongest positive contribution")
    if not negative.empty:
        parts.append(f"and {negative.iloc[0].display_name} the strongest negative contribution")
    return ", ".join(parts) + "."


def resolve_snapshot(run_dir: Path, geo_id: str, registry_path: Path) -> Snapshot:
    tables = _read_artifacts(run_dir)
    memberships = load_axis_memberships(registry_path)
    filtered = {name: table[table.geo_id.eq(geo_id)].copy() for name, table in tables.items()}
    if filtered["regime_assignments"].empty:
        raise ValueError(f"Geography has no regime chronology: {geo_id}")
    common_dates = set(filtered["regime_assignments"].date)
    for name in ("coordinates", "axis_scores"):
        common_dates &= set(filtered[name].date)
    if not common_dates:
        raise ValueError("No common production regime, coordinate, and axis-score date")
    latest = max(common_dates)
    regime_rows = filtered["regime_assignments"][filtered["regime_assignments"].date.eq(latest)]
    coordinate_rows = filtered["coordinates"][filtered["coordinates"].date.eq(latest)]
    axis_rows = filtered["axis_scores"][filtered["axis_scores"].date.eq(latest)]
    if len(regime_rows) != 1 or len(coordinate_rows) != 1:
        raise ValueError("Latest regime row must have exactly one matching coordinate")
    latest_axes = axis_rows[axis_rows.axis.astype(str).str.lower().isin(REQUIRED_AXES)].copy()
    if set(latest_axes.axis.astype(str).str.lower()) != REQUIRED_AXES or latest_axes.duplicated("axis").any():
        raise ValueError("Latest regime date must have exactly one Demand and Supply axis score")

    dims = filtered["dimension_scores"]
    dims = dims[dims.date.eq(latest)].copy()
    dims["dimension"] = dims.dimension.astype(str).str.strip().str.lower()
    active_names = set(memberships.dimension)
    active = dims[dims.dimension.isin(active_names)]
    if active.duplicated("dimension").any():
        raise ValueError("Duplicate active dimension rows exist for the geography/date")
    if set(active.dimension) != active_names or active.dimension_score.isna().any():
        raise ValueError("Weighted active-axis dimensions are missing at the latest date")
    drivers: dict[str, pd.DataFrame] = {}
    for axis in sorted(REQUIRED_AXES):
        rows = memberships[memberships.axis.eq(axis)].merge(active[["dimension", "dimension_score"]], on="dimension", validate="one_to_one")
        rows["weighted_contribution"] = rows.dimension_score.astype(float) * rows.dimension_weight
        rows["display_name"] = rows.dimension.str.replace("_", " ").str.title()
        drivers[axis] = rows
    explanation = " ".join(_sentence(axis, drivers[axis]) for axis in ("demand", "supply"))

    regime = regime_rows.iloc[0]
    coordinate = coordinate_rows.iloc[0]
    scores = latest_axes.assign(axis=latest_axes.axis.str.lower()).set_index("axis").axis_score
    current = {
        "as_of_date": latest, "major_regime": regime.major_regime, "minor_regime": regime.minor_regime,
        "demand_score": float(scores.demand), "supply_score": float(scores.supply),
        "regime_strength": float(regime.regime_strength), "max_axis_age_days": int(coordinate.max_axis_age_days),
    }
    chronology = filtered["regime_assignments"].sort_values("date")
    chronology = chronology[chronology.date <= latest]
    path = chronology.tail(12).copy()
    cutoff = latest - pd.DateOffset(years=5) + pd.offsets.MonthEnd(0)
    history = chronology[chronology.date >= cutoff].copy()
    return Snapshot(current, drivers, path, history, explanation)


def _plane(snapshot: Snapshot) -> go.Figure:
    path = snapshot.path
    extent = max(.25, float(path[["supply_pressure_score", "demand_strength_score"]].abs().max().max()) * 1.25)
    fig = go.Figure(go.Scatter(x=path.supply_pressure_score, y=path.demand_strength_score, mode="lines+markers",
        marker={"size": 7, "color": list(range(len(path))), "colorscale": "Blues", "showscale": False},
        customdata=path[["date", "major_regime", "minor_regime"]],
        hovertemplate="%{customdata[0]|%b %Y}<br>Supply %{x:+.3f}<br>Demand %{y:+.3f}<br>%{customdata[1]} · %{customdata[2]}<extra></extra>"))
    fig.add_trace(go.Scatter(x=[snapshot.current["supply_score"]], y=[snapshot.current["demand_score"]], mode="markers+text",
        marker={"size": 15, "color": "#b42318"}, text=[str(snapshot.current["minor_regime"]).replace("_", " ").title()], textposition="top center", hoverinfo="skip"))
    fig.update_layout(template="plotly_white", height=470, margin={"l": 55, "r": 25, "t": 20, "b": 50}, showlegend=False,
        xaxis={"title": "Supply", "range": [-extent, extent], "zeroline": True, "zerolinecolor": "#667085", "constrain": "domain"},
        yaxis={"title": "Demand", "range": [-extent, extent], "zeroline": True, "zerolinecolor": "#667085", "scaleanchor": "x", "scaleratio": 1})
    return fig


def _drivers(rows: pd.DataFrame, title: str) -> go.Figure:
    rows = rows.sort_values("weighted_contribution")
    fig = go.Figure(go.Bar(x=rows.weighted_contribution, y=rows.display_name, orientation="h",
        marker_color=["#b42318" if x < 0 else "#175cd3" for x in rows.weighted_contribution],
        customdata=rows[["dimension_score", "dimension_weight"]],
        hovertemplate="%{y}<br>Raw score %{customdata[0]:+.3f}<br>Weight %{customdata[1]:.3f}<br>Contribution %{x:+.3f}<extra></extra>"))
    fig.update_layout(template="plotly_white", title={"text": title, "font": {"size": 16}}, height=230, margin={"l": 120, "r": 20, "t": 45, "b": 35},
        xaxis={"title": "Weighted contribution", "zeroline": True, "zerolinecolor": "#667085"}, yaxis={"title": ""}, showlegend=False)
    return fig


def _history(snapshot: Snapshot) -> go.Figure:
    history = snapshot.history
    custom = history[["major_regime", "minor_regime"]]
    fig = go.Figure()
    for column, name, color in (("demand_strength_score", "Demand", "#175cd3"), ("supply_pressure_score", "Supply", "#b54708")):
        fig.add_trace(go.Scatter(x=history.date, y=history[column], name=name, mode="lines", line={"color": color}, customdata=custom,
            hovertemplate="%{x|%b %Y}<br>" + name + " %{y:+.3f}<br>%{customdata[0]} · %{customdata[1]}<extra></extra>"))
    fig.update_layout(template="plotly_white", height=340, margin={"l": 55, "r": 25, "t": 20, "b": 45}, hovermode="x unified",
        yaxis={"title": "Axis score", "zeroline": True, "zerolinecolor": "#667085"}, legend={"orientation": "h", "y": 1.08})
    return fig


def render_snapshot(run_dir: Path, geo_id: str, market_name: str, output_dir: Path, registry_path: Path) -> tuple[Path, Path, Snapshot]:
    snapshot = resolve_snapshot(run_dir, geo_id, registry_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    current = snapshot.current
    fragments = [pio.to_html(_plane(snapshot), full_html=False, include_plotlyjs="inline", config={"displayModeBar": False}),
                 pio.to_html(_drivers(snapshot.drivers["demand"], "Demand drivers"), full_html=False, include_plotlyjs=False, config={"displayModeBar": False}),
                 pio.to_html(_drivers(snapshot.drivers["supply"], "Supply drivers"), full_html=False, include_plotlyjs=False, config={"displayModeBar": False}),
                 pio.to_html(_history(snapshot), full_html=False, include_plotlyjs=False, config={"displayModeBar": False})]
    html = f'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>{market_name} County Macro Regime</title>
<style>body{{font-family:Arial,sans-serif;color:#101828;background:#f9fafb;margin:0}}main{{max-width:980px;margin:auto;padding:32px}}section{{background:white;border:1px solid #eaecf0;padding:24px;margin:16px 0}}h1,h2,p{{margin-top:0}}.eyebrow,.fresh{{color:#667085}}.state{{font-size:26px;font-weight:700}}.scores{{display:flex;gap:32px;font-size:18px}}.drivers{{display:grid;grid-template-columns:1fr 1fr;gap:12px}}footer{{font-size:11px;color:#98a2b3}}@media(max-width:700px){{.drivers{{grid-template-columns:1fr}}.scores{{gap:14px;flex-wrap:wrap}}main{{padding:12px}}}}</style></head><body><main>
<section id="current-state"><p class="eyebrow">County Macro Regime</p><h1>{market_name}</h1><p>As of {current['as_of_date']:%B %Y}</p><p class="state">{str(current['minor_regime']).replace('_', ' ').upper()}</p><div class="scores"><span>Demand <b>{current['demand_score']:+.2f}</b></span><span>Supply <b>{current['supply_score']:+.2f}</b></span><span>Strength <b>{current['regime_strength']:.2f}</b></span></div><p class="fresh">Oldest contributing axis input: {current['max_axis_age_days']} days</p></section>
<section id="regime-plane"><h2>Regime plane</h2><p class="eyebrow">Trailing 12 months; color progresses from earlier to current.</p>{fragments[0]}</section>
<section id="why-this-regime"><h2>Why this regime?</h2><p>{snapshot.explanation}</p></section>
<section id="dimension-drivers"><h2>Dimension drivers</h2><div class="drivers">{fragments[1]}{fragments[2]}</div></section>
<section id="historical-chronology"><h2>Historical chronology</h2><p class="eyebrow">Latest five years of monthly production assignments.</p>{fragments[3]}</section>
<footer>Visualization MVP v0.1 · Published run: {run_dir.name}</footer></main></body></html>'''
    html_path = output_dir / f"{geo_id}.html"
    json_path = output_dir / f"{geo_id}_snapshot.json"
    html_path.write_text(html, encoding="utf-8")
    payload = {"run_id": run_dir.name, "geo_id": geo_id, "market_name": market_name,
               **{key: (value.date().isoformat() if isinstance(value, pd.Timestamp) else value) for key, value in current.items()},
               "demand_drivers": snapshot.drivers["demand"][["dimension", "dimension_score", "dimension_weight", "weighted_contribution"]].to_dict("records"),
               "supply_drivers": snapshot.drivers["supply"][["dimension", "dimension_score", "dimension_weight", "weighted_contribution"]].to_dict("records"), "explanation": snapshot.explanation}
    json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    if not html_path.is_file() or html_path.stat().st_size == 0:
        raise OSError(f"Output HTML could not be generated: {html_path}")
    return html_path, json_path, snapshot
