"""Deterministic smoke test for Visualization MVP v0.1.2."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from regime.pandas_compat import MONTH_END
from regime._08_geometry_engine import _major_regime
from visualization.regime_snapshot import (
    MAJOR_BOUNDARY_DEGREES, MAJOR_REGIMES, PLANE_EXTENT, RADIAL_REFERENCES,
    REQUIRED_ARTIFACTS, _metric_drivers, _plane, load_metric_memberships, render_snapshot,
)

ROOT = Path(__file__).resolve().parents[3]
GEO_ID = "district_of_columbia_dc__county"


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(run_dir: Path, axis_registry: Path, metric_registry: Path) -> None:
    dates = pd.date_range("2020-01-31", periods=79, freq=MONTH_END)
    demand = np.linspace(-.2, .15, len(dates))
    supply = np.linspace(.1, -.12, len(dates))
    major = np.repeat(list(MAJOR_REGIMES), [20, 20, 20, 19])
    minor = [f"mid_{value}" for value in major]
    regime = pd.DataFrame({"geo_id": GEO_ID, "date": dates, "major_regime": major, "minor_regime": minor,
                           "supply_pressure_score": supply, "demand_strength_score": demand,
                           "regime_strength": np.hypot(supply, demand), "max_axis_age_days": 30})
    coordinates = pd.DataFrame({"geo_id": GEO_ID, "date": dates, "x_supply": supply, "y_demand": demand,
                                "radius": np.hypot(supply, demand), "max_axis_age_days": 30})
    axes = pd.concat([pd.DataFrame({"geo_id": GEO_ID, "date": dates, "axis": axis, "axis_score": values})
                      for axis, values in (("demand", demand), ("supply", supply))], ignore_index=True)
    scores = {"demand": .4, "price": -.2, "affordability": .1, "capital_markets": -.3,
              "supply": -.08, "liquidity": .99, "transaction_activity": .88}
    dimensions = pd.DataFrame([{"geo_id": GEO_ID, "date": date, "dimension": dimension, "dimension_score": score}
                               for date in dates for dimension, score in scores.items()])
    metric_specs = {
        "demand": (("population", .1667, .8), ("median_household_income", .1667, .4),
                   ("gdp_annual", .1667, 0), ("labor_force", .1667, .8),
                   ("employment", .1667, .4), ("laus_unemployment_rate", .1667, 0)),
        "price": (("median_sale_price", .5, -.5), ("median_ppsf", .5, .1)),
        "capital_markets": (("mortgage_30y", .5, -.8), ("spread_10y_2y", .5, .2)),
        "affordability": (("payment_burden", .25, .1),),  # second governed metric is missing; effective weight is 1.
        "supply": (("active_inventory", .6, -.2), ("permit_activity", .4, .1)),
    }
    metric_rows = [{"geo_id": GEO_ID, "date": date, "canonical_metric_key": key, "metric_score": score}
                   for date in dates for specs in metric_specs.values() for key, _, score in specs]
    metrics = pd.DataFrame(metric_rows)
    for name, frame in (("regime_assignments", regime), ("coordinates", coordinates), ("axis_scores", axes),
                        ("dimension_scores", dimensions), ("metric_scores", metrics)):
        frame.to_parquet(run_dir / f"{name}.parquet", index=False)
    axis_registry.write_text("axis,dimension,dimension_weight,enabled\n"
                        "demand,demand,0.65,true\ndemand,price,0.175,true\ndemand,affordability,0.075,true\n"
                        "demand,capital_markets,0.10,true\nsupply,supply,0.85,true\nsupply,capital_markets,0.15,true\n"
                        "demand,liquidity,1.0,false\n", encoding="utf-8")
    lines = ["canonical_metric_key,dimension,metric_weight,demand_block,block_weight,enabled,diagnostic_only,macro_enabled"]
    for dimension, specs in metric_specs.items():
        for key, weight, _ in specs:
            block = "structural" if key in {"population", "median_household_income", "gdp_annual"} else "cyclical"
            block_weight = .25 if block == "structural" else .75
            hierarchy = f"{block},{block_weight}" if dimension == "demand" else ","
            lines.append(f"{key},{dimension},{weight},{hierarchy},true,false,true")
    lines.append("price_to_income,affordability,0.75,,,true,false,true")
    lines.append("non_member,liquidity,1.0,,,true,false,true")
    metric_registry.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    production_registry = ROOT / "config/axis_registry.csv"
    registry_before = _hash(production_registry)
    metric_production_registry = ROOT / "config/metric_dimension_registry.csv"
    metric_registry_before = _hash(metric_production_registry)
    source = (ROOT / "visualization/regime_snapshot.py").read_text(encoding="utf-8").lower()
    assert "streamlit" not in source and "duckdb" not in source
    with TemporaryDirectory() as temp:
        root = Path(temp)
        run_dir, output_dir = root / "fixture_run", root / "output"
        run_dir.mkdir()
        axis_registry, metric_registry = root / "axis_registry.csv", root / "metric_dimension_registry.csv"
        _fixture(run_dir, axis_registry, metric_registry)
        html_path, json_path, snapshot = render_snapshot(
            run_dir, GEO_ID, "Washington, DC", output_dir, axis_registry, metric_registry)
        assert html_path.is_file() and html_path.stat().st_size > 1_000_000  # inline Plotly runtime
        assert json_path.is_file() and json.loads(json_path.read_text())["geo_id"] == GEO_ID
        assert snapshot.current["as_of_date"] == pd.Timestamp("2026-07-31")
        assert len(snapshot.path) == 12 and len(snapshot.history) == 61
        assert set(snapshot.drivers["demand"].dimension) == {"demand", "price", "affordability", "capital_markets"}
        assert set(snapshot.drivers["supply"].dimension) == {"supply", "capital_markets"}
        assert "liquidity" not in set(pd.concat(snapshot.drivers.values()).dimension)
        contribution = snapshot.drivers["demand"].set_index("dimension").loc["demand"]
        assert np.isclose(contribution.weighted_contribution, contribution.dimension_score * contribution.dimension_weight)
        expected = ("Demand pressure is positive, with Demand the strongest positive contribution, and Price the strongest negative contribution. "
                    "Supply pressure is negative, and Supply the strongest negative contribution.")
        assert snapshot.explanation == expected
        assert set(snapshot.metric_drivers) == {"demand", "price", "affordability", "capital_markets", "supply"}
        assert all(rows.weighted_metric_contribution.abs().is_monotonic_decreasing for rows in snapshot.metric_drivers.values())
        assert snapshot.metric_drivers["price"].weighted_metric_contribution.lt(0).any()
        assert snapshot.metric_drivers["price"].weighted_metric_contribution.gt(0).any()
        assert "non_member" not in set(pd.concat(snapshot.metric_drivers.values()).canonical_metric_key)
        affordability = snapshot.metric_drivers["affordability"].iloc[0]
        assert affordability.metric_weight == .25 and affordability.effective_metric_weight == 1
        assert np.isclose(affordability.weighted_metric_contribution, .1)
        demand_rows = snapshot.metric_drivers["demand"]
        assert set(demand_rows.demand_block) == {"structural", "cyclical"}
        assert np.isclose(demand_rows.weighted_metric_contribution.sum(), .4)
        assert np.isclose(demand_rows.query("demand_block == 'structural'").weighted_metric_contribution.sum(), .1)
        assert np.isclose(demand_rows.query("demand_block == 'cyclical'").weighted_metric_contribution.sum(), .3)
        expected_dimension_scores = (
            pd.concat(snapshot.drivers.values(), ignore_index=True)
            .drop_duplicates("dimension")
            .set_index("dimension")["dimension_score"]
            .to_dict()
        )

        assert all(
            np.isclose(
                rows.weighted_metric_contribution.sum(),
                expected_dimension_scores[dimension],
            )
            for dimension, rows in snapshot.metric_drivers.items()
        )
        assert len(snapshot.history) == 61 and snapshot.history.date.min() == pd.Timestamp("2021-07-31")
        assert set(snapshot.history.major_regime) == set(MAJOR_REGIMES)
        expected_transitions = snapshot.history[snapshot.history.major_regime.ne(snapshot.history.major_regime.shift())]
        assert snapshot.transitions.date.tolist() == expected_transitions.date.tolist()
        plane = _plane(snapshot)
        assert plane.layout.xaxis.range == (-PLANE_EXTENT, PLANE_EXTENT)
        assert plane.layout.yaxis.range == (-PLANE_EXTENT, PLANE_EXTENT)
        assert plane.layout.yaxis.scaleanchor == "x" and plane.layout.yaxis.scaleratio == 1
        shapes = list(plane.layout.shapes)
        assert sum(shape.name == "radial-reference" for shape in shapes) == len(RADIAL_REFERENCES)
        assert sum(shape.name == "major-regime-boundary" for shape in shapes) == len(MAJOR_BOUNDARY_DEGREES)
        assert {_major_regime(angle) for angle in (0, 90, 180, 270)} == set(MAJOR_REGIMES)
        regime_at_render = snapshot.path.iloc[-1].major_regime
        assert snapshot.current["major_regime"] == regime_at_render  # reference overlays never classify points
        html = html_path.read_text(encoding="utf-8")
        for anchor in ("current-state", "regime-plane", "why-this-regime", "dimension-drivers", "historical-chronology", "major-regime-chronology"):
            assert f'id="{anchor}"' in html
        assert html.count("<details ") == 6  # Capital Markets appears under both axes.
        assert "Visualization MVP v0.1.2" in html and "plotly" in html.lower()
        assert 'data-demand-block="structural"' in html and 'data-demand-block="cyclical"' in html
        payload = json.loads(json_path.read_text())
        assert {"demand_block", "metric_weight", "effective_metric_weight", "block_weight",
                "effective_block_weight", "weighted_metric_contribution"}.issubset(payload["metric_drivers"]["demand"][0])

        memberships = load_metric_memberships(metric_registry, {"demand", "price", "affordability", "capital_markets", "supply"})
        latest = pd.Timestamp("2026-07-31")
        governed_scores = {"population": .8, "median_household_income": .4, "gdp_annual": 0,
                           "labor_force": .8, "employment": .4, "laus_unemployment_rate": 0}
        def check_missing(missing: set[str], expected: float, block_weights: dict[str, float]) -> pd.DataFrame:
            evidence = pd.DataFrame([{"geo_id": GEO_ID, "date": latest, "canonical_metric_key": key,
                                      "metric_score": score}
                                     for key, score in governed_scores.items() if key not in missing])
            expected_frame = pd.DataFrame([{"dimension": "demand", "dimension_score": expected}])
            rows = _metric_drivers(evidence, memberships[memberships.dimension.eq("demand")], GEO_ID,
                                   latest, expected_frame)["demand"]
            assert np.isclose(rows.weighted_metric_contribution.sum(), expected)
            for block, weight in block_weights.items():
                assert np.isclose(rows.query("demand_block == @block").effective_block_weight.iloc[0], weight)
                assert np.isclose(rows.query("demand_block == @block").effective_metric_weight.sum(), 1)
            return rows

        missing_structural = check_missing({"gdp_annual"}, .45, {"structural": .25, "cyclical": .75})
        assert set(missing_structural.query("demand_block == 'structural'").effective_metric_weight) == {.5}
        missing_cyclical = check_missing({"labor_force"}, .25, {"structural": .25, "cyclical": .75})
        assert set(missing_cyclical.query("demand_block == 'cyclical'").effective_metric_weight) == {.5}
        cyclical_only = check_missing({"population", "median_household_income", "gdp_annual"}, .4, {"cyclical": 1})
        assert cyclical_only.demand_block.eq("cyclical").all()
        missing_dir = root / "missing"
        missing_dir.mkdir()
        try:
            render_snapshot(missing_dir, GEO_ID, "Washington, DC", output_dir, axis_registry, metric_registry)
        except FileNotFoundError as error:
            assert all(name in str(error) for name in REQUIRED_ARTIFACTS)
        else:
            raise AssertionError("Missing required artifacts did not fail closed")
    assert _hash(production_registry) == registry_before
    assert _hash(metric_production_registry) == metric_registry_before
    print("[regime_visualization_mvp] standalone rendering and contracts: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
