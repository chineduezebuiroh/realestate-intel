"""Deterministic smoke test for Visualization MVP v0.2.0."""

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
    REQUIRED_ARTIFACTS, SCHEMA_VERSION, VISUALIZATION_VERSION, _metric_drivers, _plane,
    load_county_manifest, load_metric_memberships, render_county_site, render_snapshot,
)

ROOT = Path(__file__).resolve().parents[3]
GEO_ID = "district_of_columbia_dc__county"


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(run_dir: Path, axis_registry: Path, metric_registry: Path, source_registry: Path) -> None:
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
        "demand": (("labor_force", 1 / 3, .8), ("employment", 1 / 3, .4),
                   ("laus_unemployment_rate", 1 / 3, 0)),
        "market_context": (("population", 1 / 3, .8), ("median_household_income", 1 / 3, .4),
                           ("gdp_annual", 1 / 3, 0)),
        "price": (("median_sale_price", .5, -.5), ("median_ppsf", .5, .1)),
        "capital_markets": (("mortgage_30y", .11666666666666667, -.3),
                            ("mortgage_15y", .11666666666666667, -.3),
                            ("treasury_10y", .11666666666666667, -.3),
                            ("fedfunds", .10, -.3), ("spread_10y_2y", .275, -.3),
                            ("spread_10y_fedfunds", .275, -.3)),
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
    lines = ["metric_key,canonical_metric_key,dimension,metric_weight,demand_block,block_weight,enabled,diagnostic_only,macro_enabled"]
    for dimension, specs in metric_specs.items():
        for key, weight, _ in specs:
            lines.append(f"{key},{key},{dimension},{weight},,,true,false,true")
    lines.append("price_to_income,price_to_income,affordability,0.75,,,true,false,true")
    lines.append("non_member,non_member,liquidity,1.0,,,true,false,true")
    metric_registry.write_text("\n".join(lines) + "\n", encoding="utf-8")
    source_lines = ["metric_key,frequency"]
    for dimension, specs in metric_specs.items():
        for key, _, _ in specs:
            frequency = "annual" if key in {"population", "median_household_income", "gdp_annual"} else "monthly"
            source_lines.append(f"{key},{frequency}")
    source_registry.write_text("\n".join(source_lines) + "\n", encoding="utf-8")


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
        source_registry = root / "source_metric_registry.csv"
        _fixture(run_dir, axis_registry, metric_registry, source_registry)
        html_path, json_path, snapshot = render_snapshot(
            run_dir, GEO_ID, "Washington, DC", output_dir, axis_registry, metric_registry, source_registry)
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
        assert set(demand_rows.canonical_metric_key) == {"labor_force", "employment", "laus_unemployment_rate"}
        assert np.isclose(demand_rows.weighted_metric_contribution.sum(), .4)
        assert np.allclose(demand_rows.effective_metric_weight, 1 / 3)
        assert "market_context" not in snapshot.metric_drivers
        governed = pd.read_csv(metric_registry)
        active_demand = governed.query("dimension == 'demand'")
        assert active_demand.demand_block.isna().all() and active_demand.block_weight.isna().all()
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
        for anchor in ("market-regime", "market-interpretation", "regime-drivers", "market-trajectory", "evidence-detail", "data-methodology"):
            assert f'id="{anchor}"' in html
        assert html.count("<details ") == 6  # Capital Markets appears under both axes.
        assert VISUALIZATION_VERSION == "v0.2.0" and "plotly" in html.lower()
        assert "data-demand-block" not in html
        assert "Structural —" not in html and "Cyclical —" not in html
        assert "Market Context metric evidence" not in html
        payload = json.loads(json_path.read_text())
        assert payload["schema_version"] == SCHEMA_VERSION == "2.0"
        assert payload["visualization_version"] == VISUALIZATION_VERSION
        assert set(payload["cadence_freshness"]) >= {"monthly_indicators", "annual_structural_axis_evidence", "unknown"}
        assert payload["cadence_freshness"]["monthly_indicators"]["metric_count"] > 0
        assert payload["cadence_freshness"]["annual_structural_axis_evidence"]["status"] == "not_applicable"
        assert payload["cadence_freshness"]["annual_structural_axis_evidence"]["metric_count"] == 0
        assert set(payload["interpretation"]) >= {"current_condition", "primary_drivers", "recent_movement"}
        assert payload["provenance"]["axis_registry_sha256"]
        assert "class=\"sticky-nav\"" in html and "@media(max-width:820px)" in html
        assert all(" open" not in tag for tag in html.split("<details")[1:7])
        assert {"metric_weight", "effective_metric_weight", "weighted_metric_contribution"}.issubset(payload["metric_drivers"]["demand"][0])
        assert {"demand_block", "block_weight", "effective_block_weight"}.isdisjoint(payload["metric_drivers"]["demand"][0])

        memberships = load_metric_memberships(metric_registry, {"demand", "price", "affordability", "capital_markets", "supply"})
        latest = pd.Timestamp("2026-07-31")
        governed_scores = {"labor_force": .8, "employment": .4, "laus_unemployment_rate": 0}

        def check_missing(missing: set[str], expected: float) -> pd.DataFrame:
            evidence = pd.DataFrame([{"geo_id": GEO_ID, "date": latest, "canonical_metric_key": key,
                                      "metric_score": score}
                                     for key, score in governed_scores.items() if key not in missing])
            expected_frame = pd.DataFrame([{"dimension": "demand", "dimension_score": expected}])
            rows = _metric_drivers(evidence, memberships[memberships.dimension.eq("demand")], GEO_ID,
                                   latest, expected_frame)["demand"]
            assert np.isclose(rows.weighted_metric_contribution.sum(), expected)
            assert np.isclose(rows.effective_metric_weight.sum(), 1)
            return rows

        missing_demand = check_missing({"laus_unemployment_rate"}, .6)
        assert np.allclose(missing_demand.effective_metric_weight, .5)
        stale_registry = root / "stale_metric_registry.csv"
        stale = pd.read_csv(metric_registry)
        stale.loc[stale.canonical_metric_key.eq("labor_force"), ["demand_block", "block_weight"]] = ["cyclical", .75]
        stale.to_csv(stale_registry, index=False)
        try:
            load_metric_memberships(stale_registry, {"demand", "price", "affordability", "capital_markets", "supply"})
        except ValueError as error:
            assert "superseded demand block metadata" in str(error).lower()
        else:
            raise AssertionError("Stale active Demand block metadata did not fail closed")
        county_manifest = root / "counties.csv"
        county_manifest.write_text("geo_id,market_name,level\n" + GEO_ID + ",Washington DC,county\n", encoding="utf-8")
        counties = load_county_manifest(county_manifest)
        site = root / "site"
        index_path, manifest_path = render_county_site(run_dir, counties, site, axis_registry, metric_registry, source_registry)
        assert index_path.is_file() and f"counties/{GEO_ID}.html" in index_path.read_text()
        manifest = json.loads(manifest_path.read_text())
        assert manifest["generated_counties"] == [{"geo_id": GEO_ID, "market_name": "Washington DC"}]
        assert [row["path"] for row in manifest["outputs"]] == sorted(row["path"] for row in manifest["outputs"])
        for row in manifest["outputs"]:
            assert _hash(site / row["path"]) == row["sha256"]
        first_manifest = manifest_path.read_text()
        render_county_site(run_dir, counties, site, axis_registry, metric_registry, source_registry)
        assert manifest_path.read_text() == first_manifest
        bad_manifest = root / "bad_counties.csv"
        bad_manifest.write_text("geo_id,market_name,level\nmetro,Metro,cbsa_metro\n", encoding="utf-8")
        try:
            load_county_manifest(bad_manifest)
        except ValueError as error:
            assert "county" in str(error).lower()
        else:
            raise AssertionError("Non-county publication did not fail closed")

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
