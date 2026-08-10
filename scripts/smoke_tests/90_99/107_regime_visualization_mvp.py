"""Deterministic smoke test for Visualization MVP v0.1."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from regime.pandas_compat import MONTH_END
from visualization.regime_snapshot import REQUIRED_ARTIFACTS, render_snapshot

ROOT = Path(__file__).resolve().parents[3]
GEO_ID = "district_of_columbia_dc__county"


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(run_dir: Path, registry: Path) -> None:
    dates = pd.date_range("2020-01-31", periods=79, freq=MONTH_END)
    demand = np.linspace(-.2, .15, len(dates))
    supply = np.linspace(.1, -.12, len(dates))
    regime = pd.DataFrame({"geo_id": GEO_ID, "date": dates, "major_regime": "expansion", "minor_regime": "early_expansion",
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
    for name, frame in (("regime_assignments", regime), ("coordinates", coordinates), ("axis_scores", axes), ("dimension_scores", dimensions)):
        frame.to_parquet(run_dir / f"{name}.parquet", index=False)
    registry.write_text("axis,dimension,dimension_weight,enabled\n"
                        "demand,demand,0.65,true\ndemand,price,0.175,true\ndemand,affordability,0.075,true\n"
                        "demand,capital_markets,0.10,true\nsupply,supply,0.85,true\nsupply,capital_markets,0.15,true\n"
                        "demand,liquidity,1.0,false\n", encoding="utf-8")


def main() -> int:
    production_registry = ROOT / "config/axis_registry.csv"
    registry_before = _hash(production_registry)
    source = (ROOT / "visualization/regime_snapshot.py").read_text(encoding="utf-8").lower()
    assert "streamlit" not in source and "duckdb" not in source
    with TemporaryDirectory() as temp:
        root = Path(temp)
        run_dir, output_dir = root / "fixture_run", root / "output"
        run_dir.mkdir()
        registry = root / "axis_registry.csv"
        _fixture(run_dir, registry)
        html_path, json_path, snapshot = render_snapshot(run_dir, GEO_ID, "Washington, DC", output_dir, registry)
        assert html_path.is_file() and html_path.stat().st_size > 1_000_000  # inline Plotly runtime
        assert json_path.is_file() and json.loads(json_path.read_text())["geo_id"] == GEO_ID
        assert snapshot.current["as_of_date"] == pd.Timestamp("2026-07-31")
        assert len(snapshot.path) == 12 and len(snapshot.history) == 61
        assert set(snapshot.drivers["demand"].dimension) == {"demand", "price", "affordability", "capital_markets"}
        assert set(snapshot.drivers["supply"].dimension) == {"supply", "capital_markets"}
        assert "liquidity" not in set(pd.concat(snapshot.drivers.values()).dimension)
        contribution = snapshot.drivers["demand"].set_index("dimension").loc["demand"]
        assert np.isclose(contribution.weighted_contribution, contribution.dimension_score * contribution.dimension_weight)
        expected = ("Demand pressure is positive, with Demand the strongest positive contribution, and Capital Markets the strongest negative contribution. "
                    "Supply pressure is negative, and Capital Markets the strongest negative contribution.")
        assert snapshot.explanation == expected
        html = html_path.read_text(encoding="utf-8")
        for anchor in ("current-state", "regime-plane", "why-this-regime", "dimension-drivers", "historical-chronology"):
            assert f'id="{anchor}"' in html
        missing_dir = root / "missing"
        missing_dir.mkdir()
        try:
            render_snapshot(missing_dir, GEO_ID, "Washington, DC", output_dir, registry)
        except FileNotFoundError as error:
            assert all(name in str(error) for name in REQUIRED_ARTIFACTS)
        else:
            raise AssertionError("Missing required artifacts did not fail closed")
    assert _hash(production_registry) == registry_before
    print("[regime_visualization_mvp] standalone rendering and contracts: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
