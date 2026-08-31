"""Smoke 183: deterministic assertions for the proposed LAUS v1 audit inventory."""
from pathlib import Path
import pandas as pd

PATH = Path("config/audit/laus_series_inventory_v0_1.csv")
CONTRACT_PATH = Path("docs/contracts/laus_governed_source_v1.md")
REQUIRED = {
    "laus_labor_force_nsa",
    "laus_employment_nsa",
    "laus_unemployment_rate_nsa",
}
DIAGNOSTIC = {"laus_unemployment_nsa"}


def main() -> None:
    first = PATH.read_bytes()
    frame = pd.read_csv(PATH, dtype=str)
    assert first == PATH.read_bytes()
    assert len(frame) == 840
    assert frame.series_id.is_unique
    assert not frame.isna().any().any()
    governed = frame[frame.classification != "LEGACY_ONLY"]
    assert len(governed) == 820
    assert set(governed.metric_id) == REQUIRED | DIAGNOSTIC
    assert set(governed.seasonal_adjustment) == {"NSA"}
    assert set(governed.groupby("metric_id").size()) == {205}
    assert governed.geo_id.nunique() == 205
    assert governed.groupby("geo_level").geo_id.nunique().to_dict() == {
        "cbsa_metro": 37, "county": 163, "state": 5
    }
    controlling = frame[frame.target_controlling == "true"]
    assert len(controlling) == 615
    assert set(controlling.metric_id) == REQUIRED
    assert set(frame[frame.unit == "percent"].metric_id) == {
        "laus_unemployment_rate_nsa", "laus_unemployment_rate_sa"
    }
    assert set(frame.scale_transform) == {"none"}

    diagnostic = frame[frame.classification == "GOVERNED_DIAGNOSTIC"]
    assert len(diagnostic) == 205
    assert set(diagnostic.metric_id) == DIAGNOSTIC
    assert set(diagnostic.target_controlling) == {"false"}

    contract = CONTRACT_PATH.read_text(encoding="utf-8")
    frozen_clauses = {
        "ordinary bounds": "bounds = end_year - 2 through explicit end_year",
        "ordinary cadence": "cadence = every routine governed monthly refresh",
        "deep bounds": "bounds = 1976 through explicit reviewed end_year",
        "annual cadence": "cadence = at least once per calendar year after the annual LAUS",
        "exceptional review": "-> reviewed registry/methodology update",
        "exceptional full history": "-> explicit full-history reconciliation (1976..reviewed end_year)",
        "bootstrap bounds": "Bootstrap uses `1976..explicit reviewed end_year`",
        "product governance": "repository/product-governance decision",
    }
    missing = {name: clause for name, clause in frozen_clauses.items() if clause not in contract}
    assert not missing, f"Missing frozen LAUS contract clauses: {missing}"
    print("Smoke 183 passed: LAUS v1 inventory and reconciliation policy are frozen.")


if __name__ == "__main__":
    main()
