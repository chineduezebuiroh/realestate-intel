from pathlib import Path

import duckdb
import pandas as pd
import pytest

from regime._00_config_loader import load_regime_config
from regime._01_feature_engine import build_canonical_source_metrics_with_lineage
from regime.serving_input import load_serving_input

GEO = "district_of_columbia_dc__county"


def row(source, metric, value=10, date="2024-12-31", property_type="all", geo=GEO):
    return dict(geo_id=geo, date=pd.Timestamp(date), source_id=source,
                metric_id=metric, value=float(value), property_type_id=property_type)


def database(path, rows):
    frame = pd.DataFrame(rows)
    with duckdb.connect(str(path)) as con:
        con.register("input_rows", frame)
        con.execute("CREATE TABLE fact_timeseries AS SELECT * FROM input_rows")
    return path


def observations(tmp_path, rows):
    path = database(tmp_path / "input.duckdb", rows)
    return load_serving_input(load_regime_config(), path).observations


def test_logical_acs_bps_and_derived_metrics(tmp_path):
    rows = [row("acs", "census_acs_pop_total", 10000),
            row("acs", "census_acs_median_household_income", 100000),
            row("bps", "census_bp_total_units", 20),
            row("redfin", "median_sale_price_nsa", 500000),
            row("fred_macro", "fred_mortgage_30y_avg", 6, geo="united_states__nation")]
    path = database(tmp_path / "logical.duckdb", rows)
    result, lineage = build_canonical_source_metrics_with_lineage(db_path=path)
    indexed = result[result.geo_id.eq(GEO)].set_index("canonical_metric_key")
    for key, value, origin in [("population", 10000, "acs:census_acs_pop_total"),
                               ("median_household_income", 100000, "acs:census_acs_median_household_income"),
                               ("permit_activity", 20, "bps:census_bp_total_units")]:
        assert indexed.loc[key, "value"] == value
        assert indexed.loc[key, "metric_origin"] == origin
        assert indexed.loc[key, "date"] == pd.Timestamp("2024-12-31")
    assert indexed.loc["price_to_income", "value"] == 5
    assert indexed.loc["payment_burden", "value"] > 0
    assert indexed.loc["permit_intensity", "value"] == 2
    assert not result.duplicated(["geo_id", "date", "canonical_metric_key"]).any()
    assert not lineage.empty


def test_legacy_acs_selection_and_compiled_bps(tmp_path):
    result = observations(tmp_path, [row("census_acs1", "census_acs1_pop_total", 11),
        row("census_acs5", "census_acs5_pop_total", 9),
        row("census_acs5", "census_acs5_median_household_income", 22),
        row("census_bps", "census_bp_total_units", 33)])
    indexed = result.set_index("canonical_metric_key")
    assert indexed.loc["population", "value"] == 11
    assert indexed.loc["population", "metric_origin"] == "acs1_population"
    assert indexed.loc["median_household_income", "value"] == 22
    assert indexed.loc["median_household_income", "metric_origin"] == "acs5_median_household_income"
    assert indexed.loc["permit_activity", "value"] == 33


@pytest.mark.parametrize("physical,metric,logical,logical_metric", [
    ("census_acs1", "census_acs1_pop_total", "acs", "census_acs_pop_total"),
    ("census_acs5", "census_acs5_pop_total", "acs", "census_acs_pop_total"),
    ("census_bps", "census_bp_total_units", "bps", "census_bp_total_units"),
    ("census_bps_provisional", "census_bp_total_units", "bps", "census_bp_total_units"),
])
@pytest.mark.parametrize("physical_date", ["2024-12-31", "2020-12-31"])
def test_mixed_family_representations_fail_even_disjoint(tmp_path, physical, metric, logical, logical_metric, physical_date):
    with pytest.raises(ValueError, match="Mixed logical/physical"):
        observations(tmp_path, [row(physical, metric, date=physical_date), row(logical, logical_metric)])


@pytest.mark.parametrize("source,metric", [("acs", "census_acs_pop_total"), ("bps", "census_bp_total_units")])
@pytest.mark.parametrize("property_type", ["all", "condo"])
def test_duplicates_and_property_type_ambiguity_fail(tmp_path, source, metric, property_type):
    with pytest.raises(ValueError, match="Duplicate observation or ambiguous property-type"):
        observations(tmp_path, [row(source, metric), row(source, metric, property_type=property_type)])


@pytest.mark.parametrize("source,metric", [("acs", "unknown"), ("bps", "unknown"),
    ("ces", "census_acs_pop_total")])
def test_unknown_or_contradictory_logical_owner(tmp_path, source, metric):
    with pytest.raises(ValueError, match="Unknown|Contradictory"):
        observations(tmp_path, [row(source, metric)])


def test_logical_authority_contradiction_fails(tmp_path, monkeypatch):
    import regime.serving_input as adapter
    registry = tmp_path / "logical.csv"
    registry.write_text("logical_source_id,metric_id,family_contract_version\nacs,census_acs1_pop_total,census_acs_physical_source_v1\n")
    monkeypatch.setattr(adapter, "LOGICAL_SOURCE_REGISTRY", registry)
    with pytest.raises(ValueError, match="Contradictory direct/logical"):
        observations(tmp_path, [row("acs", "census_acs_pop_total")])


def test_bps_diagnostics_not_activated(tmp_path):
    result = observations(tmp_path, [row("bps", "census_bp_total_units"), row("bps", "census_bp_total_bldgs")])
    assert set(result.canonical_metric_key) == {"permit_activity"}


@pytest.mark.parametrize("value", [float("inf"), float("-inf")])
def test_invalid_values_fail(tmp_path, value):
    with pytest.raises(ValueError, match="Invalid serving date/value"):
        observations(tmp_path, [row("acs", "census_acs_pop_total", value)])


def test_unique_legacy_grain_without_property_type_column(tmp_path):
    record = row("census_bps", "census_bp_total_units", 33)
    del record["property_type_id"]
    result = observations(tmp_path, [record])
    assert result.value.tolist() == [33]


def test_legitimate_temporal_missingness_is_preserved(tmp_path):
    result = observations(tmp_path, [row("bps", "census_bp_total_units", date="2024-01-01"),
        row("bps", "census_bp_total_units", date="2024-07-01")])
    assert set(result.date) == {pd.Timestamp("2024-01-01"), pd.Timestamp("2024-07-01")}


@pytest.mark.parametrize("source,metric", [
    ("laus", "laus_labor_force_sa"),
    ("ces_new", "ces_total_nonfarm_sa"),
    ("fred_macro", "fred_mortgage_15y_new"),
    ("bps_new", "census_bp_total_units"),
])
def test_identity_drift_fails_despite_canonical_coverage(tmp_path, source, metric):
    from test_regime_frozen_input_compatibility import fixture_rows
    rows = fixture_rows(months=2)
    original = {
        "laus": "laus_labor_force_nsa",
        "ces_new": "ces_total_nonfarm_sa",
        "fred_macro": "fred_mortgage_15y_avg",
        "bps_new": "census_bp_total_units",
    }[source]
    changed = False
    for record in rows:
        if record["metric_id"] == original and (source != "laus" or not changed):
            record["source_id"], record["metric_id"] = source, metric
            changed = True
    assert changed
    path = database(tmp_path / "drift.duckdb", rows)
    with pytest.raises(ValueError, match="Unauthorized serving source/metric identity"):
        load_serving_input(load_regime_config(), path, require_scoring_inputs=True)


def test_provisional_only_physical_bps_fails_explicitly(tmp_path):
    with pytest.raises(ValueError, match="Unsupported physical BPS provisional input"):
        observations(tmp_path, [row("census_bps_provisional", "census_bp_total_units")])


def test_registered_diagnostic_and_non_model_inputs_are_not_scored(tmp_path):
    result = observations(tmp_path, [row("fred_unemp", "fred_unemployment_rate_sa"),
        row("fred_macro", "fred_gs30"), row("ces", "ces_total_private_sa"),
        row("bps", "census_bp_total_bldgs"), row("bps", "census_bp_total_units")])
    assert set(result.canonical_metric_key) == {"permit_activity"}


def test_genuine_absence_and_sparse_history_preserve_fallback(tmp_path):
    from test_regime_frozen_input_compatibility import fixture_rows
    rows = [r for r in fixture_rows(months=2)
            if r["source_id"] != "ces" and r["metric_id"] != "fred_mortgage_15y_avg"]
    # Keep required evidence somewhere, without imposing a geography/date grid.
    for record in rows:
        if record["metric_id"] == "laus_labor_force_nsa":
            record["geo_id"] = "united_states__nation"
    rows = [r for r in rows if not (r["metric_id"] == "laus_labor_force_nsa"
                                   and r["date"] == pd.Timestamp("2010-02-28"))]
    path = database(tmp_path / "sparse.duckdb", rows)
    result = load_serving_input(load_regime_config(), path, require_scoring_inputs=True).observations
    assert "mortgage_15y" not in set(result.canonical_metric_key)
    employment = result[result.canonical_metric_key.eq("employment")]
    assert len(employment) == 2
    assert set(employment.metric_origin) == {"laus_employment"}
    labor = result[result.canonical_metric_key.eq("labor_force")]
    assert len(labor) == 1
    assert labor.geo_id.tolist() == ["united_states__nation"]
