import importlib
import sys

import duckdb
import pytest

from regime._00_config_loader import load_regime_config
from regime.pipeline_runner import run_regime_pipeline
from regime.serving_input import load_serving_input, sha256_file
from test_regime_serving_input import GEO, database, row
from test_regime_frozen_input_compatibility import fixture_rows


def test_configuration_loading_never_opens_database(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("config attempted database access")
    monkeypatch.setattr(duckdb, "connect", forbidden)
    load_regime_config(validate=True)


@pytest.mark.parametrize("default_exists", [False, True])
def test_explicit_database_ignores_default(tmp_path, monkeypatch, default_exists):
    import regime._00_config_loader as loader
    supplied = database(tmp_path / "supplied.duckdb", [row("acs", "census_acs_pop_total", 123)])
    default = tmp_path / "default.duckdb"
    if default_exists:
        database(default, [row("acs", "census_acs_pop_total", 999)])
    monkeypatch.setattr(loader, "SERVING_DB", default)
    real_connect = duckdb.connect
    def exact_only(path, *args, **kwargs):
        assert str(path) == str(supplied)
        assert kwargs.get("read_only") is True
        return real_connect(path, *args, **kwargs)
    monkeypatch.setattr(duckdb, "connect", exact_only)
    result = load_serving_input(load_regime_config(), supplied)
    assert result.observations.value.tolist() == [123]


def test_missing_database_not_created(tmp_path):
    missing = tmp_path / "missing.duckdb"
    with pytest.raises(FileNotFoundError):
        load_serving_input(load_regime_config(), missing)
    assert not missing.exists()


@pytest.mark.parametrize("table", ["other", "fact_timeseries"])
def test_wrong_schema_fails(tmp_path, table):
    path = tmp_path / "wrong.duckdb"
    with duckdb.connect(str(path)) as con:
        con.execute(f"CREATE TABLE {table} (value DOUBLE)")
    with pytest.raises(ValueError, match="Serving schema missing"):
        load_serving_input(load_regime_config(), path)


def test_hash_mismatch_before_any_run_initialization(tmp_path):
    path = database(tmp_path / "input.duckdb", [row("acs", "census_acs_pop_total")])
    root = tmp_path / "artifacts"
    with pytest.raises(ValueError, match="SHA256 mismatch"):
        run_regime_pipeline(run_id="fixture", experiment_id="fixture", artifact_root=root,
                            serving_db_path=path, expected_serving_db_sha256="0" * 64)
    assert not root.exists()


def test_missing_required_inputs_before_initialization(tmp_path):
    path = database(tmp_path / "input.duckdb", [row("acs", "census_acs_pop_total")])
    root = tmp_path / "artifacts"
    with pytest.raises(ValueError, match="Required scoring inputs missing"):
        run_regime_pipeline(run_id="fixture", experiment_id="fixture", artifact_root=root, serving_db_path=path)
    assert not root.exists()


@pytest.mark.parametrize("field", ["serving_db_path", "serving_db_sha256", "expected_serving_db_sha256",
    "input_adapter_version", "logical_source_metric_registry_sha256", "config_hashes", "completed_at_utc"])
def test_reserved_metadata_cannot_override_provenance(tmp_path, field):
    path = database(tmp_path / "input.duckdb", fixture_rows(months=1))
    root = tmp_path / "artifacts"
    with pytest.raises(ValueError, match="reserved provenance"):
        run_regime_pipeline(run_id="fixture", experiment_id="fixture", artifact_root=root,
                            serving_db_path=path, run_metadata={field: "fake"})
    assert not root.exists()


def test_matching_hash_fixture_pipeline_and_existing_runner_smoke(tmp_path, monkeypatch):
    from regime.artifacts import RegimeArtifactStore
    path = database(tmp_path / "input.duckdb", fixture_rows())
    root = tmp_path / "runs"
    expected = sha256_file(path)
    # Any default-database read anywhere in the full scoring path fails here.
    real_connect = duckdb.connect
    def exact_only(database_path, *args, **kwargs):
        assert str(database_path) == str(path)
        assert kwargs.get("read_only") is True
        return real_connect(database_path, *args, **kwargs)
    monkeypatch.setattr(duckdb, "connect", exact_only)
    manifest = run_regime_pipeline(run_id="fixture", experiment_id="fixture", artifact_root=root,
        serving_db_path=path, expected_serving_db_sha256=expected, validation_geo_ids=[GEO],
        run_metadata={"fixture": True})
    assert manifest["status"] == "complete"
    md = manifest["metadata"]
    assert md["serving_db_sha256"] == md["expected_serving_db_sha256"] == expected
    assert md["input_adapter_version"] == "macro_serving_input_v1"
    assert md["logical_source_metric_registry_sha256"] == sha256_file(__import__('pathlib').Path("config/logical_source_metric_registry.csv"))
    assert md["fixture"] is True
    store = RegimeArtifactStore(root)
    smoke = importlib.import_module("scripts.smoke_tests.10_19.16_pipeline_runner")
    monkeypatch.setattr(smoke, "RegimeArtifactStore", lambda: store)
    monkeypatch.setattr(sys, "argv", ["smoke16", "--run-id", "fixture"])
    assert smoke.main() == 0
    with pytest.raises(Exception, match="already exists"):
        run_regime_pipeline(run_id="fixture", experiment_id="fixture", artifact_root=root,
                            serving_db_path=path, expected_serving_db_sha256=expected)


@pytest.mark.parametrize("expected", ["", "no", "a" * 63, "g" * 64])
def test_invalid_expected_hash_fails_before_output(tmp_path, expected):
    path = database(tmp_path / "input.duckdb", [row("acs", "census_acs_pop_total")])
    root = tmp_path / "artifacts"
    with pytest.raises(ValueError, match="64 hexadecimal"):
        run_regime_pipeline(run_id="fixture", experiment_id="fixture", artifact_root=root,
                            serving_db_path=path, expected_serving_db_sha256=expected)
    assert not root.exists()


def test_optional_hash_still_records_independent_digest(tmp_path):
    path = database(tmp_path / "input.duckdb", [row("census_bps", "census_bp_total_units")])
    provenance = load_serving_input(load_regime_config(), path).provenance
    assert provenance["serving_db_sha256"] == sha256_file(path)
    assert provenance["expected_serving_db_sha256"] is None
    assert provenance["logical_source_metric_registry_sha256"] is None


def test_cli_accepts_expected_hash(monkeypatch):
    from scripts.run_regime_pipeline import parse_args
    expected = "a" * 64
    monkeypatch.setattr(sys, "argv", ["pipeline", "--run-id", "fixture", "--experiment-id", "fixture",
                                      "--serving-db", "explicit.duckdb", "--expected-serving-db-sha256", expected])
    args = parse_args()
    assert args.expected_serving_db_sha256 == expected
    assert args.serving_db == "explicit.duckdb"


def test_configuration_still_rejects_unknown_analytical_references():
    from dataclasses import replace
    from regime._00_config_loader import validate_regime_config
    cfg = load_regime_config()
    features = cfg.features.copy()
    features.loc[0, "metric_key"] = "unregistered_metric"
    with pytest.raises(ValueError, match="unknown metric_key"):
        validate_regime_config(replace(cfg, features=features))
