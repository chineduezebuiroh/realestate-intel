"""Reporting cutoff regressions using the existing artifact-only renderer fixture."""
import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest

from scripts import build_macro_regime_site as site
from visualization.regime_snapshot import REQUIRED_ARTIFACTS, resolve_snapshot, render_snapshot

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "visualization_smoke", ROOT / "scripts/smoke_tests/90_99/107_regime_visualization_mvp.py")
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


@pytest.fixture
def evidence(tmp_path):
    run = tmp_path / "fixture_run"
    run.mkdir()
    registries = [tmp_path / name for name in
                  ("axis_registry.csv", "metric_dimension_registry.csv", "source_metric_registry.csv")]
    smoke._fixture(run, *registries)
    # Move the fixture's complete final month to August and add a September
    # whose missing Affordability reproduces the reported selection failure.
    for name in REQUIRED_ARTIFACTS:
        path = run / name
        rows = pd.read_parquet(path)
        rows["date"] = pd.to_datetime(rows.date) + pd.offsets.MonthEnd(1)
        future = rows[rows.date.eq(rows.date.max())].copy()
        future["date"] = pd.Timestamp("2026-09-30")
        if name == "dimension_scores.parquet":
            future = future[future.dimension.ne("affordability")]
        if name == "metric_scores.parquet":
            future["metric_score"] = 99.0
        pd.concat([rows, future], ignore_index=True).to_parquet(path, index=False)
    return run, registries


def test_august_snapshot_and_all_rendered_components(evidence, tmp_path):
    run, registries = evidence
    with pytest.raises(ValueError, match="dimensions are missing"):
        resolve_snapshot(run, smoke.GEO_ID, *registries)
    html, payload, snapshot = render_snapshot(
        run, smoke.GEO_ID, "Washington DC", tmp_path / "site", *registries,
        reporting_month="2026-08")
    cutoff = pd.Timestamp("2026-08-31")
    assert snapshot.current["as_of_date"] == cutoff
    for frame in (snapshot.path, snapshot.history, snapshot.transitions):
        assert frame.date.max() <= cutoff
    for frame in snapshot.metric_drivers.values():
        assert frame.evidence_date.max() <= cutoff
        assert frame.metric_score.abs().max() < 99
    assert snapshot.freshness["latest_evaluation_month"] == "2026-08-31"
    assert snapshot.freshness["monthly_indicators"]["latest_evidence_date"] == "2026-08-31"
    assert "2026-09" not in html.read_text()
    assert "2026-09" not in payload.read_text()
    assert "August 2026" in html.read_text()
    assert json.loads(payload.read_text())["trajectory"]["history_end"] == "2026-08-31"
    # Same persisted August evidence yields identical interpretation and freshness
    # with or without any future rows in the immutable input fixture.
    for name in REQUIRED_ARTIFACTS:
        path = run / name
        rows = pd.read_parquet(path)
        rows[rows.date.le(cutoff)].to_parquet(path, index=False)
    complete = resolve_snapshot(run, smoke.GEO_ID, *registries)
    assert complete.current == snapshot.current
    assert complete.interpretation == snapshot.interpretation
    assert complete.freshness == snapshot.freshness


@pytest.mark.parametrize("month", ["2026-8", "2026-13", "2026-00", "2026-08-31", "", "2027-01"])
def test_invalid_or_unavailable_month(evidence, month):
    run, registries = evidence
    with pytest.raises(ValueError, match="Reporting month"):
        resolve_snapshot(run, smoke.GEO_ID, *registries, reporting_month=month)


def test_incomplete_requested_month_fails(evidence):
    run, registries = evidence
    with pytest.raises(ValueError, match="dimensions are missing"):
        resolve_snapshot(run, smoke.GEO_ID, *registries, reporting_month="2026-09")


def test_build_propagates_month(evidence, tmp_path, monkeypatch):
    run, registries = evidence
    config = tmp_path / "config"
    config.mkdir()
    for registry in registries:
        (config / registry.name).write_bytes(registry.read_bytes())
    monkeypatch.setattr(site, "ROOT", tmp_path)
    index, manifest = site.build(run, tmp_path / "published", reporting_month="2026-08")
    assert "Aug 2026" in index.read_text()
    assert json.loads(manifest.read_text())["source_run_id"] == run.name


def source_set_fixture(tmp_path, observation="2026-08-31", include_redfin=True):
    from core.source_artifacts.source_set_v2 import create_source_set_v2, governed_config_hashes
    artifact = "src__redfin__2026-08__r1__0123456789abcdef"
    entry = {
        "source_id": "redfin", "artifact_id": artifact,
        "logical_artifact_uri": f"artifact://source/redfin/{artifact}",
        "package_sha256": "a" * 64, "artifact_content_hash": "b" * 64,
        "provider_release_id": "download-2026-10-01", "observation_max": observation,
        "validation_status": "passed", "monthly_status": "refreshed",
        "release_tag": f"source-artifact/redfin/{artifact}", "asset_id": 1,
        "publication_receipt_id": "publication_receipt__fixture",
        "cycle_check_succeeded": True, "carried_forward": False,
        "carry_forward_policy_allowed": False,
    }
    return create_source_set_v2(
        tmp_path / "source_set.json", target_month="2026-08", created_at="2026-10-01",
        builder_git_sha="fixture", entries=[entry] if include_redfin else [],
        config_hashes=governed_config_hashes(ROOT))


def test_redfin_observation_month_not_execution_or_vintage(tmp_path):
    from visualization.reporting_month import redfin_reporting_month
    manifest = source_set_fixture(tmp_path)
    assert redfin_reporting_month(manifest, accepted_source_set_id=manifest["source_set_id"]) == "2026-08"
    with pytest.raises(ValueError, match="does not match"):
        redfin_reporting_month(manifest, accepted_source_set_id="another-cycle-source-set")


@pytest.mark.parametrize("observation", ["", "2026-08-32", "2026-08", None])
def test_redfin_invalid_observation_fails(tmp_path, observation):
    from visualization.reporting_month import redfin_reporting_month
    manifest = source_set_fixture(tmp_path, observation)
    with pytest.raises(ValueError, match="observation_max"):
        redfin_reporting_month(manifest, accepted_source_set_id=manifest["source_set_id"])


def test_redfin_missing_or_unvalidated_manifest_fails(tmp_path):
    from core.source_artifacts.publication import PublicationError
    from visualization.reporting_month import redfin_reporting_month
    manifest = source_set_fixture(tmp_path, include_redfin=False)
    with pytest.raises(ValueError, match="exactly one"):
        redfin_reporting_month(manifest, accepted_source_set_id=manifest["source_set_id"])
    manifest = source_set_fixture(tmp_path)
    manifest["sources"][0]["observation_max"] = "2026-09-30"
    with pytest.raises(PublicationError, match="semantic identity"):
        redfin_reporting_month(manifest, accepted_source_set_id=manifest["source_set_id"])


def test_cli_propagates_month(evidence, tmp_path, monkeypatch):
    import sys
    run, registries = evidence
    config = tmp_path / "config"
    config.mkdir()
    for registry in registries:
        (config / registry.name).write_bytes(registry.read_bytes())
    monkeypatch.setattr(site, "ROOT", tmp_path)
    output = tmp_path / "cli-site"
    monkeypatch.setattr(sys, "argv", ["build_macro_regime_site", "--run", str(run),
                                     "--output", str(output), "--reporting-month", "2026-08"])
    assert site.main() == 0
    payload = json.loads((output / "counties" / f"{smoke.GEO_ID}_snapshot.json").read_text())
    assert payload["as_of_date"] == "2026-08-31"
