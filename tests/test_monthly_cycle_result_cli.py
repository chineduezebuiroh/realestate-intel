from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from core.source_artifacts.publication import IdentityCollisionError
from jobs.monthly_refresh import cycle_results


CYCLE = "monthly_cycle__2026-09__test"
SOURCE = "ces"
ARTIFACT = "src__ces__2026-09__r1__abc"


def _result(*, artifact_id: str = ARTIFACT) -> dict:
    return {
        "schema_version": "monthly_source_execution_result_v1",
        "source_id": SOURCE,
        "cycle_id": CYCLE,
        "status": "succeeded",
        "candidate_artifact_id": artifact_id,
        "artifact_content_hash": "a" * 64,
        "package_sha256": "b" * 64,
        "publication_state": "published_verified",
        "validation_status": "passed",
        "provider_release_id": "ordinary-current:test",
        "observation_max": "2026-09-30",
        "prior_artifact_id": None,
        "source_change_detected": True,
        "retryability": "not_applicable",
        "evidence_uri": f"artifact://source/ces/{artifact_id}",
        "accepted_pointer_changed": False,
    }


def _catalog(*, artifact_id: str = ARTIFACT) -> dict:
    def record(object_id: str) -> dict:
        return {"object_type": "source", "object_id": object_id,
            "artifact_content_hash": "a" * 64, "package_sha256": "b" * 64,
            "publication_state": "published_immutable_verified",
            "metadata": {"source_id": SOURCE, "provider_release_id": "ordinary-current:test"}}
    return {"immutable_records": [record(artifact_id),
        record("src__ces__2026-09__r2__different")]}


@pytest.fixture
def cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({
        "schema_version": "monthly_refresh_policy_v2",
        "sources": [{"source_id": SOURCE, "acquisition_mode": "automated"}],
    }))
    result = tmp_path / "result.json"
    output = tmp_path / "receipt.json"

    class FakeAPI:
        def __init__(self, repository: str, token: str):
            self.repository = repository

    class FakeCAS:
        def __init__(self, api, path: str, branch: str):
            pass

        def read(self):
            return _catalog(), "catalog-sha"

    class FakeStore:
        existing = None

        def __init__(self, api, branch: str):
            self.last_commit_sha = None

        def put(self, proposed):
            value, changed = cycle_results.add_record(type(self).existing, proposed)
            if changed:
                type(self).existing = value
                self.last_commit_sha = "commit-created-123"
            return value, changed

    monkeypatch.setattr(cycle_results, "GitHubAPI", FakeAPI)
    monkeypatch.setattr(cycle_results, "GitHubCatalogCAS", FakeCAS)
    monkeypatch.setattr(cycle_results, "GitHubCycleResultStore", FakeStore)

    def invoke(value: dict) -> dict:
        result.write_text(json.dumps(value))
        monkeypatch.setattr(sys, "argv", ["cycle_results", "--repository", "owner/repo",
            "--branch", "main", "--token", "test", "--result", str(result),
            "--policy", str(policy), "--output", str(output)])
        assert cycle_results.main() == 0
        return json.loads(output.read_text())

    return invoke, FakeStore


def test_cli_new_result_writes_receipt_with_commit_sha(cli):
    invoke, store = cli
    receipt = invoke(_result())

    assert receipt["record_changed"] is True
    assert receipt["commit_sha"] == "commit-created-123"
    assert receipt["cycle_id"] == CYCLE
    assert receipt["source_id"] == SOURCE
    assert store.existing["result"]["candidate_artifact_id"] == ARTIFACT


def test_cli_exact_repeat_is_valid_idempotent_receipt(cli):
    invoke, store = cli
    first = invoke(_result())
    second = invoke(_result())

    assert first["semantic_identity"] == second["semantic_identity"]
    assert second["record_changed"] is False
    assert second["commit_sha"] is None
    assert store.existing["result"]["candidate_artifact_id"] == ARTIFACT


def test_cli_contradictory_cycle_source_identity_fails_closed(cli):
    invoke, store = cli
    invoke(_result())
    contradictory = _result(artifact_id="src__ces__2026-09__r2__different")

    with pytest.raises(IdentityCollisionError, match="durable cycle-result collision"):
        invoke(contradictory)
    assert store.existing["result"]["candidate_artifact_id"] == ARTIFACT
