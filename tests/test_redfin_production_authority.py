"""Production branch authority tests for the local Redfin producer."""
from __future__ import annotations

import base64
import json
import sys
from unittest.mock import patch

import pytest

from jobs.monthly_refresh import redfin


PRIOR_ID = "src__redfin__2026-07__r1__fixture"
CANDIDATE_ID = "src__redfin__2026-08__r1__fixture"


class CatalogCASFixture:
    branch: str | None = None

    def __init__(self, api, path, branch, fixture=False):
        assert path == redfin.CATALOG_PATH
        assert fixture is False
        type(self).branch = branch

    def read(self):
        return ({
            "accepted": {"source": {"redfin": PRIOR_ID}},
            "immutable_records": [{
                "object_type": "source",
                "object_id": CANDIDATE_ID,
                "package_sha256": "a" * 64,
                "artifact_content_hash": "b" * 64,
                "remote_repository": redfin.REPOSITORY,
                "release_tag": "source-redfin-fixture",
                "release_id": 1,
                "asset_id": 2,
                "asset_filename": "fixture.tar",
                "publication_state": "published_immutable_verified",
                "metadata": {"source_id": "redfin"},
            }],
        }, "catalog-oid")


class GitHubAPIFixture:
    calls: list[tuple[str, str, dict | None]] = []

    def __init__(self, repository, token):
        assert repository == redfin.REPOSITORY
        assert token == "fixture-token"
        type(self).calls = []

    def request(self, method, path, payload=None, expected=(200,)):
        type(self).calls.append((method, path, payload))
        if method == "GET":
            assert expected == (200, 404)
            return None, None
        assert method == "PUT" and expected == (200, 201)
        return {}, None


def _run_main(monkeypatch: pytest.MonkeyPatch, *extra_args: str) -> int:
    def run_fixture(*, catalog, readiness_writer, **kwargs):
        # Candidate publication must not move the accepted source pointer.
        assert catalog["accepted"]["source"]["redfin"] == PRIOR_ID
        changed = readiness_writer({
            "drop_id": "2026-08",
            "drop_content_hash": "c" * 64,
            "target_month": "2026-08",
            "cycle_id": "monthly_cycle__2026-08__fixture",
            "candidate_artifact_id": CANDIDATE_ID,
        })
        assert changed is True
        assert catalog["accepted"]["source"]["redfin"] == PRIOR_ID
        return {"status": "succeeded", "accepted_pointer_changed": False}

    monkeypatch.setattr(redfin, "_gh_token", lambda: "fixture-token")
    monkeypatch.setattr(redfin, "GitHubAPI", GitHubAPIFixture)
    monkeypatch.setattr(redfin, "GitHubCatalogCAS", CatalogCASFixture)
    monkeypatch.setattr(redfin, "add_readiness",
        lambda state, record, **kwargs: ({"schema_version": "monthly_refresh_readiness_v1",
                                          "records": [record]}, True))
    monkeypatch.setattr(redfin, "run", run_fixture)
    monkeypatch.setattr(redfin.subprocess, "run", lambda *a, **k: type("Result", (), {"stdout": "deadbeef\n"})())
    monkeypatch.setattr(sys, "argv", ["redfin", *extra_args])
    return redfin.main()


def test_migration_execution_uses_main_for_catalog_and_readiness(monkeypatch, capsys):
    assert _run_main(monkeypatch) == 0
    assert CatalogCASFixture.branch == "main"
    get_call, put_call = GitHubAPIFixture.calls
    assert get_call[:2] == (
        "GET", "/contents/config%2Fmonthly_refresh_readiness.json?ref=main")
    assert put_call[:2] == ("PUT", "/contents/config%2Fmonthly_refresh_readiness.json")
    assert put_call[2]["branch"] == "main"
    readiness = json.loads(base64.b64decode(put_call[2]["content"]))
    assert readiness["records"][0]["candidate_artifact_id"] == CANDIDATE_ID
    assert json.loads(capsys.readouterr().out)["accepted_pointer_changed"] is False


def test_production_runner_rejects_non_main_authority(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["redfin", "--authority-branch", "other"])
    with pytest.raises(SystemExit, match="2"):
        redfin.main()
