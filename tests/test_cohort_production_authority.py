"""Hosted master-cohort production authority boundary regression tests."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml
import pytest

from jobs.monthly_refresh import cohort


CYCLE = "monthly_cycle__2026-08__a9e022a980d29cd7"
CANDIDATE = "src__redfin__2026-08__r1__f2ca39c3c36a9c2b"


def _authority_snapshot(tmp_path: Path) -> Path:
    root = tmp_path / "authority"
    for relative in (cohort.READINESS, cohort.CATALOG, cohort.RESULT_REGISTRY,
                     cohort.POLICY, cohort.EXECUTION_REGISTRY):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(relative.read_bytes())
    readiness_path = root / cohort.READINESS
    readiness = json.loads(readiness_path.read_text())
    july, august = readiness["records"]
    assert july["target_month"] == "2026-07" and august["target_month"] == "2026-08"
    july["consumed"] = True
    august["consumed"] = False
    readiness_path.write_text(json.dumps(readiness))
    return root


def _cli(monkeypatch, capsys, *args: str) -> dict:
    monkeypatch.setattr(sys, "argv", ["cohort", *args])
    assert cohort.main() == 0
    return json.loads(capsys.readouterr().out)


def test_normal_uses_main_snapshot_not_stale_execution_checkout(tmp_path, monkeypatch, capsys):
    # The checked-out fixture deliberately has both records unconsumed, which
    # reproduces the failed execution branch.  The authority snapshot does not.
    local = json.loads(cohort.READINESS.read_text())
    assert [record["consumed"] for record in local["records"]] == [False, False]
    authority = _authority_snapshot(tmp_path)
    output = tmp_path / "cycle.json"

    resolved = _cli(monkeypatch, capsys, "resolve", "--mode", "normal",
                    "--authority-root", str(authority), "--output", str(output))

    assert resolved["cycle_id"] == CYCLE
    assert resolved["redfin_candidate_pin"]["candidate_artifact_id"] == CANDIDATE
    assert resolved["fan_out"] is True
    assert json.loads(output.read_text()) == resolved


def test_resume_and_replay_bind_explicit_identity_to_authority(tmp_path, monkeypatch, capsys):
    authority = _authority_snapshot(tmp_path)
    for mode in ("resume", "replay"):
        output = tmp_path / f"{mode}.json"
        resolved = _cli(monkeypatch, capsys, "resolve", "--mode", mode,
                        "--cycle-id", CYCLE, "--authority-root", str(authority),
                        "--output", str(output))
        assert resolved["cycle_id"] == CYCLE
        assert resolved["redfin_candidate_pin"]["candidate_artifact_id"] == CANDIDATE
        assert resolved["invocation_mode"] == mode
        plan = _cli(monkeypatch, capsys, "resume-plan", "--authority-root", str(authority),
                    "--cycle-json", str(output),
                    "--output", str(tmp_path / f"{mode}-plan.json"))
        if mode == "replay":
            assert plan["reuse"] == []
            assert tuple(plan["run"]) == cohort.REQUIRED_SOURCES


def test_resume_plan_reads_catalog_results_and_inventory_from_authority(
        tmp_path, monkeypatch, capsys):
    authority = _authority_snapshot(tmp_path)
    cycle_path = tmp_path / "cycle.json"
    cycle = _cli(monkeypatch, capsys, "resolve", "--mode", "resume",
                 "--cycle-id", CYCLE, "--authority-root", str(authority),
                 "--output", str(cycle_path))
    plan = _cli(monkeypatch, capsys, "resume-plan", "--authority-root", str(authority),
                "--cycle-json", str(cycle_path), "--output", str(tmp_path / "plan.json"))

    assert plan["reuse"] == ["redfin"]
    assert set(plan["run"]) == set(cohort.REQUIRED_SOURCES) - {"redfin"}
    assert plan["results"][0]["candidate_artifact_id"] == CANDIDATE
    assert cycle["redfin_candidate_pin"]["candidate_artifact_id"] == CANDIDATE


def test_authority_identity_drift_fails_closed(tmp_path, monkeypatch):
    authority = _authority_snapshot(tmp_path)
    catalog_path = authority / cohort.CATALOG
    catalog = json.loads(catalog_path.read_text())
    candidate = next(record for record in catalog["immutable_records"]
                     if record.get("object_id") == CANDIDATE)
    candidate["artifact_content_hash"] = "0" * 64
    catalog_path.write_text(json.dumps(catalog))
    monkeypatch.setattr(sys, "argv", ["cohort", "resolve", "--mode", "normal",
        "--authority-root", str(authority), "--output", str(tmp_path / "cycle.json")])

    with pytest.raises(ValueError, match="Redfin readiness artifact_content_hash mismatch"):
        cohort.main()


def test_workflow_pins_read_only_main_snapshot_for_both_planning_steps():
    workflow = yaml.safe_load(Path(".github/workflows/monthly-refresh-production.yml").read_text())
    job = workflow["jobs"]["resolve-cycle"]
    assert workflow["permissions"] == {"contents": "read"}
    assert job["env"]["DURABLE_AUTHORITY_BRANCH"] == "main"
    authority = next(step for step in job["steps"]
                     if step.get("name") == "Acquire read-only production authority snapshot")
    assert authority["with"] == {"ref": "${{ env.DURABLE_AUTHORITY_BRANCH }}",
                                  "path": "production-authority",
                                  "persist-credentials": False, "fetch-depth": 1}
    commands = "\n".join(step.get("run", "") for step in job["steps"])
    assert commands.count('--authority-root "$GITHUB_WORKSPACE/production-authority"') == 2
