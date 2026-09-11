from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest
import yaml

from jobs.monthly_refresh.cycle_results import governed_record


WORKFLOW = Path(".github/workflows/acs-monthly-source.yml")


def _workflow() -> dict:
    # PyYAML 1.1 parses the unquoted Actions key `on` as True.
    value = yaml.safe_load(WORKFLOW.read_text())
    value["on"] = value.pop(True)
    return value


def test_manual_dispatch_is_explicit_constrained_and_keeps_main_authority():
    workflow = _workflow()
    dispatch = workflow["on"]["workflow_dispatch"]["inputs"]
    assert dispatch["source_id"]["options"] == ["census_acs1", "census_acs5"]
    assert dispatch["invocation_mode"]["options"] == ["normal", "resume", "replay"]
    assert dispatch["cycle_id"]["required"] is True

    call = workflow["on"]["workflow_call"]
    assert set(call["inputs"]) == {"source_id", "cycle_id", "invocation_mode"}
    assert call["secrets"] == {"CENSUS_API_KEY": {"required": True}}

    job = workflow["jobs"]["execute"]
    assert job["env"]["DURABLE_AUTHORITY_BRANCH"] == "main"
    checkout = next(step for step in job["steps"] if step.get("uses", "").startswith("actions/checkout@"))
    assert "with" not in checkout  # dispatched/calling ref supplies the executable code
    command = next(step["run"] for step in job["steps"] if step.get("id") == "result")
    assert '--branch "$DURABLE_AUTHORITY_BRANCH"' in command
    assert "GITHUB_REF_NAME" not in command


def test_secret_is_runtime_only_and_workflow_has_no_promotion_surface():
    workflow = _workflow()
    result_step = next(step for step in workflow["jobs"]["execute"]["steps"] if step.get("id") == "result")
    assert result_step["env"]["CENSUS_API_KEY"] == "${{ secrets.CENSUS_API_KEY }}"
    assert "CENSUS_API_KEY" not in result_step["run"]

    text = WORKFLOW.read_text()
    forbidden = ("accepted.source", "accepted.source_set", "canonical market", "serving market",
                 "family-resolution", "monthly-refresh-production")
    assert all(token not in text for token in forbidden)
    registry = json.loads(Path("config/monthly_source_execution_registry.json").read_text())
    assert {member["source_id"] for member in registry["members"]}.isdisjoint(
        {"census_acs1", "census_acs5"}
    )


@pytest.mark.parametrize(
    ("flag", "value"), [("--source-id", "census_acs"), ("--mode", "force")]
)
def test_hosted_cli_rejects_unapproved_source_and_mode(flag, value, tmp_path):
    arguments = {
        "--source-id": "census_acs1", "--mode": "normal", "--cycle-id": "cycle",
        "--repository": "owner/repo", "--branch": "main",
        "--workspace": str(tmp_path / "work"), "--output": str(tmp_path / "result.json"),
    }
    arguments[flag] = value
    command = [sys.executable, "-m", "jobs.monthly_refresh.acs_hosted"]
    for name, argument in arguments.items():
        command.extend((name, argument))
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    assert completed.returncode == 2
    assert "invalid choice" in completed.stderr


@pytest.mark.parametrize("source_id", ["census_acs1", "census_acs5"])
def test_slower_cadence_acs_result_is_recordable_without_cohort_membership(source_id):
    policy = json.loads(Path("config/monthly_refresh_policy.json").read_text())
    object_id = f"src__{source_id}__2024-12__r1__" + "a" * 16
    result = {
        "schema_version": "monthly_source_execution_result_v1", "source_id": source_id,
        "cycle_id": "cycle", "status": "succeeded", "candidate_artifact_id": object_id,
        "artifact_content_hash": "a" * 64, "package_sha256": "b" * 64,
        "publication_state": "published_verified", "validation_status": "passed",
        "provider_release_id": f"acs/{'acs1' if source_id.endswith('1') else 'acs5'}:2024",
        "observation_max": "2024-12-31", "prior_artifact_id": None,
        "source_change_detected": True, "retryability": "not_applicable",
        "accepted_pointer_changed": False, "evidence_uri": "artifact://candidate",
    }
    catalog = {"immutable_records": [{
        "object_type": "source", "object_id": object_id,
        "artifact_content_hash": "a" * 64, "package_sha256": "b" * 64,
        "publication_state": "published_immutable_verified",
        "metadata": {"source_id": source_id, "provider_release_id": result["provider_release_id"]},
    }]}
    record = governed_record(result, policy, catalog)
    assert record["source_id"] == source_id
    assert result["accepted_pointer_changed"] is False
