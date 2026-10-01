from __future__ import annotations

import re
from pathlib import Path

from jobs.monthly_refresh.cohort import resolve_invocation


WORKFLOW = Path(".github/workflows/monthly-refresh-production.yml")


def test_schedule_mode_is_structurally_normal_only_and_shared_by_all_callers():
    text = WORKFLOW.read_text()
    expression = "${{ github.event_name == 'schedule' && 'normal' || (inputs.mode || 'normal') }}"
    assert text.count(expression) == 1
    assert "MODE: " + expression in text
    assert "invocation_mode: ${{ steps.resolve.outputs.invocation_mode }}" in text
    assert 'echo "invocation_mode=$(jq -r .invocation_mode' in text

    mode_arguments = re.findall(r"^\s+invocation_mode: (.+)$", text, flags=re.MULTILINE)
    assert mode_arguments
    assert set(mode_arguments) == {"${{ steps.resolve.outputs.invocation_mode }}",
                                   "${{ needs.resolve-cycle.outputs.invocation_mode }}"}
    assert mode_arguments.count("${{ needs.resolve-cycle.outputs.invocation_mode }}") == 10
    assert "invocation_mode: ${{ inputs.mode }}" not in text
    assert "inputs.mode || 'normal'" not in text.replace(expression, "")

    # The schedule side of the sole normalization expression is a literal
    # normal value; resume/replay remain workflow_dispatch choices only.
    schedule_side = expression.split("||", 1)[0]
    assert "'normal'" in schedule_side
    assert "resume" not in schedule_side
    assert "replay" not in schedule_side


def test_master_has_no_promotion_or_authorization_execution_path():
    text = WORKFLOW.read_text()
    assert "cohort-promotion-live" not in text
    assert "serving-market-promotion" not in text
    assert "authorization_token" not in text
    assert text.rstrip().endswith("retention-days: 90")
    assert "Persist create-once Phase 2 handoff on production authority" in text


def test_normal_no_eligible_redfin_remains_no_op(tmp_path: Path):
    policy = tmp_path / "policy.json"
    policy.write_text("policy")
    value = resolve_invocation(mode="normal", policy_path=policy,
        readiness={"schema_version": "monthly_refresh_readiness_v1", "records": []},
        catalog={"schema_version": "artifact_catalog_v1", "accepted": {
            "source": {}, "source_set": None, "canonical_market": None, "serving_market": None},
            "immutable_records": []})

    assert value == {"status": "no_op", "reason": "no_eligible_redfin_catalyst",
                     "fan_out": False, "invocation_mode": "normal"}
