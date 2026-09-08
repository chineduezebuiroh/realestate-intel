"""Smoke 203: hosted cohort-promotion proof remains manual and non-mutating."""
from pathlib import Path

import yaml
from jobs.monthly_refresh.control_plane_migration import SOURCE_BRANCH, TARGET_BRANCH

path=Path(".github/workflows/cohort-promotion-proof.yml")
workflow=yaml.safe_load(path.read_text())
triggers=workflow.get(True,workflow.get("on"))
assert set(triggers)=={"workflow_dispatch"}
assert set(triggers["workflow_dispatch"]["inputs"])=={"cycle_id"}
assert workflow["permissions"]=={"contents":"read"}
text=path.read_text()
assert "scripts/smoke_tests/202_cohort_promotion.py" in text
assert "live promotion performed: false" in text
assert "physical BPS pointers changed: false" in text
assert "Redfin consumed: false" in text
assert "provider discovery performed: false" in text
assert "activate_source" not in text and "schedule:" not in text and "push:" not in text
print("Smoke 203 hosted cohort promotion proof passed")

live_path=Path(".github/workflows/cohort-promotion-live.yml")
live=yaml.safe_load(live_path.read_text()); live_triggers=live.get(True,live.get("on"))
assert set(live_triggers)=={"workflow_dispatch"}
inputs=live_triggers["workflow_dispatch"]["inputs"]
assert set(inputs)=={"cycle_id","intent","confirmation"}
assert live["permissions"]=={"contents":"write"}
live_text=live_path.read_text()
assert "PROMOTE_GOVERNED_COHORT" in live_text and "--live" in live_text
assert "schedule:" not in live_text and "push:" not in live_text
assert "cohort_promotion_hosted" in live_text

# Code executes from the workflow-dispatch ref, while durable control-plane
# reads and CAS writes continue to target the authority branch.
execution_ref="monthly-refresh-orchestration"
authority_branch="main"
steps=live["jobs"]["promote"]["steps"]
checkout=next(step for step in steps if step.get("uses")=="actions/checkout@v4")
assert checkout.get("with",{}).get("ref")=="${{ github.ref_name }}"
assert checkout["with"]["ref"]!="main"
assert execution_ref!=authority_branch
adapter=next(step for step in steps if step.get("name")=="Execute exact hosted adapter")
assert f"--branch {authority_branch}" in adapter["run"]
assert "--branch monthly-refresh-orchestration" not in adapter["run"]
assert 'if [[ "$INTENT" == live ]]' in adapter["run"]
assert 'args+=(--live --confirm "$CONFIRMATION")' in adapter["run"]
assert "--live" not in adapter["run"].split('if [[ "$INTENT" == live ]]')[0]
job_gate=live["jobs"]["promote"]["if"]
assert "inputs.intent == 'preflight'" in job_gate
assert "inputs.intent == 'live'" in job_gate
assert "inputs.confirmation == 'PROMOTE_GOVERNED_COHORT'" in job_gate
print("Smoke 203 manual live cohort workflow passed")

migration_path=Path(".github/workflows/control-plane-migration.yml")
migration=yaml.safe_load(migration_path.read_text())
migration_triggers=migration.get(True,migration.get("on"))
assert set(migration_triggers)=={"workflow_dispatch"}
assert migration["permissions"]=={"contents":"write"}
migration_text=migration_path.read_text()
assert "schedule:" not in migration_text and "push:" not in migration_text
assert 'with: {ref: "${{ github.ref_name }}"}' in migration_text
assert "monthly-refresh-orchestration" not in migration_text  # branches are frozen in the adapter
assert execution_ref==SOURCE_BRANCH and SOURCE_BRANCH!=TARGET_BRANCH and TARGET_BRANCH=="main"
assert "control_plane_migration" in migration_text
assert "MIGRATE_GOVERNED_JULY_CONTROL_PLANE" in migration_text
assert 'if [[ "$INTENT" == live ]]' in migration_text
print("Smoke 203 migration workflow keeps execution, source, and target refs distinct")
