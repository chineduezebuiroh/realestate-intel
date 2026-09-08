"""Smoke 203: hosted cohort-promotion proof remains manual and non-mutating."""
from pathlib import Path

import yaml

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
print("Smoke 203 manual live cohort workflow passed")
