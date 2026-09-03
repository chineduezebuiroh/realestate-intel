"""Smoke 198: BPS CBSA crosswalk is exact-code-only and read-only."""
from __future__ import annotations

import hashlib
import tempfile
import zipfile
from pathlib import Path

import pandas as pd

from jobs.monthly_refresh.bps_cbsa_verification import (
    canonical_cbsa, compiled_inventory, crosswalk, diagnose, governance_decision,
    history_distribution,
)

assert len(canonical_cbsa(Path("config/geo_manifest.generated.csv"))) == 64
canonical = pd.DataFrame([
    {"provider_cbsa_id": "12345", "canonical_geo_id": "alpha__cbsa_metro", "canonical_geo_name": "Alpha"},
    {"provider_cbsa_id": "23456", "canonical_geo_id": "beta__cbsa_metro", "canonical_geo_name": "Beta"},
])
compiled = pd.DataFrame([
    {"provider_cbsa_id": "12345", "provider_name": "Alpha old name", "observation_date": "2026-04-01", "total_units": 1.0},
    {"provider_cbsa_id": "12345", "provider_name": "Alpha old name", "observation_date": "2026-04-01", "total_units": 1.0},
    {"provider_cbsa_id": "99999", "provider_name": "Beta", "observation_date": "2026-04-01", "total_units": 2.0},
])
provisional = pd.DataFrame([
    {"provider_cbsa_id": "12345", "provider_name": "Completely different", "observation_date": "2026-07-01", "total_units": 3.0},
])
compiled, diagnostics, conflict_keys, conflict_rows = diagnose(compiled, "compiled")
assert diagnostics["identical_duplicate_excess_row_count"] == 1
assert conflict_keys.empty and conflict_rows.empty
assert history_distribution(compiled).iloc[0].observation_count == 1
result = crosswalk(compiled, provisional, canonical).set_index("provider_compiled_cbsa_id")
assert result.loc["12345", "mapping_status"] == "EXACT_MATCH"
assert result.loc["99999", "mapping_status"] == "UNMAPPED"  # matching name has no authority
assert "PROVISIONAL_ONLY" not in set(result.mapping_status)
decision, _ = governance_decision(compiled, provisional, canonical, has_tokens=False)
assert decision == "BLOCK_CBSA"  # canonical 23456 is concretely absent from both parents
assert governance_decision(compiled, pd.concat([provisional, provisional.assign(provider_cbsa_id="23456")]),
                           canonical, has_tokens=False)[0] == "PROMOTE_CBSA"
assert governance_decision(compiled, pd.concat([provisional, provisional.assign(provider_cbsa_id="23456")]),
                           canonical, has_tokens=True)[0] == "CBSA_GOVERNANCE_DECISION_DEFERRED"
conflict = pd.concat([compiled, compiled.iloc[[0]].assign(
    total_units=9.0, provider_raw__name="Alpha alternate")])
usable, conflict_diag, conflict_keys, conflict_rows = diagnose(conflict, "compiled")
assert not ((usable.provider_cbsa_id == "12345") & (usable.observation_date == "2026-04-01")).any()
assert conflict_diag["conflicting_duplicate_key_count"] == 1
assert conflict_diag["conflicting_duplicate_row_count"] == 2
assert conflict_diag["affected_cbsa_identity_count"] == 1
assert set(conflict_rows.total_units) == {1.0, 9.0}
assert "provider_raw__name" in conflict_rows
assert governance_decision(usable, provisional, canonical, has_tokens=False,
                           has_conflicts=True)[0] == "CBSA_GOVERNANCE_DECISION_DEFERRED"

fixture = Path("/tmp/bps_cbsa_ambiguous_manifest.csv")
pd.DataFrame([
    {"level": "cbsa_metro", "census_code": "12345", "geo_slug": "one", "geo_name": "One"},
    {"level": "cbsa_metro", "census_code": "12345", "geo_slug": "two", "geo_name": "Two"},
]).to_csv(fixture, index=False)
try:
    canonical_cbsa(fixture)
except ValueError as exc:
    assert "ambiguous" in str(exc)
else:
    raise AssertionError("ambiguous canonical identity accepted")

catalog = Path("config/artifact_catalog.json")
databases = [Path("data/market_public.duckdb"), Path("data/market_serving.duckdb")]
before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in [catalog, *databases]}
assert {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in before} == before
assert "place" not in set(pd.read_csv("config/bps_governed_geographies_v1.csv").level)
source = Path("jobs/monthly_refresh/bps_cbsa_verification.py").read_text()
for forbidden in ("discover_latest", "artifact_catalog", "duckdb", "redfin", "Source Set", "accepted_pointer"):
    assert forbidden not in source
workflow = Path(".github/workflows/bps-cbsa-contract-verification.yml").read_text()
assert "workflow_dispatch:" in workflow and "schedule:" not in workflow
assert "contents: read" in workflow
job_env = workflow.split("    env:\n", 1)[1].split("    steps:\n", 1)[0]
assert "${{ runner.temp }}" not in job_env  # invalid context makes workflow_dispatch unparsable
assert 'INPUT_ROOT="$RUNNER_TEMP/bps-cbsa-contract-inputs"' in workflow
assert 'EVIDENCE_ROOT="$RUNNER_TEMP/bps-cbsa-contract-evidence"' in workflow
assert "PROVIDER_CONTRACT_REVIEW_REQUIRED" in workflow and "INFRASTRUCTURE_BLOCKER" in workflow
assert "monthly_source_input_pins/$CYCLE_ID" in workflow
assert '.provider_release_id == "202604"' in workflow
assert '.provider_release_id == "2607"' in workflow
assert ".members.compiled_zip.url" in workflow and ".members.cbsa_metro.url" in workflow
assert workflow.count("sha256sum --check --strict") == 2
assert "jobs.monthly_refresh.bps_cbsa_verification" in workflow
for forbidden in ("discover_latest", "artifact_catalog", "duckdb", "redfin", "source-set", "accepted_pointer", "schedule:"):
    assert forbidden not in workflow.lower()

# End-to-end fixture proves semantic conflicts still produce complete evidence.
with tempfile.TemporaryDirectory() as temporary:
    root = Path(temporary)
    compiled_csv = root / "compiled.csv"
    pd.DataFrame([
        {"period": "Monthly", "year": "2026", "month": "4", "location_type": "Metro",
         "state_fips": "", "county_fips": "",
         "cbsa_code": "12345", "total_units": "1", "name": "Alpha", "division": "A"},
        {"period": "Monthly", "year": "2026", "month": "4", "location_type": "Metro",
         "state_fips": "", "county_fips": "",
         "cbsa_code": "12345", "total_units": "9", "name": "Alpha Division", "division": "B"},
    ]).to_csv(compiled_csv, index=False)
    compiled_zip = root / "compiled.zip"
    with zipfile.ZipFile(compiled_zip, "w") as archive:
        archive.write(compiled_csv, compiled_csv.name)
    inventory, raw = compiled_inventory(compiled_zip)
    assert raw["provider_columns"][-2:] == ["name", "division"]
    assert {"provider_raw__name", "provider_raw__division", "provider_row_fingerprint"} <= set(inventory)
    # The run-level evidence contract is asserted without depending on the fixed-width fixture parser.
    evidence = root / "evidence"; evidence.mkdir()
    _, diag, keys, rows = diagnose(inventory, "compiled")
    keys.to_csv(evidence / "compiled_cbsa_conflicting_keys.csv", index=False)
    rows.to_csv(evidence / "compiled_cbsa_conflicting_rows.csv", index=False)
    assert len(pd.read_csv(evidence / "compiled_cbsa_conflicting_keys.csv")) == 1
    assert len(pd.read_csv(evidence / "compiled_cbsa_conflicting_rows.csv")) == 2
    assert "provider_raw__name" in diag["conflict_distinguishing_provider_fields"]
    assert "provider_raw__division" in diag["conflict_distinguishing_provider_fields"]
print("[smoke] BPS CBSA contract verification passed")
