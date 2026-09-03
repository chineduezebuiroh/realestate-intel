"""Smoke 198: BPS CBSA crosswalk is exact-code-only and read-only."""
from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd

from jobs.monthly_refresh.bps_cbsa_verification import (
    canonical_cbsa, crosswalk, diagnose, governance_decision, history_distribution,
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
compiled, diagnostics = diagnose(compiled, "compiled")
assert diagnostics["identical_duplicate_excess_row_count"] == 1
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
conflict = pd.concat([compiled, compiled.iloc[[0]].assign(total_units=9.0)])
try:
    diagnose(conflict, "compiled")
except ValueError as exc:
    assert "conflicting duplicate" in str(exc)
else:
    raise AssertionError("ambiguous/conflicting identity accepted")

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
assert "monthly_source_input_pins/$CYCLE_ID" in workflow
assert '.provider_release_id == "202604"' in workflow
assert '.provider_release_id == "2607"' in workflow
assert ".members.compiled_zip.url" in workflow and ".members.cbsa_metro.url" in workflow
assert workflow.count("sha256sum --check --strict") == 2
assert "jobs.monthly_refresh.bps_cbsa_verification" in workflow
for forbidden in ("discover_latest", "artifact_catalog", "duckdb", "redfin", "source-set", "accepted_pointer", "schedule:"):
    assert forbidden not in workflow.lower()
print("[smoke] BPS CBSA contract verification passed")
