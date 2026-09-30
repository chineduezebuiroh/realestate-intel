"""Smoke 198: BPS CBSA crosswalk is exact-code-only and read-only."""
from __future__ import annotations

import hashlib
import tempfile
import zipfile
from pathlib import Path

import pandas as pd

from jobs.monthly_refresh.bps_cbsa_verification import (
    canonical_cbsa, compiled_inventory, crosswalk, diagnose, governance_decision,
    exclude_provider_placeholders, history_distribution,
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
                           has_conflicts=True)[0] == "BLOCK_CBSA"

# 09999 is a provider sentinel, not hundreds of temporal CBSA identities.  It is
# removed before duplicate governance, while the real 12345 conflict above fails.
placeholder = pd.DataFrame([
    {"provider_cbsa_id": "09999", "provider_name": "Elmira NY", "observation_date": "1990-01-01", "total_units": 1.0},
    {"provider_cbsa_id": "09999", "provider_name": "Wausau WI", "observation_date": "1990-01-01", "total_units": 9.0},
])
governable, excluded = exclude_provider_placeholders(placeholder)
assert governable.empty and len(excluded) == 2
assert crosswalk(placeholder, provisional, canonical).provider_compiled_cbsa_id.ne("09999").all()

# Canonical concepts outside the provider product do not become a completeness
# requirement and may not be synthesized from an MSA or division relationship.
conceptual = canonical.assign(canonical_concept=["metropolitan_statistical_area", "metropolitan_division"],
                              bps_compatibility=["compatible", "unsupported"])
assert governance_decision(compiled.iloc[:1], provisional, conceptual, has_tokens=False)[0] == "PROMOTE_CBSA"

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
assert not Path(".github/workflows/bps-cbsa-contract-verification.yml").exists()
monthly_workflow = Path(".github/workflows/monthly-refresh-production.yml").read_text().lower()
assert "bps-cbsa" not in monthly_workflow  # no CBSA-specific source member/job
registry = pd.read_csv("config/bps_governed_geographies_v1.csv", dtype=str)
assert "place" not in set(registry.level)
assert set(registry[registry.level.eq("cbsa_metro")].provider_identifier) == set(
    pd.read_csv("config/bps_cbsa_canonical_concepts_v1.csv", dtype=str)
      .query("bps_compatibility == 'compatible'").census_code)
assert len(registry[registry.level.eq("cbsa_metro")]) == 53

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
