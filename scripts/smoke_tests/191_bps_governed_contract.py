"""Smoke 191: governed BPS-A registry, adapter, reconciliation, and artifact safety."""
from __future__ import annotations

import hashlib
import json
import tempfile
from copy import deepcopy
from pathlib import Path

import pandas as pd

from core.source_artifacts import create_artifact
from sources.census_bps.artifact import (
    ADAPTER_CONTRACT_VERSION, CANONICAL_COLUMNS, EXPECTED_GEOGRAPHIES,
    REQUIRED_METRIC, SOURCE_ID, build_request_plan, canonicalize, load_registry,
    reconcile,
)

registry = load_registry()
assert len(registry) == EXPECTED_GEOGRAPHIES == 168
assert len({row["geo_id"] for row in registry}) == 168
assert {row["level"] for row in registry} == {"nation", "state", "county"}
assert {row["metric_id"] for row in registry} == {REQUIRED_METRIC}
assert {row["classification"] for row in registry} == {"GOVERNED_REQUIRED"}
hashes = {"fixture": "a" * 64}
plan = build_request_plan(release_month="2026-04", config_hashes=hashes)
assert plan["endpoint_identity"].endswith("BPS_Compiled_File_202604.zip")
assert "key" not in json.dumps(plan).lower() and plan["acquisition_mode"] == "complete_compiled_snapshot"
assert plan == build_request_plan(release_month="2026-04", config_hashes=hashes)
assert plan["adapter_contract_version"] == ADAPTER_CONTRACT_VERSION

by_geo = {row["geo_id"]: row for row in registry}
nation = next(row for row in registry if row["level"] == "nation")
state = next(row for row in registry if row["level"] == "state")
county = next(row for row in registry if row["level"] == "county")

def provider(row, value, *, year="2026", month="04"):
    item = {"period": "Monthly", "location_type": row["provider_location_type"], "year": year, "month": month, "total_units": value}
    if row["level"] == "state": item["state_fips"] = row["provider_identifier"]
    if row["level"] == "county": item["county_fips"] = row["provider_identifier"]
    return item

observations = [provider(county, "0"), provider(nation, "1,234"), provider(state, None)]
frame, diagnostics = canonicalize(plan, observations)
assert list(frame.columns) == list(CANONICAL_COLUMNS)
assert set(frame.geo_id) == {county["geo_id"], nation["geo_id"]}
assert set(frame.metric_id) == {REQUIRED_METRIC} and set(frame.source_id) == {SOURCE_ID}
assert set(frame.property_type_id) == {"all"} and set(frame.property_type) == {"all"}
assert set(map(str, frame.date)) == {"2026-04-01"}
assert frame.loc[frame.geo_id.eq(county["geo_id"]), "value"].iloc[0] == 0.0
assert frame.loc[frame.geo_id.eq(nation["geo_id"]), "value"].iloc[0] == 1234.0
assert diagnostics["unavailable_observation_count"] == 1 and diagnostics["target_month"] == "2026-04"
reverse, reverse_diag = canonicalize(plan, reversed(observations))
pd.testing.assert_frame_equal(frame, reverse); assert diagnostics == reverse_diag

# Duplicate, unexpected identity, malformed values/date, and structural drift fail closed.
def rejected(rows, message):
    try: canonicalize(plan, rows)
    except ValueError as exc: assert message in str(exc)
    else: raise AssertionError("invalid BPS provider truth was accepted")
rejected([provider(nation, 1), provider(nation, 2)], "duplicate")
rejected([{"period":"Monthly","location_type":"Place","place_fips":"1150000","year":2026,"month":4,"total_units":1}], "unexpected")
rejected([provider(nation, "not-a-number")], "malformed")
rejected([provider(nation, -1)], "invalid")
rejected([provider(nation, 1, month="13")], "date")
rejected([{**provider(nation, 1), "period":"Annual"}], "non-monthly")
drift = deepcopy(plan); drift["adapter_contract_version"] = "drift"
try: canonicalize(drift, [provider(nation, 1)])
except ValueError: pass
else: raise AssertionError("structural provider/request drift was accepted")

# Provider overlap wins, provider-new appends, and prior-only governed truth persists.
prior = pd.DataFrame([
 {"geo_id":nation["geo_id"],"metric_id":REQUIRED_METRIC,"date":pd.Timestamp("2026-03-01").date(),"property_type_id":"all","value":10.,"source_id":SOURCE_ID,"property_type":"all"},
 {"geo_id":nation["geo_id"],"metric_id":REQUIRED_METRIC,"date":pd.Timestamp("2026-04-01").date(),"property_type_id":"all","value":11.,"source_id":SOURCE_ID,"property_type":"all"},
])
reconciled = reconcile(prior, frame[frame.geo_id.eq(nation["geo_id"])])
assert len(reconciled) == 2
assert reconciled.loc[pd.Series(reconciled.date).map(str).eq("2026-03-01"), "value"].iloc[0] == 10.
assert reconciled.loc[pd.Series(reconciled.date).map(str).eq("2026-04-01"), "value"].iloc[0] == 1234.

# Candidate construction is deterministic with fixed provenance and cannot mutate accepted state.
catalog = Path("config/artifact_catalog.json")
before = hashlib.sha256(catalog.read_bytes()).hexdigest()
with tempfile.TemporaryDirectory() as tmp:
    manifests=[]
    for name in ("one", "two"):
        manifests.append(create_artifact(Path(tmp)/name, reconciled, source_id=SOURCE_ID,
          source_family="Census Building Permits Survey", source_type="revisionary_compiled_snapshot",
          provider="U.S. Census Bureau", distribution_channel="BPS compiled master file",
          provider_release_id=plan["provider_release_id"], provider_release_timestamp_or_date=None,
          retrieved_at="2026-05-15T00:00:00Z", artifact_created_at="2026-05-15T00:01:00Z",
          target_month=diagnostics["target_month"], source_request_identity=plan["source_request_identity"],
          source_urls_or_endpoint_identity=[plan["endpoint_identity"]], config_hashes=hashes))
    assert manifests[0]["artifact_id"] == manifests[1]["artifact_id"]
    assert manifests[0]["data_sha256"] == manifests[1]["data_sha256"]
assert hashlib.sha256(catalog.read_bytes()).hexdigest() == before
print("[smoke] governed BPS-A contract passed")
