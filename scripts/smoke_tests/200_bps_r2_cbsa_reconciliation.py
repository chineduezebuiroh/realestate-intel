"""Smoke 200: freeze the immutable-r2 BPS CBSA reconciliation."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


COMPILED = "src__census_bps__2026-04__r2__993afaddb934ce4f"
PROVISIONAL = "src__census_bps_provisional__2026-07__r2__61c56540953237cb"
COMPILED_MISSING = {
    "15700", "17340", "18860", "20660", "21700", "32300", "39780",
    "43760", "45000", "46020", "46380",
}
PROVISIONAL_MISSING = {"15680", "31460", "36140"}

concept_path = Path("config/bps_cbsa_canonical_concepts_v1.csv")
assert hashlib.sha256(concept_path.read_bytes()).hexdigest() == (
    "76007778d36e44e00a7ac83310761ac99175c5ef7d97da7a29cff9c75d88b03c"
)
concepts = pd.read_csv(concept_path, dtype=str)
governed = set(concepts.loc[concepts.bps_compatibility.eq("compatible"), "census_code"])
assert len(governed) == 53
assert not set(concepts.loc[concepts.canonical_concept.eq("metropolitan_division"), "census_code"]) & governed

compiled = governed - COMPILED_MISSING
provisional = governed - PROVISIONAL_MISSING
shared = compiled & provisional
compiled_only = compiled - provisional
provisional_only = provisional - compiled
union = compiled | provisional
absent = governed - union

assert (len(compiled), len(provisional), len(shared), len(compiled_only),
        len(provisional_only), len(union), len(absent)) == (42, 50, 39, 3, 11, 53, 0)
assert shared | compiled_only == compiled
assert shared | provisional_only == provisional
assert len(shared) + len(compiled_only) + len(provisional_only) == len(union)
assert len(union) + len(absent) == 53
assert compiled_only == {"15680", "31460", "36140"}
assert provisional_only == {"15700", "17340", "18860", "20660", "21700",
                            "32300", "39780", "43760", "45000", "46020", "46380"}
assert not absent
assert "09999" not in union

catalog = json.loads(Path("config/artifact_catalog.json").read_text())
records = {item["object_id"]: item for item in catalog["immutable_records"]}
assert records[COMPILED]["artifact_content_hash"] == (
    "993afaddb934ce4f8ea40e14a8e29ce63ddb6c1c743ba1e976b796b185dced4e"
)
assert records[PROVISIONAL]["artifact_content_hash"] == (
    "61c56540953237cb72cc2fec062e9aeb092de411153cd78a250994254004f7ab"
)

# The offline fixture records direct physical membership from the exact immutable
# r2 data.parquet inventories; arithmetic alone is not accepted as physical proof.
fixture = pd.read_csv(
    "scripts/smoke_tests/fixtures/bps_r2_physical_cbsa_inventory.csv",
    dtype=str,
)
assert set(fixture.census_code) == set(concepts.census_code)
physical_compiled = set(fixture.loc[fixture.compiled_present.eq("true"), "census_code"])
physical_provisional = set(fixture.loc[fixture.provisional_present.eq("true"), "census_code"])
assert physical_compiled & governed == compiled
assert physical_provisional & governed == provisional
assert not physical_compiled - governed
assert not physical_provisional - governed

from jobs.monthly_refresh.bps_family_resolution import _cbsa_diagnostics

def physical_frame(column):
    return pd.DataFrame({"geo_id": fixture.loc[fixture[column].eq("true"), "canonical_geo_id"]})

physical_diagnostics = _cbsa_diagnostics(
    physical_frame("compiled_present"),
    physical_frame("provisional_present"),
    concept_path,
)
assert physical_diagnostics["actual_count_tuple"] == [53, 42, 50, 39, 3, 11, 53, 0]
assert physical_diagnostics["compiled_only_codes"] == sorted(compiled_only)
assert physical_diagnostics["provisional_only_codes"] == sorted(provisional_only)
assert physical_diagnostics["absent_from_both_codes"] == []
assert physical_diagnostics["compiled_unsupported_concept_codes"] == []
assert physical_diagnostics["provisional_unsupported_concept_codes"] == []
print("[smoke] immutable-r2 BPS CBSA reconciliation passed")
