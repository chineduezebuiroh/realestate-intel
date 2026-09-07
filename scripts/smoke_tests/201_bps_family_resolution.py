"""Smoke 201: deterministic full-key BPS logical-family resolution."""
from __future__ import annotations
import copy
import tempfile
from pathlib import Path
import pandas as pd
from jobs.monthly_refresh.bps_family_resolution import (
    ABSENT_CBSA_CODES, EXPECTED_PARENTS, RECORD_VERSION, _cbsa_diagnostics,
    _parent, add_record, resolve_frames,
)

cols=["geo_id","metric_id","date","property_type_id","value","source_id","property_type"]
def frame(rows, source):
    return pd.DataFrame([[g,"census_bp_total_units",pd.Timestamp(d).date(),"all",float(v),source,"all"] for g,d,v in rows],columns=cols)
c=frame([("compiled_only__county","2026-04-01",1),("same__county","2026-04-01",2),
         ("different__county","2026-04-01",3),("temporal__county","2026-04-01",4)],"census_bps")
p=frame([("provisional_only__county","2026-07-01",5),("same__county","2026-04-01",2),
         ("different__county","2026-04-01",99),("temporal__county","2026-07-01",6)],"census_bps_provisional")
out,lineage,d=resolve_frames(c,p)
assert len(out)==6 and set(out.source_id)=={"bps"} and not out.duplicated(["geo_id","metric_id","date","property_type_id"]).any()
assert out.loc[out.geo_id.eq("different__county"),"value"].item()==3
assert set(out.loc[out.geo_id.eq("temporal__county"),"date"])=={pd.Timestamp("2026-04-01").date(),pd.Timestamp("2026-07-01").date()}
assert not any(str(x).startswith("2026-05") or str(x).startswith("2026-06") for x in out.date)
assert (d["compiled_only_key_count"],d["provisional_only_key_count"],d["overlap_key_count"],
        d["overlap_same_value_count"],d["overlap_differing_value_count"],d["compiled_wins_count"])==(2,2,2,1,1,2)
evidence=lineage.loc[lineage.geo_id.eq("different__county")].iloc[0]
assert evidence.compiled_value==3 and evidence.provisional_value==99 and evidence.winning_parent=="compiled"
assert evidence.compiled_artifact_id==EXPECTED_PARENTS["compiled"]["artifact_id"]
for bad in (pd.concat([c,c.iloc[[0]]]), pd.concat([p,p.iloc[[0]].assign(value=100)])):
    try: resolve_frames(bad,p if bad is not p else c)
    except ValueError: pass
    else: raise AssertionError("duplicate parent key accepted")
record={"schema_version":RECORD_VERSION,"resolution_id":"id","accepted_pointer_changed":False,
        "source_set_created":False,"duckdb_mutated":False,"redfin_consumed":False,"provider_discovery_performed":False}
assert add_record(None,record)[1] and not add_record(record,copy.deepcopy(record))[1]
try: add_record(record,{**record,"resolution_id":"other"})
except Exception: pass
else: raise AssertionError("resolution identity collision accepted")
try: _parent({"object_id":"wrong"},Path("never-read"),"compiled")
except ValueError as exc: assert "identity mismatch" in str(exc)
else: raise AssertionError("wrong immutable parent accepted")

# A reconciliation contradiction remains fail-closed and carries complete evidence.
concepts_path = Path("config/bps_cbsa_canonical_concepts_v1.csv")
concepts = pd.read_csv(concepts_path, dtype=str)
compatible_geos = concepts.loc[concepts.bps_compatibility.eq("compatible"), "canonical_geo_id"]
bad_compiled = pd.DataFrame({"geo_id": compatible_geos.iloc[:1]})
bad_provisional = pd.DataFrame({"geo_id": compatible_geos.iloc[:1]})
try:
    _cbsa_diagnostics(bad_compiled, bad_provisional, concepts_path)
except ValueError as exc:
    message = str(exc)
    for field in ("actual_count_tuple", "compiled_cbsa_codes", "provisional_cbsa_codes",
                  "shared_codes", "compiled_only_codes", "provisional_only_codes",
                  "union_codes", "absent_from_both_codes", "expected_count_tuple",
                  "expected_absent_from_both_codes", "compiled_missing_vs_governed_codes",
                  "provisional_missing_vs_governed_codes",
                  "compiled_unsupported_concept_geo_ids",
                  "provisional_unsupported_concept_geo_ids"):
        assert field in message
    assert all(code in message for code in ABSENT_CBSA_CODES)
else:
    raise AssertionError("CBSA reconciliation contradiction did not fail closed")

workflow=Path(".github/workflows/bps-family-resolution.yml").read_text()
master=Path(".github/workflows/monthly-refresh-production.yml").read_text()
assert "workflow_dispatch:" in workflow and "schedule:" not in workflow and "push:" not in workflow
assert "bps-family-resolution" not in master
print("[smoke] BPS family resolution passed")
