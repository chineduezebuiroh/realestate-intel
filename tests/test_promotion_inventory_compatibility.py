from copy import deepcopy
import pytest
from core.source_artifacts.hashing import sha256_json
from core.source_artifacts.promotion import (LEGACY_SOURCE_TRANSITION_ORDER, LEGACY_VERSION,
    SOURCE_TRANSITION_ORDER, create_promotion_record, validate_promotion_record)
from core.source_artifacts.publication import PublicationError


def _record(version, order):
    semantic={"schema_version":version,"cycle_id":"cycle","source_set_id":"set",
        "source_set_semantic_sha256":"a"*64,"canonical_artifact_id":"market",
        "canonical_artifact_hash":"b"*64,"expected_source_pointers":{s:None for s in order},
        "target_source_pointers":{s:"src__"+s for s in order},"expected_source_set":None,
        "expected_canonical":None,"readiness_id":"ready","resolution_id":"resolution",
        "operation_order":["accept_source_set","accept_canonical_market","accept_sources","consume_redfin"]}
    return {**semantic,"promotion_id":"cohort_promotion__"+sha256_json(semantic)[:24]}


def test_historical_nine_member_record_retains_v1_identity_and_validates():
    record=_record(LEGACY_VERSION, LEGACY_SOURCE_TRANSITION_ORDER)
    assert validate_promotion_record(deepcopy(record)) == record
    assert "fred_unemp" not in record["target_source_pointers"]


def test_new_ten_member_record_requires_fred_unemp():
    pointers={s:None for s in SOURCE_TRANSITION_ORDER}
    targets={s:"src__"+s for s in SOURCE_TRANSITION_ORDER}
    record=create_promotion_record(cycle_id="cycle",source_set_id="set",
        source_set_semantic_sha256="a"*64,canonical_artifact_id="market",
        canonical_artifact_hash="b"*64,expected_source_pointers=pointers,
        target_source_pointers=targets,expected_source_set=None,expected_canonical=None,
        readiness_id="ready",resolution_id="resolution")
    assert record["schema_version"] == "cohort_promotion_record_v2"
    assert set(record["target_source_pointers"]) == set(SOURCE_TRANSITION_ORDER)
    missing=deepcopy(record); missing["target_source_pointers"].pop("fred_unemp")
    with pytest.raises(PublicationError, match="inventory"):
        validate_promotion_record(missing)
