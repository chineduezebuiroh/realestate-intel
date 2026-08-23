SCHEMA_VERSION = "source_artifact_schema_v1"
CONTRACT_VERSION = "source_artifact_contract_v1"
REFRESH_VERSION = "source_refresh_revision_v0_2"
CANONICAL_KEY = ["geo_id", "metric_id", "date", "property_type_id"]
CANONICAL_COLUMNS = ["geo_id", "metric_id", "date", "property_type_id", "value", "source_id", "property_type"]
LINEAGE_COLUMNS = CANONICAL_KEY + ["provider_release_id", "provider_vintage", "source_request_identity", "latest_source_hash_or_drop_id", "source_artifact_id"]
