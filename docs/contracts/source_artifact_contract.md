# Governed Source Artifact Contract

## Purpose, status, and versions

A governed source artifact is the durable, immutable unit of canonical state for one source identity at one governed release. It replaces an opaque prior DuckDB as the historical-truth dependency.

* Contract: `source_artifact_contract_v1`
* Artifact schema: `source_artifact_schema_v1`
* Status: design target; no runtime consumer exists yet
* Default granularity: one complete canonical artifact per source identity per governed release

## Logical package

```text
<artifact_id>/
  manifest.json
  data.parquet                 # complete canonical state (default)
  lineage.parquet              # optional only where row groups have differing lineage
  validation.json              # deterministic validation evidence
```

A single deterministic archive may transport the directory, but archive identity is not a storage-provider URL. `data.parquet` uses fixed column order, types, deterministic sorting and governed writer settings.

Required data columns are:

```text
geo_id: string, non-null
metric_id: string, non-null
date: date32, non-null
property_type_id: string, non-null
value: float64, nullable only when a source contract expressly permits it
source_id: string, non-null and constant
property_type: string, nullable/default "all"
```

Canonical uniqueness is `(geo_id, metric_id, date, property_type_id)`. Data is sorted by that key. No local absolute path or storage credential may appear.

## Minimal sufficient lineage

Release-wide lineage belongs in `manifest.json`, not repeated on every row: provider, release/vintage, retrieval time, request identity, endpoint identities, contract/config/Git hashes, prior artifact, and artifact ownership.

When one canonical snapshot contains keys last supplied by different releases—as Redfin preserve-prior state necessarily does—`lineage.parquet` is required. It contains the canonical key plus compact lineage fields:

```text
provider_release_id
provider_vintage
source_request_identity
latest_source_hash_or_drop_id
source_artifact_id                 # artifact that first persisted current value
```

Repeated lineage values may be dictionary encoded. `retrieved_at` remains manifest-level unless it genuinely differs by row lineage. This answers where, which release, when, which request/config, and which artifact without a full bitemporal warehouse.

## Required manifest

```json
{
  "schema_version": "source_artifact_schema_v1",
  "artifact_contract_version": "source_artifact_contract_v1",
  "artifact_id": "src__redfin__2026-08__r1__<content-prefix>",
  "source_id": "redfin",
  "source_family": "Redfin monthly market data",
  "source_type": "rolling_full_snapshot_manual",
  "provider": {"name": "Redfin", "distribution_channel": "manual export"},
  "provider_release_id": "2026-08",
  "provider_release_timestamp_or_date": "2026-08-31",
  "retrieved_at": "2026-09-05T12:00:00Z",
  "target_month": "2026-08",
  "artifact_status": "complete",
  "validation_status": "passed",
  "canonical_key": ["geo_id", "metric_id", "date", "property_type_id"],
  "observation_min": "2012-01-31",
  "observation_max": "2026-08-31",
  "row_count": 652282,
  "geography_count": 372,
  "metric_count": 11,
  "metric_inventory": ["..."],
  "geography_level_inventory": ["..."],
  "source_request_identity": "sha256:<request-and-config-hash>",
  "source_urls_or_endpoint_identity": ["provider-export:redfin:county"],
  "revision_policy_id": "source_refresh_revision_v0_2:redfin",
  "absence_semantics": "preserve_prior",
  "retraction_evidence": null,
  "prior_artifact_id": "...",
  "prior_artifact_sha256": "...",
  "data_filename": "data.parquet",
  "data_sha256": "...",
  "data_size_bytes": 123,
  "lineage_filename": "lineage.parquet",
  "lineage_sha256": "...",
  "schema_identity": "sha256:<canonical-schema-json>",
  "contract_config_identities": {"git_sha": "...", "policy_sha256": "..."},
  "validation_filename": "validation.json",
  "validation_sha256": "...",
  "storage": {
    "storage_backend": "github_release_or_object_store",
    "artifact_uri": "artifact://source/redfin/<artifact_id>",
    "max_single_asset_size_bytes": 0,
    "partition_strategy": "none"
  },
  "warnings": []
}
```

`0` above means the implementation must resolve a governed backend limit before publication; it never means unlimited. Production manifests use a positive configured threshold.

Additional required semantics:

* `provider_release_id` must be provider-native when stable. Otherwise use a deterministic governed surrogate derived from source endpoint identities, declared release period and response hashes; mark the surrogate method.
* `source_request_identity` hashes canonical request parameters, selected-series/query specification, mapping identities, transform version and relevant tracked config hashes.
* Counts and inventories are validation surfaces, not the only acceptance test.
* The manifest itself is canonical JSON (UTF-8, sorted keys, normalized timestamps, no floats for counts). A package/bundle SHA-256 is recorded by the durable-storage index and source-set manifest.
* `artifact_id` is semantic plus a data/lineage content-hash prefix. Full hashes remain authoritative, avoiding circular dependence on a manifest that contains its own storage URI.

## Complete-state semantics and exceptions

Default `artifact_kind=complete_canonical_snapshot`: `data.parquet` represents the complete current governed state for that source after overlap and absence reconciliation. Monthly no-release reuses the prior artifact bytes and ID.

Delta artifacts are exceptional. A source contract may allow them only when it defines an immutable base, ordered delta chain, compaction, maximum chain depth, per-delta validation, and deterministic complete-state materialization. Canonical assembly never interprets deltas directly; a source materializer must first produce/verify complete state.

ACS artifacts preserve immutable vintage identity. Implementations may package multiple governed ACS vintages into a complete source snapshot or reference an immutable set, but a prior vintage's rows and hash cannot be changed.

BPS final and provisional artifacts remain separate. A governed BPS-family resolution artifact may additionally be published; it records both exact parents.

## Immutability and correction

Published `(artifact_id, full content SHA-256)` is immutable. Storage must reject overwrite. A correction creates a new release revision (`r2`, etc.), new ID, new hashes, and `supersedes_artifact_id`; the old artifact remains addressable under retention policy. Mutable aliases such as `latest` are prohibited as production inputs.

Publication is two phase:

1. upload to a unique temporary key and verify all file hashes/schema/validation;
2. create immutable durable object/index record only after verification.

A failed publication never makes an artifact resolvable.

## Storage roles and provider neutrality

| Layer | Allowed role | Prohibited role |
|---|---|---|
| Git | code, contracts, registries, small specs/manifests | material Parquet/raw data/DuckDB |
| Durable artifact storage | immutable canonical source packages and indexes | mutable `latest` authority |
| GitHub Actions artifact | temporary debug/build evidence | durable canonical truth |
| Actions workspace | disposable fetch/reconcile/build | persistence authority |
| GitHub Pages | static public product | source artifact store |

GitHub Releases may be the first durable backend. Consumers resolve storage-neutral `artifact://` references through a configured resolver and verify expected hashes. Assembly must not parse GitHub-specific URLs or tags. Migration to S3/object storage changes resolver configuration, not artifact identity or assembly semantics.

## Size, compression, and partition guardrails

* Default encoding is compressed Parquet (initial implementation should benchmark Zstandard and record writer/version/settings in `schema_identity`).
* Before publication, validate uncompressed rows, compressed bytes, backend limit, configured `max_single_asset_size_bytes`, and expected size-change bounds versus prior artifact.
* If the package exceeds the positive governed threshold, fail `storage_strategy_required`; never silently split.
* An approved deterministic partition strategy lists every partition path/hash/row count in the manifest. Preferred partition order is source-specific stable geography level, then observation-year ranges only if necessary. Partition changes are schema/contract changes.
* One source release must remain one logical artifact even when physically partitioned.

Repository evidence reports 652,282 normalized Redfin rows, 372 geographies and 11 metrics for July 2026. Seven wide raw exports can be multi-GB, but the canonical artifact contains only compact keys, one numeric value and lineage, so compressed Parquet should be orders of magnitude smaller—plausibly tens rather than thousands of MB. Exact size is deliberately **not asserted** without a hosted raw scan. Other governed source scopes are materially smaller, except potentially long BLS histories. Initial implementation must benchmark fixture/accepted local outputs and set the threshold below the selected backend's practical maximum with headroom.

## Artifact resolution and reuse

A durable source-artifact index is an immutable collection of records keyed by exact `artifact_id` and SHA-256. Availability checks may consult provider metadata and the index, but production resolution uses explicit identities only.

On no new release:

* do not rewrite or re-upload;
* select the prior exact artifact;
* record `monthly_status=reused` or `no_new_release_expected` in the source-set manifest;
* preserve the original provider release and artifact hashes.

## Validation contract

Before publication verify canonical schema/types/order, source ID constancy, exact key uniqueness, finite values/source-specific null contract, authorized metrics and geographies, observation bounds, source-specific coverage/revision/absence rules, row-lineage reconciliation, prior-artifact hashes, deterministic rerun hash, and all declared files. Any failure yields no governed artifact.
