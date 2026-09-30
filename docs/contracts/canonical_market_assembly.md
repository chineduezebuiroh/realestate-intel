# Canonical Market Assembly Contract

## Purpose, status, and boundary

The canonical assembler deterministically transforms an explicit verified source set into a candidate `market.duckdb`. It contains no provider fetch code and no provider-specific revision, absence, vintage, or reconciliation logic.

* Assembly contract: `canonical_market_assembly_v1`
* Source-set schema: `source_set_manifest_v1`
* Input artifact schema: `source_artifact_schema_v1`
* Status: design target; not implemented or runtime-consumed

The boundary is strict:

```text
provider acquisition + source-specific reconciliation
→ immutable complete canonical source artifacts
→ optional provider-family resolver (BPS)
→ generic canonical assembler
→ candidate market.duckdb
→ existing serving candidate builder
→ market_serving.duckdb
```

The serving builder remains downstream packaging. It must never fetch providers or reconcile source history.

## Source-set manifest

The sole production input is an immutable, canonical-JSON source-set manifest. Neither `latest`, storage listing order, directory discovery, nor mutable tags are authority.

```json
{
  "schema_version": "source_set_manifest_v1",
  "source_set_id": "source_set__2026-08__v1__<content-prefix>",
  "target_month": "2026-08",
  "created_at": "2026-09-05T14:00:00Z",
  "source_refresh_revision_contract_version": "source_refresh_revision_v0_2",
  "source_artifact_schema_version": "source_artifact_schema_v1",
  "canonical_market_assembly_contract_version": "canonical_market_assembly_v1",
  "required_source_inventory": [
    "redfin", "ces", "laus", "census_bps", "census_bps_provisional",
    "census_acs1", "census_acs5", "fred_macro", "fred_unemp",
    "bea_gdp_qtr", "bea_gdp_ann", "census_nrc_fred"
  ],
  "sources": {
    "redfin": {
      "source_id": "redfin",
      "artifact_id": "src__redfin__2026-08__r1__...",
      "artifact_sha256": "...",
      "artifact_uri": "artifact://source/redfin/...",
      "provider_release_id": "2026-08",
      "observation_max": "2026-08-31",
      "monthly_status": "refreshed",
      "check_status": "new_release",
      "validation_status": "passed"
    },
    "census_acs5": {
      "source_id": "census_acs5",
      "artifact_id": "src__census_acs5__2024__r1__...",
      "artifact_sha256": "...",
      "artifact_uri": "artifact://source/census_acs5/...",
      "provider_release_id": "2024",
      "observation_max": "2024-12-31",
      "monthly_status": "reused",
      "check_status": "no_new_release_expected",
      "validation_status": "passed"
    }
  },
  "family_resolutions": {
    "census_bps_resolved": {
      "artifact_id": "family__census_bps__2026-08__...",
      "artifact_sha256": "...",
      "artifact_uri": "artifact://family/census_bps/...",
      "parent_artifact_ids": ["<final>", "<provisional>"],
      "policy_id": "bps_final_provisional_precedence_v1"
    }
  },
  "assembly_inputs": {
    "census_bps": "family_resolutions.census_bps_resolved",
    "redfin": "sources.redfin"
  },
  "git_sha": "...",
  "governed_config_hashes": {"config/source_metric_registry.csv": "..."},
  "warnings": []
}
```

The real manifest contains all source entries and an explicit `assembly_inputs` mapping. The `required_source_inventory` is sorted and exact. `monthly_status` is one of `refreshed`, `reused`, `no_new_release`, or `no_new_release_expected` for accepted inputs; waiting/failure/stale statuses cannot produce an assembly-ready manifest.

The source-set ID is derived from target month, schema/contract versions, ordered source IDs, full artifact hashes, family-resolution hashes, Git SHA and governed config hashes. Timestamps are lineage but excluded from computations where they would prevent deterministic identity.

## Input resolution and verification

Before database creation, the assembler must:

1. validate source-set canonical JSON and supported versions;
2. require exact inventory with no missing/extra/duplicate source IDs;
3. resolve each storage-neutral URI through configured backend adapters;
4. verify package, manifest, data, lineage, and validation hashes before parsing;
5. require manifest `source_id`, artifact ID/hash, release ID, and schema to equal the source-set declaration;
6. require artifact status complete and validation passed;
7. validate target-month eligibility without requiring every observation maximum to equal the target month;
8. verify source registry ownership of every metric and governed geography membership;
9. validate BPS family result and exact parent hashes;
10. reject mutable aliases, unverified local substitutions and unsupported partitions.

No network/provider call is permitted after input resolution begins.

## Generic assembly behavior

The assembler creates a new candidate path; it never opens an existing `market.duckdb` as truth. In deterministic source order it builds governed dimension tables and loads canonical facts from complete source artifacts. It may build derived views and registry-governed derived metrics only through existing separately governed canonical logic; source truth remains unchanged.

Provider-specific algorithms are prohibited. The assembler understands only:

* artifact and source-set schemas;
* canonical fact schema and metric ownership;
* generic dimensions/geography registries;
* declared family-resolution outputs;
* governed derived-view definitions;
* deterministic database settings and validation.

## BPS final/provisional boundary

Final/provisional precedence belongs in the BPS-family resolver, not generic assembly and not either independent source producer. This is justified because:

* each source must first reconcile preserve-prior within its own lineage;
* final/provisional artifacts must remain independently reusable/auditable;
* precedence is provider-family logic, not a generic cross-source rule;
* a resolved artifact can prove exact parents, final ownership counts, provisional fill counts and conflicts.

The resolver emits final-owned keys plus provisional-only eligible gaps. It fails on unauthorized metrics, duplicate keys, ungoverned conflicts, missing parents, or parent hash mismatch. The generic assembler loads only the resolved result for canonical BPS facts while retaining both parent identities in assembly lineage.

## Database schema and validation

### Before and during load

* canonical columns/types match the assembly schema;
* source ID is constant and declared;
* canonical keys are unique within each assembly input;
* metric ownership is exclusive and exact;
* no two non-family inputs claim the same canonical metric/key;
* property/geography identities are governed;
* source rows/counts/hashes reconcile to manifests;
* NaN/infinite/null behavior follows metric contracts;
* input ordering cannot alter output.

### After load

* expected source inventory and exact metric inventory are present;
* `(geo_id, metric_id, date, property_type_id)` is globally unique;
* dimensions and facts have referential integrity;
* geography membership and legacy-ID exclusions pass;
* per-source rows, date bounds, geography/metric counts and value summaries reconcile;
* final/provisional BPS ownership/fill evidence reconciles;
* required derived views build and validate;
* source artifact, source-set, contract, config and Git identities are recorded in canonical metadata tables;
* deterministic rerun from identical inputs produces the governed semantic identity and equivalent exported content.

Row counts alone never establish acceptance. Failed assembly deletes/quarantines only the candidate and cannot mutate a prior database.

## Candidate output and manifest

Output is a candidate `market.duckdb` plus `canonical_market_manifest.json` containing:

```text
schema_version
canonical_market_build_id
source_set_id + sha256
target_month
assembly_contract_version
artifact/config/git identities
per-source reconciliation counts and observation bounds
database filename, size, optional SHA-256
semantic table hashes / deterministic export hashes
validation status and evidence hashes
warnings
```

Whole-DuckDB byte identity may vary with DuckDB version/storage layout. Reproducibility authority is fixed DuckDB/Python/dependency versions plus deterministic sorted table exports and semantic table hashes; a file SHA is still recorded for transport integrity. A durable DB release is optional convenience/audit evidence, never historical-truth authority.

## `market.duckdb` lifecycle

Each eligible cloud run downloads exact governed artifacts, builds from scratch at a unique candidate path, validates, and hands the accepted database downstream. The workspace/database may be discarded after publication. No prior DB is needed. If retained, the DB artifact references the source-set exactly and can be regenerated.

Known nondeterminism risks that implementation must eliminate or pin include provider-generated timestamps leaking into data, unordered frames/SQL, Parquet writer changes, floating aggregation order, DuckDB version/storage format, locale/timezone, and derived use of wall-clock date. Provider retrieval nondeterminism is frozen before assembly by immutable artifact publication.

## `market_serving.duckdb` lifecycle

The accepted canonical DB feeds the existing candidate-first serving builder:

```text
accepted market.duckdb
→ candidate market_serving.duckdb
→ serving/source/freshness/Redfin-target validation
→ workflow-local promotion/use
```

Both databases may be disposable in cloud execution. Serving metadata records canonical build/source-set identity. Provider fetching, overlap resolution, absence preservation, release detection and BPS family resolution are prohibited in this layer.

## Local-to-cloud Redfin gate

Local production creates:

```text
artifact_id = src__redfin__<target_month>__r<revision>__<content-prefix>
provider_release_id = <target_month>
```

It validates all files/manifests/data/lineage, uploads to a unique storage key, verifies remote bytes, and atomically publishes an immutable index record. A corrected artifact increments revision and explicitly supersedes the prior artifact; it does not overwrite it.

Cloud eligibility resolves an **exact expected target-month index record** from a storage-neutral governed index/API or an explicit dispatch input. It never asks for `latest`. Missing record returns `waiting_for_manual_redfin` with exit zero. A present but invalid/conflicting record blocks. The source-set pins the chosen artifact ID, URI and SHA-256.

The initial backend may map identities to immutable GitHub Release tags such as `source-redfin-2026-08-r1` and an exact asset name, but those conventions live in a storage resolver. Canonical orchestration consumes storage-neutral identity/hash records.

## Future monthly cloud state machine

```text
target_month_resolved
→ redfin_artifact_check
  missing → waiting_for_manual_redfin (clean no-op)
→ check_all_online_sources
→ refresh_or_reuse_each_source_artifact
→ resolve_BPS_family_artifact
→ validate_and_publish_source_set_manifest
→ resolve_and_verify_all_inputs
→ assemble_candidate_market_db
→ validate_canonical_market
→ build_and_validate_candidate_serving_db
→ run_and_validate_immutable_Macro_Regime
→ build/smoke_static_site
→ immutable publication handoff
→ deploy Pages
→ complete
```

Analytics completion and Pages deployment status remain distinguishable. Idempotency is keyed by target month plus exact source-set ID; a complete run cannot silently resolve newer artifacts.

## Reproducibility contract

Given the exact source-set manifest and its hash, immutable artifact packages, Git commit, governed config hashes, contract versions, dependency/runtime lock, and deterministic build parameters, the canonical facts and semantic table hashes must reproduce. Any input identity change creates a new source-set/build identity. No wall clock, mutable alias, directory listing, provider call, or opaque prior DB influences output.

## Versioning

* `source_refresh_revision_v0_2` — source truth and monthly policy.
* `source_artifact_contract_v1` / `source_artifact_schema_v1` — package semantics/schema.
* `canonical_market_assembly_v1` — assembly semantics.
* `source_set_manifest_v1` — input manifest schema.

Breaking changes to canonical keys/types, truth/absence/retraction semantics, artifact completeness, hash/identity calculation, required manifest fields, family precedence, source inventory/ownership or deterministic assembly require the relevant major/version increment. Additive optional metadata may use a backward-compatible minor version only after reader validation is defined.

## Migration without a flag day

1. Approve these contracts and external lag/probe decisions.
2. Implement shared artifact schema, canonical JSON/hashing, storage-neutral resolver, reconciliation primitives and fixture validation without changing production.
3. Build Redfin durable state, inbox and canonical artifact producer locally; parallel-validate against accepted Redfin DB rows.
4. Build FRED macro artifact producer to prove cloud acquisition, complete-state reconciliation and reuse.
5. Build a canonical assembler proof from Redfin + FRED artifacts and compare relevant facts to the current DB.
6. Migrate FRED unemployment, NRC, CES, LAUS, BEA, ACS, and BPS family one vertical slice at a time, retaining parity evidence and current production fallback.
7. Run complete shadow source-set assembly and downstream serving/Regime parity.
8. Promote cloud source-set assembly as monthly authority only after human acceptance; deprecate direct-to-DuckDB source jobs/workflows separately.

At no point is the immutable Redfin baseline rewritten or an unverified reconstructed DB declared authoritative.

## Storage feasibility and roles

The accepted July Redfin evidence is 652,282 normalized rows across 372 geographies and 11 metrics. That long, narrow canonical table is far smaller and more compressible than seven wide multi-GB raw exports. Dictionary-encoded keys, dates and numeric columns should make single-source compressed Parquet practical for an initial release backend; exact bytes must be benchmarked locally without scanning hosted raw data. Online source artifacts should be smaller, with long BLS histories the next likely size concern.

Implementation must configure a positive backend threshold and safety headroom, validate size before publication, and fail `storage_strategy_required` before exceeding it. Deterministic partitioning is available without changing one-logical-artifact-per-source granularity. A future S3/object-store transition changes URI resolution only.

Storage roles are fixed: Git is logic/governance; durable Releases/object storage are canonical source state; Actions artifacts are temporary evidence; Actions workspaces contain disposable databases; Pages hosts only the static product. Material Parquet and DuckDB files are prohibited from Git history.
