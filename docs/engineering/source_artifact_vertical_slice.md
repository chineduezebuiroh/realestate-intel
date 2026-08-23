# Governed source-artifact vertical slice

## Boundary and status

This implementation proves the `redfin` and `fred_macro` path defined by the
v0.2 contracts.  It runs beside existing ingestion and is deliberately marked
`partial_vertical_slice=true`; it cannot replace `data/market.duckdb`, serving,
Macro Regime, orchestration, or publication.  No network publication is hidden
inside reconciliation.

## Package, identity, and hashes

`core/source_artifacts` owns the provider-neutral schema, reconciliation,
storage interfaces, manifests, validation, source sets, and assembly.  An
artifact directory contains canonical Zstandard Parquet `data.parquet`,
canonical-JSON `validation.json`, canonical-JSON `manifest.json`, and optional
key-aligned `lineage.parquet`.  Data columns are explicitly ordered as
`geo_id, metric_id, date, property_type_id, value, source_id, property_type` and
sorted by the four-column canonical key.  PyArrow settings, index omission, and
types are controlled. Semantic determinism is required; Parquet bytes may still
depend on the pinned PyArrow version.

The one artifact ID algorithm hashes canonical JSON containing source ID,
provider release ID, target month, correction revision, full data and lineage
hashes, artifact schema, and refresh contract; it uses the first 16 hex digits
only as a readable suffix while manifests retain full SHA-256 hashes.
`retrieved_at` is lineage and is excluded. `artifact_content_hash` is this full
semantic hash. File hashes cover exact bytes. The source-set `artifact_sha256`
is a deterministic package-envelope hash over sorted filename, separator, and
file bytes (including the non-self-hashed manifest). A manifest can separately
be hashed as ordinary file bytes; no file contains its own hash.

The configurable `max_single_asset_bytes` guard fails with
`storage_strategy_required`; it never partitions. `ArtifactResolver` separates
resolution from creation/validation/publication, and `LocalArtifactResolver`
supports exact `artifact://` records (plus explicit local `file://` paths).
`ArtifactPublisher` is an intentionally unimplemented publication boundary.

## Redfin local workflow

The managed inbox is `data/redfin/raw/incoming/`. Registration identifies the
exact seven governed filename families, rejects unknown/duplicate/missing
families, inspects every endpoint, requires a common latest month, derives the
drop ID, and hashes every input. It copies into a sibling staging directory,
verifies hashes, atomically renames the complete directory, then clears incoming
files. `--keep-incoming` provides non-mutating fixture/dry inspection. An exact
existing registration is `already_registered`; changed bytes for the same month
are `conflicting_drop`.

`data/redfin/state/canonical_redfin.duckdb` is ignored durable local state and
does not mutate the immutable July baseline. Bootstrap accepts already governed
baseline-normalized facts and stamps the deterministic `2026-07` lineage.
Reconciliation validates/canonicalizes a contribution, begins a transaction,
replaces returned keys, preserves prior-only keys, validates uniqueness, and
commits; every exception rolls back. Artifact emission reads the complete state,
requires its latest governed vintage to equal the target month, and emits a
key-aligned lineage sidecar. Raw retention now additionally requires metadata
proof of `canonical_state_status=reconciled` and
`canonical_artifact_status=validated`, while retaining the existing three-drop,
baseline, history, and quarantine safeguards.

Local responsibility ends at a validated immutable upload-ready directory. Its
manifest provides path-independent artifact URI, ID, full hashes, size, and
target month. Durable-backend upload credentials, immutable two-phase upload,
remote verification, and index publication remain cloud prerequisites.

## FRED and assembly

The FRED producer accepts a normalized complete ordinary-current response and
uses the shared preserve-prior primitive: new-only adds, overlap replaces, and
prior-only survives. Fixture injection is network-free. Production acquisition
fails explicitly without `FRED_API_KEY`; it reuses the existing ordinary-current
series retrieval and preserves its physical spread direction without changing
the existing direct-ingestion entry point.

The source-set ID excludes `created_at` and hashes sorted source identities,
full package hashes, contract/config versions, Git SHA, and the `none` family
resolution. Validation resolves exact URIs and revalidates package identity.
The assembler does no acquisition or reconciliation. It optionally validates
metric ownership and governed geography registries, rejects global key
collisions, builds a compatible seven-column `fact_timeseries` plus artifact
metadata in a new candidate, and refuses either production database path.

## Staged acceptance

### A — code and smokes

```bash
python -m compileall core/source_artifacts sources/redfin sources/fred_macro scripts/validate_source_artifact.py scripts/validate_source_set.py scripts/build_market_from_artifacts.py scripts/register_redfin_inbox.py
PYTHONPATH=. python scripts/smoke_tests/160_169/164_source_artifact_vertical_slice.py
PYTHONPATH=. python scripts/smoke_tests/160_169/160_redfin_ingestion_v2.py
PYTHONPATH=. python scripts/smoke_tests/160_169/161_redfin_merge_semantics.py
PYTHONPATH=. python scripts/smoke_tests/160_169/162_redfin_metric_domains.py
PYTHONPATH=. python scripts/smoke_tests/160_169/163_monthly_refresh_orchestration.py
git diff --check
```

### B–E — local Redfin, with manual review stops

First bootstrap a **copied candidate** state from accepted baseline-normalized
facts through `sources.redfin.state.bootstrap_state`; never point the proof at a
production DB. Exercise inbox without clearing it:

```bash
PYTHONPATH=. python -m scripts.register_redfin_inbox --root /tmp/redfin-fixture/raw --keep-incoming
PYTHONPATH=. python -m scripts.validate_source_artifact artifacts/source_artifacts/redfin/<artifact_id>
sha256sum artifacts/source_artifacts/redfin/<artifact_id>/{manifest.json,data.parquet,lineage.parquet,validation.json}
du -ah artifacts/source_artifacts/redfin/<artifact_id>
```

Only after fixtures pass, invoke `emit_artifact` against the reviewed governed
local state into `artifacts/source_artifacts/redfin/`; validate and inspect the
size/hash/lineage above, then **stop for manual review and do not publish**.

### F–G — FRED and partial candidate

Real FRED execution additionally needs outbound network, `fredapi`, and a valid
secret. Review its normalized output before artifact emission:

```bash
test -n "$FRED_API_KEY"
PYTHONPATH=. python -c 'from sources.fred_macro.artifact import acquire_current; acquire_current()'
PYTHONPATH=. python -m scripts.validate_source_set artifacts/source_artifacts/source_set.json \
  --artifact 'artifact://source/redfin/<id>=artifacts/source_artifacts/redfin/<id>' \
  --artifact 'artifact://source/fred_macro/<id>=artifacts/source_artifacts/fred_macro/<id>'
PYTHONPATH=. python -m scripts.build_market_from_artifacts \
  --source-set artifacts/source_artifacts/source_set.json \
  --output artifacts/canonical_market_vertical_slice/candidate.duckdb \
  --artifact 'artifact://source/redfin/<id>=artifacts/source_artifacts/redfin/<id>' \
  --artifact 'artifact://source/fred_macro/<id>=artifacts/source_artifacts/fred_macro/<id>' \
  --metric-registry config/source_metric_registry.csv --geo-manifest config/geo_manifest.generated.csv
```

Before actual cloud publication, configure a durable backend/positive asset
limit, credentials, exact immutable index, two-phase upload/verification,
  dependency lock, tracked config hashes, Git identity, and reviewed FRED
  normalized output. Remaining sources, BPS family resolution, full source
inventory, monthly artifact orchestration, serving, analytics, Pages, and any
production DB promotion remain explicitly out of scope.
