# Canonical market artifact contract v1

## Boundary

`canonical_market_artifact_v1` describes an immutable assembled
`market.duckdb`; it does not build, publish, promote, or change a database.
Phase 1 tests create only temporary fixture DuckDBs. The compatible
`fact_timeseries` schema and generic assembler are unchanged.

## Semantic identity

The market artifact ID hashes:

* exact source-set ID and semantic SHA-256;
* canonical assembly contract and schema identities;
* consumed config hashes;
* tracked builder contract identity and dependency-lock identity;
* explicit assembly revision;
* resulting database SHA-256.

Raw Git SHA and build timestamp are recorded as provenance but deliberately do
not alter semantic identity: unrelated commits must not manufacture new market
truth. A code or dependency change that affects governed construction must
advance the builder/dependency contract identity; changed database bytes always
change identity.

## Manifest validation

The strict manifest records the exact source set and source-set package hash,
DB filename/SHA, compressed transport package SHA, table inventory, row/source/
geography/metric counts, first/last dates, duplicate-key count, validation state,
warnings, builder Git SHA, dependency lock, and build time. Validation requires
an exact source-set-style ID, passed validation, zero duplicate canonical keys,
nonempty table inventory, at least one source, full lowercase hashes, and a
recomputed matching market artifact ID.

`compressed_package_sha256` is a reserved exact transport identity. Phase 1 does
not package or publish production DuckDBs; a future publisher supplies it after
building the deterministic database transport envelope.

## Registry relationship

The generic catalog supports `canonical_market` as a first-class record and a
separate mutable accepted pointer. It also supports a future `serving_market`
record whose typed metadata references the exact canonical market artifact.
Serving remains a derived product/query contract and is not implemented here.

## Idempotency and failure

Same full semantic inputs and DB bytes produce the same ID regardless of
operational time or Git provenance. Different DB bytes produce a different ID.
Forcing changed bytes under the old ID fails validation. A missing or malformed
source-set reference, nonzero duplicate count, or failed validation state fails
closed.

No GitHub Release implementation is active. Phase 2 may publish validated
canonical manifests and packages through the registry interfaces without
changing generic assembly semantics.
