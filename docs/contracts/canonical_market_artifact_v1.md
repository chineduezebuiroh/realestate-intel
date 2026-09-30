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

The common GitHub Release publisher accepts deterministic allowlisted
non-source packages for `source_set` and `canonical_market`, validates their
typed manifest and database identities before upload and after download, and
then uses the existing receipt/catalog lifecycle. Publication never activates
an accepted pointer.

## Cohort-controlled implementation boundary

`assemble_source_set_v2` is the production-candidate assembler for a validated
logical v2 set.  It resolves only each entry's exact immutable URI, verifies the
artifact/manifest identities, validates metric ownership (including an exact
family's declared physical owner identities) and geography,
property, date, finite-value, and canonical-key contracts, and builds a new
isolated DuckDB.  It never reads `accepted.source`, never loads physical family
parents, and prohibits production and serving database paths.  The candidate is
not authoritative until immutable publication, catalog insertion, and the
prepared cohort promotion described by the monthly production contract.
