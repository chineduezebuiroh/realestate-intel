# Artifact registry contract v1

## Status and boundary

Phase 1 remains the backend-neutral contract. Phase 2A adds the GitHub adapter
and a clearly isolated fixture-only tracked catalog; no production catalog or
production pointer is active. See
`docs/contracts/github_release_artifact_transport_v1.md`. The fixture catalog
cannot be supplied as a production registry merely by its path.

## Publication package

A source package preserves the existing source-artifact directory without a
source-specific layout. Its allowlist is derived from `manifest.json`:
`manifest.json`, its named data and validation files, and its named lineage file
when present. There may be no other file or directory.

Phase 1 uses a deterministic **uncompressed POSIX PAX `.tar`**, not `.tar.zst`.
The repository has no pinned Python Zstandard dependency and the runner does not
provide a governed `zstd` binary. Introducing an unpinned compressor would make
the claimed byte identity environment-dependent. Phase 2 may add a pinned,
deterministic Zstandard transport envelope, with its own hash, without changing
the governed tar member contract.

Members are lexically ordered and top-level only. Each is a regular file with
mtime 0, uid/gid 0, empty owner/group names, mode 0644, and no PAX extensions.
Filesystem mtimes, absolute paths, workspace metadata, nested paths, symlinks,
hardlinks, devices, and FIFOs are rejected. Extraction first verifies the full
package SHA-256, validates every member and the manifest-derived exact allowlist,
writes into a new directory without `extractall`, and runs the existing source
artifact validator. Failed extraction removes the partial directory.

## Receipt and state machine

`governed_artifact_publication_receipt_v1` records exact logical URI, typed
object ID, semantic content and package/member hashes, backend/repository,
release tag and numeric release/asset IDs, asset name, publication/verification
times, final state, publisher Git provenance, and contract versions. Offline
fixtures use deterministic positive integer IDs; production uses GitHub numeric
IDs. The strict loader does not permit strings or missing IDs.

States and legal transitions are:

```text
prepared -> uploaded -> remotely_verified -> published_immutable_verified
    |           |                |
    +-> failed  +-> failed       +-> failed
```

`failed` and `published_immutable_verified` are terminal. Skipping, reversing,
or finalizing before verification fails. Receipt identity hashes immutable facts
but excludes attempt-varying `published_at`, `verified_at`, and `receipt_id`.
Only `published_immutable_verified` receipts are catalog-eligible.

## Catalog

`artifact_catalog_v1` has four top-level fields: schema version,
`compare_and_swap.expected_git_blob_sha`, sorted `immutable_records`, and
`accepted`. The compare-and-swap value is either null for an offline/new catalog
or a lowercase Git blob object ID (40 characters for SHA-1 repositories or 64
for SHA-256 repositories) for the serialized updater. This is a compare-and-swap
identity, not the package integrity digest.

Records are discriminated by `object_type`: `source`, `source_set`,
`canonical_market`, or `serving_market`. Common fields pin logical URI, object
ID, repository, release tag/ID, asset ID/name, package/content hashes, receipt
ID, final state, and typed metadata. Source metadata adds source/data/provider
and observation identity. Serving metadata references its exact canonical
market, proving the catalog is not source-only.

Immutable `(object_type, object_id)`, logical URI, and `(repository, asset_id)`
identities are unique. An identical insert is idempotent. Any conflict is a hard
identity collision. Records are sorted by type and ID. All hashes are full
lowercase SHA-256. Malformed records, nonfinal states, and dangling accepted
pointers fail closed.

`accepted.source.<source_id>`, `accepted.canonical_market`, and
`accepted.serving_market` are mutable governance conveniences separated from
immutable records. Pointer changes do not mutate records. Immutable source sets
copy exact records and never resolve these aliases.

## Publisher and commit point

`ArtifactPublisher` exposes `inspect`, `prepare`, `upload`, `verify`, and
`finalize`; it intentionally has no opaque `publish(path)` operation.
`OfflineArtifactPublisher` simulates absent objects, interrupted uploads,
verification failures, orphan states, immutable finalization, idempotent same
bytes, and hard collisions without network I/O.

`GitHubReleaseArtifactPublisher` now implements those phases against exact
GitHub Release and numeric asset identities. `GitHubCatalogCAS` remains a
separate operation, preserving the catalog as the governance commit point.

Bytes are not governed merely because they exist. Resolution eligibility
requires verified bytes, immutable finalization, a valid final receipt, and its
matching immutable catalog record. **The catalog record is the governance commit
point.** The fixture resolver therefore refuses uploaded, verified-but-
uncataloged, finalized-but-uncataloged, receiptless, or tampered objects.

## Resolver

`CatalogPackageResolver` is the offline proof of the future remote adapter:
exact URI -> one immutable catalog record -> matching verified receipt -> numeric
asset lookup -> package SHA verification -> safe extraction -> existing artifact
validation -> local directory. It implements the existing `ArtifactResolver`
interface, so generic assembly remains unchanged and storage-neutral.

## Compatibility

Source artifact schema v1, source-set v1, `LocalArtifactResolver`, `assemble()`,
Redfin/FRED producers, the Actions prior bridge, and monthly orchestration remain
unchanged. Phase 2 must add GitHub transports behind these interfaces rather
than put GitHub behavior into canonical assembly.
