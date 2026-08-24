# GitHub Release artifact transport v1

## Boundary and identity

This contract implements GitHub Releases behind the registry's existing
publisher and resolver interfaces. Source tags are
`source-artifact/<source_id>/<artifact_id>` and assets are
`<artifact_id>.tar`. Phase 2A acceptance exclusively uses
`source-artifact-fixture/fixture_source/<artifact_id>`. Tags, asset names, and
logical URIs derive from validated immutable identity; `latest`, run IDs,
timestamps, filename searches, and overwrite/clobber behavior are forbidden.

The fixture is deterministic and reused, so acceptance runs do not accumulate
releases. No Redfin, FRED, source-set, or database artifact is in this namespace.

## Authentication and API behavior

The writer uses the built-in `GITHUB_TOKEN` and only `contents: write`.
Read-only resolvers need only `contents: read`. No PAT, Actions write,
administration, or Packages permission is required. Hosted Ubuntu runners have
`gh`, but this implementation uses the versioned REST API for deterministic
status handling. API authentication, authorization, rate-limit, missing
repository, upload/download, unexpected status, tag/ID/name, and CAS failures
fail closed. Bearer authentication is sent to the GitHub asset API, but never
forwarded to the short-lived signed redirect URL.

## Two-phase publication

The publisher validates the caller's exact metadata and follows the unchanged
state machine:

1. Build/validate the deterministic uncompressed tar and create or recover the
   exact draft Release, validate its deterministic tag and numeric Release ID,
   and retain that numeric ID as the primary remote identity (`prepared`).
2. Upload without clobber, or independently download an existing same-named
   asset and prove byte equality (`uploaded`). Different bytes are a collision.
3. Download by numeric asset ID into a fresh workspace, verify package SHA-256,
   safely extract, run artifact validation, and compare artifact/content/member
   hashes (`remotely_verified`).
4. Publish the draft by numeric Release ID, re-query that exact numeric Release,
   revalidate its tag and numeric Release/asset identities, and emit the genuine
   receipt (`published_immutable_verified`). A known numeric Release returning
   missing fails closed; only a fresh publication attempt may recover through
   deterministic tag discovery.

Finalization never updates a catalog. A verified or published Release without a
catalog record is an orphan and is ineligible for governed resolution. A rerun
can verify identical remote bytes, reproduce the receipt, and finish cataloging
without another Release or asset. Failed verification is never cataloged.

## Catalog commit point and CAS

The fixture-only tracked catalog is
`artifacts/fixture_registry/catalog.json`; production accepted pointers cannot
consume it. Its update reads and validates the target-branch file, captures the
Git blob object ID, inserts one immutable record deterministically, revalidates,
and sends a GitHub Contents API update with that blob ID as the precondition.
Repository workflow concurrency serializes writers. HTTP conflict fails and
requires a fresh read; no blind overwrite occurs. Identical records are a no-op.
Insertion does not move an accepted pointer. Pointer movement remains an
explicit, separately reviewed governance action, and exact source-set references
remain pinned when pointers move.

## Resolver and immutability

Resolution is exact catalog URI to repository, numeric Release ID, exact tag,
numeric asset ID, and filename. It rejects uncataloged orphans and mismatches,
downloads by asset ID, verifies the package hash, safely extracts, validates the
artifact, and compares semantic identity before returning a local directory.

The transport is **logically immutable by contract**: it never deletes,
replaces, or clobbers. GitHub immutable Releases may add platform enforcement
when enabled in repository settings, but the REST adapter does not claim or
infer that setting. A maintainer can still delete a Release where repository
policy permits it; that is a governance and disaster-recovery risk.

## Fixture operation and cleanup

Run `.github/workflows/artifact-registry-fixture-acceptance.yml` manually after
merge. It runs offline smokes, publishes and re-downloads the fixed fixture,
performs the CAS catalog insertion, resolves it, repeats publication to prove
idempotency, rejects altered bytes, and uploads a receipt, catalog snapshot, and
machine-readable report. Keep the deterministic fixture Release for inspection;
manual deletion is only needed when intentionally resetting the acceptance
environment.
