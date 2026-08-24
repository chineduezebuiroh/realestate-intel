# Durable governed source-artifact registry and cloud canonical assembly v0.1

**Status:** Phase 2A transport implemented for fixture acceptance; not production configuration
**Scope:** reconnaissance and design only  
**Decision date:** 2026-08-24

## Executive decision

Use **GitHub Releases as the durable backend**, organized as **one immutable
release per governed artifact**, with deterministic tags and one deterministic
package plus a small publication receipt. Keep the **accepted/current catalog in
tracked Git JSON**, updated by a serialized, compare-and-swap hosted workflow
after remote byte verification. Publish source sets, canonical
`market.duckdb`, and serving snapshots by the same immutable-release pattern.
Source sets always pin an exact artifact URI, release tag, asset name, asset ID,
and SHA-256; they never resolve `latest` during assembly.

This is preferable now to S3/R2/B2. The accepted source payloads are small, the
repository already authenticates and downloads exact Release assets, and
GitHub Actions is the compute plane. Releases avoid a new account, bucket,
billing boundary, IAM policy, SDK, and local credential. Object storage becomes
the governed escape hatch if a package approaches 2 GiB, aggregate cost or
availability becomes material, write concurrency outgrows serialized catalog
updates, or organizational recovery requires an independently administered
copy. This is a deliberate choice, not a claim that Releases are an append-only
WORM store: governance depends on immutable-release settings, permissions,
verified hashes, audit receipts, and backup.

## Evidence and present boundary

The source-artifact vertical slice already provides deterministic canonical
Parquet, manifests, content/package hashes, exact `artifact://` identities,
validation, preserve-prior reconciliation, a storage-neutral resolver boundary,
source-set validation, and generic DuckDB assembly. Redfin emits complete state
with key-aligned lineage; FRED detects unchanged canonical state. The accepted
two-source assembly proves semantics, not production publication.

Today `LocalArtifactResolver` maps exact URIs to directories and
`ArtifactPublisher` is only an abstract one-method placeholder. The assembler
calls the resolver and never acquires providers, which is the correct boundary.
The current source-set v1 is explicitly a partial vertical slice, assigns every
entry `refreshed`, makes required equal included, and permits empty
`config_hashes`; those constraints must be replaced by a production version.
The FRED workflow's 30-day Actions artifact is acceptance evidence only.

Phase 2A adds a REST-backed Release publisher/resolver and serialized tracked
fixture-catalog writer. It deliberately proves only the
`source-artifact-fixture/fixture_source/...` namespace after merge through a
manual hosted workflow. Production Release publication and pointer activation
remain deferred.

The existing monthly orchestrator is a resumable **local** pipeline that reads a
mutable full DB, gates on local Redfin drop states, builds/promotes a local
serving DB, runs analytics, and publishes a regime bundle. It is not the future
cloud source-set coordinator. Its locking, checkpoint, explicit-failure, exact
release-input, and checksum patterns should be preserved; its direct refresh,
local DB promotion, and Redfin-trigger assumptions should not.

## Options evaluated

| Backend | Advantages | Disadvantages | Decision |
|---|---|---|---|
| GitHub Releases | Existing identity/auth/audit plane; exact tags/assets; 2 GiB per asset; no new service; `GITHUB_TOKEN`; `gh` works locally | Maintainer can delete unless permissions/settings prevent it; REST limits; not an object-store SLA; repository coupling | **Use now** |
| S3 | Mature versioning/Object Lock, lifecycle, replication, large objects, conditional writes | New AWS account/cost/IAM/credentials; multipart and retention administration | Escape hatch |
| Cloudflare R2 / Backblaze B2 | S3-compatible, inexpensive, large-object oriented | Same additional credential/vendor/DR burden; less repository-native audit | Not justified now |
| Git blobs/LFS | Versioned pointers and familiar review | Binary growth/quotas; Git file limits; LFS bandwidth/credentials; wrong lifecycle | Reject |
| Actions artifacts/cache | Native workflow transfer | Expiring run evidence; deletion and retention policies; cache is mutable/evictable | Acceptance/transient only |

## Current platform constraints (verified against GitHub documentation)

GitHub's current documentation states:

* each Release asset must be under **2 GiB**, with **1,000 assets per release**;
  there is no documented total release size or bandwidth limit. Our
  one-package-per-release design avoids the asset-count ceiling. [About
  releases](https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases)
* immutable releases can lock the tag and assets and produce a release
  attestation after publication. This must be enabled and publication must not
  use a rolling tag. [Immutable
  releases](https://docs.github.com/en/code-security/supply-chain-security/understanding-your-software-supply-chain/immutable-releases)
* the upload REST endpoint streams binary content; a successful upload can
  still leave a `starter` asset after an upstream failure, which should be
  deleted before retry. Duplicate names are not an overwrite contract and must
  be treated as a conflict to inspect, never `--clobber`. [Release assets REST
  API](https://docs.github.com/en/rest/releases/assets)
* normal Git warns above **50 MiB**, blocks files above **100 MiB**, and browser
  upload has a separate **25 MiB** limit; repositories are recommended to stay
  ideally below 1 GiB and strongly below 5 GiB. These Git limits do not govern
  Release assets. [About large files on
  GitHub](https://docs.github.com/en/repositories/working-with-files/managing-large-files/about-large-files-on-github)
* Actions log/artifact retention is configurable: public repositories are
  limited to 90 days; private repositories can configure up to 400 days. A
  particular artifact's retention cannot exceed the repository setting.
  Therefore it is not indefinite governance. [Artifact and log
  retention](https://docs.github.com/en/actions/administration-and-supporting-enterprise/managing-your-enterprise/managing-use-of-actions-for-your-enterprise/enforcing-policies-for-github-actions-in-your-enterprise#configuring-the-retention-period-for-github-actions-artifacts-and-logs)
* a standard Linux GitHub-hosted runner has **14 GB SSD** and is a clean
  ephemeral VM. That comfortably covers the observed databases but is a real
  preflight limit for input packages plus extraction, candidate DB, temporary
  files, and validation copies. [Hosted runner
  reference](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)
* authenticated REST requests normally receive **5,000 requests/hour** for a
  user token; installation and `GITHUB_TOKEN` limits are documented separately.
  A monthly run with tens of assets is nowhere near this, but code must honor
  pagination, retry headers, and secondary limits. [REST rate
  limits](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api)
* release creation/upload requires repository contents write access. A workflow
  can use the built-in token with `permissions: contents: write`; callers should
  grant no broader scope. [Automatic token
  authentication](https://docs.github.com/en/actions/security-for-github-actions/security-guides/automatic-token-authentication)

The 2 GiB asset and 14 GB workspace limits are real blockers only when a
preflight size envelope is exceeded. Authentication, pagination, serialization,
and deterministic packaging are implementation details. Human deletion remains
a governance/DR risk and requires least privilege plus an independent backup or
migration trigger; Releases alone cannot make a repository owner powerless.

## Immutable organization and addressing

Use release-per-artifact rather than a long-lived release, source release, or
cycle release. It eliminates shared-asset mutation and most concurrent-upload
races, never approaches 1,000 assets, makes deletion/recovery granular, and
allows a release to be made immutable only after verification.

| Object | Tag pattern | Asset pattern |
|---|---|---|
| Source | `source-artifact/<source_id>/<artifact_id>` | `<artifact_id>.tar.zst` |
| Source set | `source-set/<target_month>/<source_set_id>` | `<source_set_id>.json` |
| Canonical DB | `canonical-market/<target_month>/<market_artifact_id>` | `<market_artifact_id>.duckdb.zst` |
| Serving DB | `serving-market/<target_month>/<serving_artifact_id>` | `<serving_artifact_id>.duckdb.zst` |

Packages must be deterministically built: sorted POSIX paths; fixed uid/gid,
mode, mtime, locale, and compressor parameters; no parent directory or symlinks;
and an extraction allowlist. The package SHA-256 is distinct from the existing
logical directory hash and from `data_sha256`. Record all three where
applicable. Names and tags are derived from validated identity, never supplied
free-form.

The logical URI remains storage neutral:
`artifact://source/<source_id>/<artifact_id>`. A catalog record binds it to
`github-release://<owner>/<repo>/<tag>/<asset_id>/<asset_name>` and records the
package SHA. The asset's numeric ID protects against name-only ambiguity. Exact
resolution is allowed without the mutable current pointer once the catalog
record is known.

## Catalog and mutable pointers

Add a future tracked candidate such as
`config/source_artifact_catalog_v1.json`. It contains append-only artifact
records and a distinct `accepted_pointers` map. Records include source/artifact
ID, logical URI, release tag/database ID/asset ID/name, package and payload
hashes, provider release identity, target month, observation bounds, publication
time and receipt, validation state, source policy version, config hashes, and
supersession status.

Only a hosted catalog workflow writes it. It uses repository concurrency, reads
the expected Git blob SHA, re-verifies the immutable release and downloaded
bytes, inserts an absent record or proves an identical record, advances the
pointer according to policy, validates the full catalog, and creates a reviewed
commit/PR. A changed existing immutable record is forbidden. A stale compare
fails and retries from the new catalog. The pointer is discovery convenience;
source sets copy exact records and never embed `current` or `latest`.

Tracked Git is chosen because metadata is small, reviewable, recoverable from
history, and supports atomic commits. A mutable Release asset would need unsafe
delete/re-upload semantics; a dedicated database adds unnecessary state.

## Two-phase publication contract

1. Validate the local artifact and required config hashes; reject untracked or
   dirty identity inputs according to production policy.
2. Build the deterministic package; calculate logical package, archive, and
   member hashes; produce a signed-by-workflow publication intent/receipt.
3. Resolve the deterministic release tag. If an immutable release exists,
   download by asset ID and compare bytes. Equal is idempotent success;
   different bytes or metadata is a hard identity collision.
4. Otherwise create a **draft** release at the exact tag, upload the final asset
   name without clobber, and reject unexpected assets or `starter` state.
5. Download the asset into a fresh directory using the API, verify archive SHA,
   safely extract, re-run source validation, and compare manifest identity and
   member hashes.
6. Upload the receipt, publish/freeze the release using immutable-release
   controls, then re-query it. A draft or failed verification is not current.
7. Dispatch the serialized catalog update. The index advances only after all
   verification. A failure leaves an unindexed draft or verified immutable
   release; rerun resumes at the first incomplete phase.

GitHub offers no atomic transaction spanning an asset and a Git commit. The
safe commit point is the catalog update. Temporary asset names are unnecessary
because draft visibility plus a final deterministic name provides staging;
consumers accept cataloged, published, immutable records only. Never use
`gh release upload --clobber`.

## Resolver design

Retain `ArtifactResolver.resolve(uri) -> Path` for assembly compatibility and
introduce a remote implementation rather than GitHub logic in `assemble()`.
`GitHubReleaseArtifactResolver` should be constructed with a read-only catalog,
repository identity, token provider, workspace/cache root, and size ceiling. It:

1. parses and canonicalizes the exact URI (no aliases);
2. locates one catalog record and requires `published_immutable_verified`;
3. checks tag, numeric asset ID, filename, archive hash, and maximum size;
4. downloads to a `.partial` file, hashes while streaming, then atomically
   renames;
5. safely extracts into a digest-keyed temporary directory;
6. validates the artifact and exact ID/source/package hashes;
7. returns that local directory and caches only under the verified archive
   digest for the workflow run.

A generic `RemoteObjectFetcher` could isolate HTTP transport, but the first
production adapter should be Release-specific. The publisher interface needs a
structured publication result and explicit `inspect/upload/verify/finalize`
phases; a single returned string is insufficient.

## Production source-set contract

Create a new schema rather than relaxing the partial v1 validator. Identity
must include target month; sorted exact artifact records (URI, ID, archive SHA,
logical package SHA, provider release ID, observation max); required and
included inventories; per-source monthly result and eligibility decision;
contract/schema versions; tracked Git SHA; family resolution; and hashes of
configuration actually consumed.

At minimum hash:

* `config/monthly_refresh_policy.json` (inventory, cadence and gate);
* `config/source_refresh_revision_policy_v0_2.json` (source reconciliation,
  absence, failure and stale policy);
* `config/source_metric_registry.csv` (metric ownership used by assembly);
* `config/geo_manifest.generated.csv` (geography admission used by assembly);
* the versioned canonical assembly contract or, preferably, a future small
  machine-readable assembly schema/policy consumed by validation.

Each source artifact already pins its own source-specific inputs (for example
Redfin baseline/domain manifests). Do not duplicate unrelated analytics,
feature, or presentation registries in the source-set identity.

Per-source check outcomes are:

* `refreshed`: provider success changed canonical content; use new artifact;
* `unchanged`: successful query/reconciliation proved identical content; use
  the exact prior accepted artifact;
* `provider_still_stale`: successful governed availability check proved no new
  release and lag is within policy; carry prior artifact and record evidence;
* `failed`: technical/provider/auth/validation failure; never relabel stale.

Every governed source is checked monthly. A required source may carry forward
only for `unchanged` or `provider_still_stale` and only within its governed lag.
A newly detected release that fails acquisition/reconciliation blocks. A
technical failure blocks unless a source policy explicitly defines a reviewed
degraded mode; it never silently carries forward. Redfin is stricter: the exact
target-month artifact must exist, otherwise status is
`waiting_for_manual_redfin` and no source set is minted. Required inventory may
not differ from included inventory in a complete set. Optional/family sources
require explicit family-resolution evidence, not omission.

## Cloud monthly workflow

Use a scheduled/dispatch coordinator plus reusable source workflows and a
separate assembly job/workflow:

1. Create an immutable cycle attempt keyed by target month and policy/Git SHA.
2. Fan out automated source checks independently (bounded matrix); each returns
   a small signed/checksummed status record and publishes any changed source
   artifact before reporting success.
3. Query the catalog for the target-month Redfin prerequisite. Do **not** trigger
   automated workflows from Redfin. If absent, persist a successful
   `waiting_for_manual_redfin` cycle status and stop cleanly.
4. The eligibility gate validates every status against refresh policy and the
   catalog. Fail on acquisition failure, excessive lag, missing required source,
   or detected-but-unpublished release.
5. Build and publish one immutable source-set manifest. If the identical set
   exists, verify/reuse it. A different identity for the same attempt is a new
   governed revision, never replacement.
6. A separate least-privilege assembly job downloads exact pinned assets through
   the resolver, runs generic assembly into runner temporary storage, validates
   schema/key/count/date/source inventory and integrity, and closes DuckDB.
7. Publish the canonical DB artifact and validation receipt, then advance its
   tracked pointer. Trigger serving only from that exact canonical ID.

Retries resolve persisted immutable outputs and skip completed phases. A cycle
can be dispatched again after Redfin publication; no Redfin event is required.
Repository concurrency serializes source-specific publication/catalog writes;
source acquisition can remain parallel.

## Canonical `market.duckdb`

Treat it as another immutable governed artifact and do not commit it. Identity
is derived from exact source-set ID/hash, canonical schema and assembly contract
versions, relevant configuration hashes, builder Git SHA/dependency lock, and
resulting DB SHA-256. Its manifest/receipt records validation, table/schema
inventory, row/source/metric/geography/date statistics, source-set reference,
archive SHA, and database SHA. Publish one release per canonical artifact and
maintain a distinct tracked accepted pointer. Same source set plus same build
identity and bytes is reuse; a byte difference is a reproducibility failure
unless a governed assembly revision changes identity.

Retain all source-set manifests indefinitely. Retain all canonical DBs initially;
annual review may keep all monthly DBs while small, then retain the latest 24
monthly plus year-end and release-pinned DBs only **after** proving any pruned DB
can be reproduced from indefinitely retained sources and sets.

## Serving database recommendation

Keep `market_serving.duckdb`, but define it as a curated downstream contract,
not merely compression: product/query isolation, stable tables/views,
controlled metrics/geographies, and protection from canonical schema evolution.
The repository's regime pipeline defaults to this DB, and the builder also
constructs BPS-specific views and supplements tables beyond copying facts.

Do **not** retain the blanket 2015 cutoff as an architectural requirement. The
builder currently applies `SERVING_START_DATE = 2015-01-01`, while accepted
canonical FRED history begins in 1947. Nothing in the inspected production
consumer contract proves that Macro Regime mathematically requires only
post-2015 history. Longer warmups, future diagnostics, and new consumers can be
harmed by truncation. In the next implementation phase, measure output/feature
parity and make history a consumer-declared, validated policy. Default future
serving should preserve full canonical history and reduce dimensions/metrics or
materialize views only when justified.

Publish the serving DB as an immutable release artifact derived from an exact
canonical market ID plus serving contract/config/builder identity and DB SHA.
Maintain its own accepted pointer. It is deployable input, never canonical
source truth.

## Storage envelope

| Object | Observed size | Expected growth | Backend / limit | Risk |
|---|---:|---|---|---|
| Redfin `data.parquet` | ~1.23 MB (accepted evidence) | Monthly complete state; likely gradual | Release; <2 GiB asset | Low |
| FRED `data.parquet` | ~40 KB (accepted evidence) | Small revised full history | Release; <2 GiB | Low |
| Future source artifact | No defensible repository measurement | Source-dependent; preflight each | Release until package nears 2 GiB | Unknown, governed threshold required |
| Source-set manifest | JSON, expected KB; current proof not retained here | Linear in source count | Release and catalog metadata | Negligible |
| Canonical market DB | Accepted two-source row counts exist, but candidate bytes unavailable; local `data/market.duckdb` absent | Sources/geographies/history grow | Release; 2 GiB asset and 14 GB runner envelope | Unknown; measure in acceptance |
| Serving DB | 15,478,784 bytes (~14.76 MiB), tracked working copy | Curated scope; likely moderate | Release; <2 GiB | Low now |

The tracked `data/market_public.duckdb` is 9,973,760 bytes (~9.51 MiB), but it
is not evidence for canonical DB size. The design must enforce both a positive
per-asset ceiling below 2 GiB and a workspace budget (compressed inputs,
unpacked inputs, candidate, verification copy, DuckDB temp headroom) below 14 GB.

## Credentials and security

Automated sources retain only provider secrets required by their adapter (for
example `FRED_API_KEY`). Publication jobs receive built-in `GITHUB_TOKEN` with
`contents: write`; acquisition/assembly jobs use `contents: read`, and Actions
prior-resolution uses `actions: read` only while the bridge exists. Pin third
party actions and dependencies, never print tokens, validate archive paths, and
use environments/required reviewers for pointer promotion if desired.

The future one-command local Redfin runner should use the operator's existing
GitHub CLI session (`gh auth status`; token supplied by `gh`, not read or stored
by this code) to dispatch/upload with least privilege. Prefer dispatching a
hosted publication workflow after locally producing and validating the package;
if local upload is necessary, `gh release` can use the existing session. A
fine-grained PAT scoped only to this repository's Contents write is fallback,
not a file or environment value the runner persists. SSH credentials cannot
upload Release assets.

## Idempotency, failures, and recovery

* Upload success/verification failure leaves an unindexed draft. Rerun inspects
  it, removes only a documented `starter`/partial asset, or verifies the exact
  final asset; it never overwrites unknown bytes.
* Existing ID plus equal archive, member, and manifest hashes is no-op success.
  Same ID with any different byte is a hard collision and incident.
* Identical source-set identity is reused. Same target month with changed exact
  inputs creates a governed set revision.
* Existing validated canonical DB for the same full identity is reused; force
  rebuild writes a candidate and must prove identical bytes or create a governed
  assembly revision.
* Redfin reconciliation commits local durable state before emission. If
  publication fails, rerun validates state at the target vintage, recognizes or
  re-emits the same artifact deterministically, and resumes publication; it does
  not apply the raw drop again.

Recovery paths:

* Lost local `market.duckdb`: resolve an accepted immutable canonical DB, or
  rebuild from its source set, exact artifacts, code/dependency lock, and config.
* Lost local Redfin state: restore the immutable July baseline, then preferably
  restore the newest retained complete Redfin artifact (data plus key lineage)
  as state and replay later governed artifacts/drops. Raw three-drop retention
  alone is not the recovery plan.
* Ephemeral runner: expected; every accepted input/output is remote and exact.
* Corrupt serving pointer/asset: verify SHA, roll pointer back to a prior
  immutable serving artifact, then rebuild from pinned canonical input.
* Removed source asset: block resolution; restore from an independently retained
  backup/verified local copy under the same identity or rebuild from governed
  Redfin baseline/raw lineage/provider source where possible. Never substitute.
* Corrupt catalog: reconstruct immutable records by enumerating deterministic
  tags and receipts, verify every asset, compare Git history, and commit a
  reviewed repair. Pointer recovery is from Git history and cycle/source sets.

## Retention by storage class

* Redfin raw drops: keep the existing separately governed baseline/history and
  three-drop policy; do not conflate it with canonical retention.
* Canonical source artifacts and receipts: retain **all indefinitely**. Their
  measured size and reproducibility value make pruning uneconomic now.
* Source sets: retain all indefinitely in releases; catalog records and Git
  history remain small.
* Canonical DB: retain all initially; future policy may keep 24 months plus all
  year-end, release-pinned, and incident artifacts only after reproducibility
  proof and backup.
* Serving DB: retain 12 rolling months plus year-end and every product-pinned
  version; it is derivable. Never delete a still-referenced asset.
* Actions artifacts: 30-day acceptance/debug evidence only; caches disposable.

## Staged implementation plan

1. **Contract and deterministic packaging foundation.** Extend storage interfaces,
   define catalog/source-set v2 schemas and package/receipt validation, and add
   fixture tests. Acceptance: byte-identical packages, collision and safe
   extraction tests. Rollback: new code remains unused.
2. **Release publisher, resolver, and catalog workflow.** Implement draft upload,
   download verification, immutable finalization, tracked compare-and-swap
   catalog PR, and read-only resolver. Acceptance: fixture repository publishes,
   resumes failures, rejects changed bytes, resolves on a clean runner. Rollback:
   Actions bridge/local resolver remains authoritative.
3. **Redfin one-command publish.** Compose existing inbox, state reconciliation,
   artifact validation, and publisher without changing semantics. Acceptance:
   accepted July no-op/idempotency plus a fixture next month; failure after state
   commit resumes. Rollback: existing manual stages and local state remain.
4. **FRED durable cutover.** Publish accepted/unchanged output to Releases and
   resolve prior via catalog; retain 30-day evidence temporarily. Acceptance:
   two real runs reproduce unchanged ID/hash from clean runners. Rollback:
   acceptance bridge remains available, with no DB mutation.
5. **Production source-set v2 and two-source cloud assembly.** Add eligibility
   coordinator, config hashes, exact remote resolution, and candidate DB
   validation; Redfin absence stops cleanly. Acceptance: reproduce accepted
   661,329-row two-source result and duplicate-key zero. Rollback: local proof
   and legacy production DB stay untouched.
6. **Canonical DB publication.** Govern identity, receipt, release, pointer, and
   rebuild test. Acceptance: clean-run download SHA and deterministic rebuild.
   Rollback: keep canonical candidate unpromoted.
7. **Serving cloud derivation/publication.** Measure full-history versus current
   cutoff, freeze a serving contract, publish exact derivative, and run Macro
   Regime parity. Acceptance: reviewed parity and rollback from prior serving
   release. Rollback: current local serving snapshot remains.
8. **Migrate remaining sources one at a time.** Each adapter proves provider
   check outcomes, reconciliation, durable prior resolution, and parity before
   its legacy direct-to-DB job is retired. Legacy workflows remain until explicit
   source acceptance; no batch migration.

## Non-decisions and review gates

This document does not authorize a publisher, resolver, workflow, catalog,
source-set v2, production database write, source migration, serving cutoff
change, or secret. Before implementation, confirm immutable releases are
available/enabled for this repository, measure canonical package/workspace size,
and approve catalog PR/promotion governance and independent backup expectations.
