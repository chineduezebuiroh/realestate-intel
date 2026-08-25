# Redfin monthly source execution v1

**Status:** Phase 3A implemented; real-data acceptance remains an operator step.

## Operator and production boundary

Place exactly one export for each governed Redfin family in
`data/redfin/raw/incoming/`, then run:

```bash
PYTHONPATH=. python -m jobs.monthly_refresh.redfin
```

The command invokes the existing atomic registration, drop validation, durable
reconciliation, artifact emission, artifact validation, deterministic package,
GitHub Release remote verification, and production-catalog CAS primitives. It
returns `monthly_source_execution_result_v1`. It never assembles or promotes a
source set/market, builds serving, runs Macro Regime, promotes local state, or
moves `accepted.source.redfin`.

Production is pinned to
`chineduezebuiroh/realestate-intel@monthly-refresh-orchestration`. Authentication
comes from `gh auth token`; the token is neither printed nor persisted.

## Accepted state and bootstrap prerequisite

Accepted local state is `data/redfin/state/canonical_redfin.duckdb`. Routine
execution fails closed if this file or the catalog's accepted Redfin record is
absent; it never silently replays the baseline.

The production catalog initially lacks an accepted Redfin pointer, so the exact
proven July artifact must first be migrated separately:

```bash
PYTHONPATH=. python -m jobs.monthly_refresh.redfin \
  --bootstrap-accepted-artifact artifacts/source_artifacts/redfin/2026-07
```

This explicit path accepts only artifact
`src__redfin__2026-07__r1__b10214595868c2ff` with data SHA-256
`0ed5c374372bdcc8f5969dcbd6cd5015c55f2f84a2506687dea00081ccb49924`,
proves row equality with accepted state, publishes/catalogs it, and activates
the pointer. It registers no drop and starts no monthly cycle. This migration
is the only Phase 3A operation that activates the pointer.

## Candidate state, identity, and ledger

Candidates use
`data/redfin/state/candidates/<cycle_id>/candidate_redfin.duckdb`. A temporary
`.building.duckdb` is copied exactly from accepted state, reconciled in a DuckDB
transaction, and atomically renamed. Failure cannot mutate accepted state; its
SHA-256 is checked around candidate construction. The complete emitted artifact
and publication workspace live beside the candidate, suitable for later atomic
cohort promotion.

The drop content hash covers drop ID and registered file records. The shared
`jobs.monthly_refresh.production.cycle_id` covers drop ID, content hash,
target/drop month, and monthly-policy SHA-256. Runtime timestamps and Actions or
Release IDs are evidence, never cycle identity.

`data/redfin/state/monthly_source_ledger.json` is a durable, cycle-keyed
`redfin_monthly_cycle_ledger_v1`. States are `registered`, `validated`,
`candidate_running`, `candidate_ready`, `failed_retryable`, and
`failed_terminal`. It pins drop/prior/candidate/package identities. It never
writes `consumed_successfully`, which remains reserved for cohort completion.
Evidence under `artifacts/audit/redfin_monthly/<cycle_id>/` contains identity,
validation, candidate, publication/catalog, result, and failure summaries but
never provider files or secrets.

## Publication, idempotency, failure, and retention

The Release is `source-artifact/redfin/<artifact_id>` with
`<artifact_id>.tar`. Publication prepares, uploads, remotely redownloads and
validates, finalizes, records a receipt, inserts through catalog CAS, then
resolves through that catalog. Candidate insertion does not activate Redfin.

`source_change_detected` compares canonical `data_sha256` with accepted state.
Drop lineage remains governed provenance, so a new provider release may retain
lineage significance even when canonical data is unchanged.

After inbox clearing, rerun resumes the ledger cycle. A valid pinned candidate
is reused; immutable Release and catalog operations are idempotent. Transport
and unexpected I/O errors are retryable. Schema/domain/month conflicts,
missing governed prerequisites, and identity drift are terminal until operator
correction. A candidate published before catalog failure is recovered by exact
remote identity on retry.

Existing raw retention already requires the newest drop to be promoted,
published, reconciled, and validated before older drops become eligible. A
merely published Phase 3A candidate is not considered consumed or deletable.

## Local acceptance handoff

```bash
test -f data/redfin/state/canonical_redfin.duckdb
sha256sum data/redfin/state/canonical_redfin.duckdb
gh auth status
# place exactly seven governed exports in data/redfin/raw/incoming/
PYTHONPATH=. python -m jobs.monthly_refresh.redfin
find artifacts/audit/redfin_monthly -name source_execution_result.json -print
sha256sum data/redfin/state/canonical_redfin.duckdb
PYTHONPATH=. python -m jobs.monthly_refresh.redfin
```

The two state hashes must match. The rerun must return the same cycle, artifact,
Release/asset, and package identities with no catalog duplicate and no pointer
movement. Inspect the exact Release and integration-branch catalog, then stop:
do not promote, assemble canonical market, build serving, or run Macro Regime.

## Stable identity and July bootstrap transition

The production rule is **latest governed registered drop owns returned keys**.
The stable drop consists of month, sorted seven-family hashes, and governed
policy/configuration hashes. Exact replay resolves the same cycle, lineage,
artifact, package, Release/asset, and a catalog no-op; acceptance never moves.

The accepted July bootstrap used `baseline:2026-07`, a baseline hash including
`baseline_id`, and bootstrap ownership. Monthly reconciliation uses a
`redfin-drop:<drop identity>` request, registered-files hash, and monthly owner.
Those semantic lineage fields differ even when data and seven hashes match, so
the `b102...` to `cabe...` transition is valid and both records remain immutable.
`core.source_artifacts.identity.compare_artifact_identity` emits the deterministic
field-level explanation. Attempt timestamps, paths, Git SHA, and DuckDB container
bytes are not semantic identity.
