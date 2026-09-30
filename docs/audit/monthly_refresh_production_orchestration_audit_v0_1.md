# Monthly refresh production orchestration audit v0.1

## Finding

The repository has two different meanings of “monthly refresh.” `jobs/monthly_refresh/orchestrator.py`
is a resumable local downstream/serving and regime coordinator over an already-mutated
`market.duckdb`; it verifies rows rather than executing governed source artifacts. The full,
incremental, provisional, and `.github/workflows/refresh_*.yml` jobs mutate or stage provider data
independently. They are useful migration/source boundaries, but are not a transactional cohort.

FRED is the mature vertical slice: its source runner reconciles an exact prior artifact, and
`fred_durable.py` publishes, verifies, catalogs, resolves, and optionally activates it. The current
workflow is an acceptance workflow with dispatch/push triggers and optional manual activation—not
the future production scheduler. Release transport, catalog CAS, immutable artifact validation,
Source Set v2, and canonical-market validation are reusable without redesign.

Redfin already has a managed seven-family inbox, atomic registration, validation, transactional
state reconciliation, artifact emission, and lifecycle metadata. Its current `promoted`/`published`
states mix source/downstream milestones and the existing orchestrator treats them as readiness.
Production instead needs one durable drop-content identity plus cycle-consumption evidence. Inbox
presence is explicitly insufficient.

## Existing commit points and gaps

1. Immutable source publication and catalog insertion are durable evidence and need not roll back.
2. Per-source accepted pointers currently can move independently (`--activate` in the FRED bridge).
3. Source Set v2 pins exact artifact/package/content identities and is the correct cohort boundary.
4. Canonical market and serving promotion are later, distinct commits. The serving database must
   not store orchestration state.
5. There is no common source-result envelope, durable cycle ledger, Redfin consumption marker,
   production cohort fan-out, barrier, or multi-pointer promotion transaction yet.

## Reuse and eventual deprecation

Retain independently runnable provider jobs, artifact/package validation, Release registry/CAS,
Redfin governance, and Source Set v2. After migrated replacements reach acceptance, retire the
legacy `run_refresh_all*`, delete-first full/incremental/provisional mutation paths, the old monthly
serving/regime coordinator as a *master*, per-source production schedules, and the FRED push-based
acceptance bridge. Do not delete them before parity, rollback, and lineage are proven.

No production data, pointer, provider call, Release, schedule, or serving/regime execution was part
of this audit.
