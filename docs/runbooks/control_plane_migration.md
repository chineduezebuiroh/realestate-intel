# Governed July control-plane migration

## Boundary

`main` is the durable production control-plane branch.  The temporary
`monthly-refresh-orchestration` integration branch contains the coherent July
evidence produced by migration-era hosted source workflows.  The manual
`control-plane-migration.yml` workflow performs a one-time, exact bootstrap of
that evidence into `main`; it is not source execution or cohort promotion.

The adapter is pinned to
`monthly_cycle__2026-07__7cab1c5df177a1e4`, source branch
`monthly-refresh-orchestration`, target branch `main`, and the frozen BPS
resolution/republication identities.  It has no provider client or Release
publisher.

## Exact closure

The source objects are five physical monthly execution results, the exact BPS
family resolution, both BPS r2 republication records, and the unconsumed Redfin
readiness record.  Every string in that evidence carrying an exact `src__`
identity is resolved in the source catalog.  This includes candidate, prior,
parent, superseded, and logical BPS artifacts.  Only those immutable catalog
records absent on `main` are added.

The catalog merge preserves every existing `main` record and preserves the
entire `accepted` value byte-for-value.  An existing record with the same
object identity must have the same immutable identity.  URI, asset, namespace,
or content contradictions fail catalog validation.  Evidence paths use
create-once/exact-reuse semantics.  Readiness is merged as one exact unconsumed
record and validated against the merged catalog.

## Ordering and recovery

Preflight performs remote reads and emits the exact plan only.  Live execution:

1. rechecks the frozen `main` catalog blob SHA and accepted-pointer pre-state;
2. creates the deterministic `prepared` migration record;
3. writes the merged catalog with the GitHub Contents blob precondition;
4. creates/reuses immutable evidence in canonical path order;
5. creates/reuses unconsumed readiness last;
6. rereads the catalog, proves accepted pointers unchanged, and marks the
   migration record `complete`.

A stale initial catalog writes nothing.  A failure after preparation resumes
against the same migration identity.  An exact completed rerun is a no-op.
Neither mode mutates `monthly-refresh-orchestration`.

## Registration and tests

The workflow is manual-only and becomes dispatchable after this reviewed change
is merged to the default branch.  Do not dispatch it during implementation.

```bash
PYTHONPATH=. python scripts/smoke_tests/205_control_plane_migration.py
PYTHONPATH=. python scripts/smoke_tests/203_cohort_promotion_hosted.py
git diff --check
```

## Non-mutating hosted preflight

```bash
gh workflow run control-plane-migration.yml --ref monthly-refresh-orchestration \
  -f intent=preflight -f confirmation=''
```

## Controlled live migration

Run only after reviewing a successful preflight and confirming no other writer
is active on the `main` catalog:

```bash
gh workflow run control-plane-migration.yml --ref monthly-refresh-orchestration \
  -f intent=live -f confirmation=MIGRATE_GOVERNED_JULY_CONTROL_PLANE
```

## Post-migration verification

```bash
gh run list --workflow control-plane-migration.yml --limit 5
gh run view <RUN_ID> --log
gh api 'repos/chineduezebuiroh/realestate-intel/contents/config/artifact_catalog.json?ref=main' \
  --jq '.content' | tr -d '\n' | base64 -d | jq '.accepted'
gh api 'repos/chineduezebuiroh/realestate-intel/contents/config/monthly_refresh_readiness.json?ref=main' \
  --jq '.content' | tr -d '\n' | base64 -d | jq \
  '.records[] | select(.readiness_id=="redfin_readiness__monthly_cycle__2026-07__7cab1c5df177a1e4")'
gh api 'repos/chineduezebuiroh/realestate-intel/contents/config/control_plane_migrations?ref=main' \
  --jq '.[].name'
gh api 'repos/chineduezebuiroh/realestate-intel/contents/config/cohort_promotion_records?ref=main' \
  --silent >/dev/null && echo 'unexpected promotion record directory' || true
```

After verification, rerun the existing cohort-promotion **preflight**.  Its
durable authority remains `--branch main`.

## Side-effect matrix

| Authority | Preflight | First live migration | Exact live rerun |
|---|---|---|---|
| Provider discovery/ingestion | none | none | none |
| Source Releases/artifact bytes | read none | unchanged | unchanged |
| `monthly-refresh-orchestration` | read only | read only | read only |
| `main` immutable catalog records | compare | add required missing records | exact reuse |
| `main` accepted pointers | compare | unchanged | unchanged |
| Monthly/BPS evidence | compare | create missing exact JSON | exact reuse |
| Redfin readiness | compare | create unconsumed exact record | exact reuse |
| Migration record | none | prepared, then complete | exact reuse |
| Source Set/canonical Releases | none | none | none |
| Cohort promotion record | none | none | none |
| Serving data/pointer | none | unchanged | unchanged |
