# Governed July cohort promotion live adapter

## Registration and authority

The manual workflow `.github/workflows/cohort-promotion-live.yml` becomes
dispatchable only after this reviewed change is merged to the default branch.
It has no schedule, push trigger, monthly fan-out, provider discovery, or source
ingestion. Registering the workflow means merging it; do not dispatch it as part
of implementation review.

The sole permission is `contents: write`. It is needed to create immutable
GitHub Releases and to update tracked control-plane JSON by the GitHub Contents
API. No Actions, deployments, Pages, issues, or package permission is used.

Durable state is stored at:

* immutable Source Set Release `source-set/<source_set_id>` and catalog record
  in `config/artifact_catalog.json`;
* immutable canonical Release `canonical-market/<market_artifact_id>` and the
  same catalog;
* prepared record
  `config/cohort_promotion_records/<cycle_id>.json` (containing the deterministic
  `promotion_id`);
* accepted pointers in `config/artifact_catalog.json`;
* exact readiness consumption in `config/monthly_refresh_readiness.json`.

## Review and preflight

```bash
PYTHONPATH=. python scripts/smoke_tests/202_cohort_promotion.py
PYTHONPATH=. python scripts/smoke_tests/203_cohort_promotion_hosted.py
PYTHONPATH=. python scripts/smoke_tests/204_cohort_promotion_live_adapter.py
git diff --check
```

Exact non-mutating hosted preflight (safe to run after workflow registration):

```bash
gh workflow run cohort-promotion-live.yml --ref monthly-refresh-orchestration \
  -f cycle_id=monthly_cycle__2026-07__7cab1c5df177a1e4 \
  -f intent=preflight -f confirmation=''
```

The same production adapter can be run locally against the exact durable July
state on `main`. For a public repository it uses unauthenticated read-only API
access when `GITHUB_TOKEN` is absent; setting a token only increases API rate
limits. Keep the workspace to reuse immutable packages by package SHA-256:

```bash
PYTHONPATH=. python -m jobs.monthly_refresh.cohort_promotion_hosted \
  --repository chineduezebuiroh/realestate-intel --branch main \
  --cycle-id monthly_cycle__2026-07__7cab1c5df177a1e4 \
  --workspace .cache/cohort-promotion-july --git-sha local-preflight \
  --output .cache/cohort-promotion-july-report.json
```

Omitting `--live` constructs the logical Source Set, resolves and validates all
exact immutable source packages, validates metric ownership, assembles the
isolated canonical database, and validates the prepared promotion plan. It
does not publish objects, write GitHub state, move accepted pointers, consume
Redfin readiness, or perform provider discovery. Read-only transport rejects
non-GET API calls locally as an additional guard.

## Controlled live dispatch (do not run during implementation)

```bash
gh workflow run cohort-promotion-live.yml --ref monthly-refresh-orchestration \
  -f cycle_id=monthly_cycle__2026-07__7cab1c5df177a1e4 \
  -f intent=live -f confirmation=PROMOTE_GOVERNED_COHORT
```

The adapter publishes and verifies Source Set, then canonical candidate, then
persists the deterministic prepared record **before** accepted-state mutation.
It performs one CAS and remote reread per operation, in frozen order:

1. `accepted.source_set`;
2. `accepted.canonical_market`;
3. `accepted.source.bps`;
4. `accepted.source.ces`;
5. `accepted.source.fred_macro`;
6. `accepted.source.laus`;
7. `accepted.source.redfin`;
8. exact Redfin readiness `consumed=true`.

At each boundary, target means already complete, expected-old means pending,
and every other value is a contradiction. A failed/stale blob precondition
writes nothing; rerunning rereads durable state and resumes. Readiness cannot be
consumed until every prior target is remotely confirmed. Completion includes an
additional exact no-op recovery pass.

## Post-run verification

```bash
gh run list --workflow cohort-promotion-live.yml --limit 5
gh run view <RUN_ID> --log
gh api 'repos/chineduezebuiroh/realestate-intel/contents/config/artifact_catalog.json?ref=main' \
  --jq '.content' | tr -d '\n' | base64 -d | jq '.accepted'
gh api 'repos/chineduezebuiroh/realestate-intel/contents/config/monthly_refresh_readiness.json?ref=main' \
  --jq '.content' | tr -d '\n' | base64 -d | jq \
  '.records[] | select(.readiness_id=="redfin_readiness__monthly_cycle__2026-07__7cab1c5df177a1e4")'
gh api 'repos/chineduezebuiroh/realestate-intel/contents/config/cohort_promotion_records?ref=main' \
  --jq '.[].name'
```

Then run the exact preflight command again. It must reproduce the same IDs and
must not write. A reviewed exact live rerun must report a complete no-op.

## Side-effect matrix

| Authority | Preflight | First live run | Exact live rerun |
|---|---|---|---|
| Provider APIs / discovery | none | none | none |
| Source artifacts / physical BPS | none | none | none |
| Source Set Release/catalog record | none | create or exact reuse | exact reuse |
| Canonical Release/catalog record | none | create or exact reuse | exact reuse |
| Prepared promotion record | none | create before activation | exact reuse |
| Accepted logical pointers | none | eight ordered boundaries excluding readiness | no-op |
| Physical BPS accepted pointers | none | absent/unchanged | absent/unchanged |
| Exact Redfin readiness | none | consumed last | already-consumed no-op |
| Serving authority / DuckDB | none | unchanged | unchanged |
| Downstream serving processing | none | not started | not started |

## July 2026 live completion record

The first live governed cohort promotion completed successfully through the
shared hosted cohort-promotion lifecycle for
`monthly_cycle__2026-07__7cab1c5df177a1e4`. The durable promoted identities are:

* Source Set: `source_set__2026-07__v2__b80b9554de5208c4`;
* canonical market: `market__2026-07__r1__89f7e097af74f87d`;
* promotion: `cohort_promotion__89964f95cfe330e995de8253`.

Terminal verification reported `source_set_accepted=true`,
`canonical_accepted=true`, and `complete=true`. The recoverable transaction
advanced, in order, `accepted.source_set`, `accepted.canonical_market`, the
logical pointers `accepted.source.bps`, `accepted.source.ces`,
`accepted.source.fred_macro`, `accepted.source.laus`, and
`accepted.source.redfin`, and then consumed the exact Redfin readiness record
last. No provider discovery occurred. No `accepted.source.census_bps` or
`accepted.source.census_bps_provisional` pointer was introduced: BPS acceptance
remains exclusively the logical `accepted.source.bps` authority.

Serving-market mutation was outside this transaction and remains outstanding.
This completion proves the shared live cohort machinery for the sources migrated
so far; it is not the final intended initial Source Set v2/full-cohort canonical
market milestone. Remaining governed source integrations still precede that
milestone, beginning with ACS.

The controlled July promotion described by this runbook is therefore complete.
Future promotions retain the same fail-closed preflight, single-writer, exact-ID,
and recoverable-order requirements; any contradiction or stale CAS must be
investigated rather than overwritten.
