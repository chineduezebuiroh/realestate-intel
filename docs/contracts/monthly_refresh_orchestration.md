# Monthly refresh orchestration contract

## Boundary and target identity

Monthly production is a local authoritative operation. Redfin's seven manually downloaded families remain under ignored `data/redfin/raw/`, and the mutable source database remains `data/market.duckdb`. GitHub Actions neither observes nor mutates either resource. Explicit `YYYY-MM` deterministically identifies `monthly_refresh_YYYYMM_v1` and `macro_regime_production_YYYYMM_v1`. July 2026 is the already-applied bootstrap; orchestration does not reconstruct it.

The GitHub boundary begins only after local analytics, serving validation, regime validation, static build, and immutable bundle construction. `--publish` explicitly creates a named Release and dispatches Pages with the exact run, tag, asset, and SHA-256. Without it, no GitHub authentication is needed.

## Source inventory and safety decision

| Source | Class/cadence | Existing entry point | Monthly behavior and safety |
|---|---|---|---|
| Redfin | manual/monthly | governed v2 lifecycle scripts | exact-month prerequisite; blocks until transactionally applied |
| CES | required/monthly | `jobs.full_refresh.run_refresh_bls_ces` | freshness verification; delete-first job is not auto-invoked |
| LAUS | required/monthly | `jobs.full_refresh.run_refresh_bls_laus` | freshness verification; delete-first job is not auto-invoked |
| FRED macro/unemployment | required/daily-monthly | source modules only | verification; no governed full-refresh job |
| Census BPS/provisional | required/monthly | full job/source module | verification; delete-first transform is not auto-invoked |
| BEA quarterly/annual GDP | slower/quarterly-annual | source modules | unchanged is `no_new_release_expected` |
| ACS 1/5-year | slower/annual | `jobs.full_refresh.run_refresh_census_acs` | unchanged is expected; delete-first transform is not auto-invoked |
| Census NRC/FRED | governed/monthly | source modules | verification; no governed full-refresh job |

There is no `jobs/incremental/` directory. Several full refreshes delete production rows and do not expose a candidate database plus promotion contract. V1 does not pretend they are safe: it records deterministic pre/post evidence and verifies the locally refreshed full DB. Transactional hardening and governed publication-lag thresholds remain blockers before automatic fetching. Operators continue controlled source procedures first. This is safer than embedding partially successful delete-first network jobs.

## State, evidence, and resume

State is atomically persisted under ignored `artifacts/monthly_refresh/months/<target>.json`; run evidence is under `artifacts/monthly_refresh/runs/<run_id>/`. `manifest.json` records identities, state, completed gates, failure, and output identities. `source_refresh.json` records per-source cadence, timestamps, pre/post dates and counts, validation, warnings, and errors. `freshness.json`, `publication.json`, the deterministic bundle, and transient `site/` complete the evidence.

States advance through `waiting_for_manual_redfin`, `ready`, `sources_validated`, `serving_candidate_built`, `serving_validated`, `serving_promoted`, `regime_built`, `regime_validated`, `site_built`, `publish_bundle_created`, optionally `publication_dispatched`, then `analytics_complete` or `complete`. Exceptions persist `failed` and the exact gate. Completed gates are skipped on resume. Publication failure preserves `analytics_complete=true`. A fully published month returns `already_complete` without mutation.

The exclusive lock contains PID, host, month, and UTC creation time. A second process fails before mutation. Dead-PID recovery requires `--recover-stale-lock` after operator verification; a live lock is never stolen.

## Redfin gate

Missing, registered, validated, candidate-built, and candidate-validated states return clean `waiting_for_manual_redfin` and the precise next governed command. These are scheduler-success no-ops. The chain starts only once the target was transactionally applied (`serving_refreshed`) or published/promoted. It never reapplies July or bypasses v2.

## Serving, analytics, and publication

Serving construction writes a run-specific candidate on the same filesystem. It never removes the live database. Existing validation runs on the candidate, followed by an exact target-month Redfin check; only then does `Path.replace()` atomically promote after connections close. Failure preserves live bytes.

`config/monthly_refresh_policy.json` selects settled version-controlled registries, not an August artifact, and changes no scoring/calibration. The existing runner receives target month, refresh run, Git SHA, serving identity, and policy pointer. A complete nonempty regime manifest gates the existing DC-only site builder.

Pages dispatch now requires explicit immutable inputs rather than mutable variables or `latest`. It verifies checksum before extraction and runs Smoke 108, preserving relative URLs, embedded Plotly, and no browser DuckDB runtime.

## Commands and scheduling

```bash
PYTHONPATH=. python -m jobs.monthly_refresh --target-month 2026-08 --status
PYTHONPATH=. python -m jobs.monthly_refresh --target-month 2026-08
PYTHONPATH=. python -m jobs.monthly_refresh --target-month 2026-08 --publish
```

Exit zero means waiting, already complete, analytics complete, or complete. Nonzero means lock contention or a failed gate. Local cron/`launchd` may retry the non-publish command; GitHub cron cannot replace it. Fix a persisted failure and rerun; completed gates are skipped. Never delete state to force a second monthly identity.

## Staged first-production acceptance

1. **Status stop:** run `--status`; verify July baseline, inventories, and Redfin next action.
2. **Fixture stop:** run Smokes 160–163 and serving checks.
3. **Readiness stop:** register/validate the real drop using printed commands.
4. **Full-DB stop:** run controlled source procedures and Redfin apply; validate dates/counts.
5. **Candidate stop:** build an explicit serving candidate and validate with `SERVING_DUCKDB_PATH`.
6. **Promotion stop:** promote only the accepted candidate; validate live serving.
7. **Regime stop:** run without `--publish`; verify the immutable manifest.
8. **Site stop:** inspect generated site via `python -m http.server`.
9. **Bundle stop:** independently compare `sha256sum` with `publication.json`.
10. **GitHub stop:** run `--publish`; inspect Release, exact workflow inputs, Pages smoke/deployment, root page, and direct DC page.

Remaining first-run blockers are source-job transactional hardening, formal publication-lag thresholds, and operator confirmation that required online sources were refreshed. Raw files and databases are never committed or uploaded by the analytical phase.
