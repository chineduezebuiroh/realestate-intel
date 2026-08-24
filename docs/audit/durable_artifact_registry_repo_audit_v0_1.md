# Durable artifact registry repository audit v0.1

**Audit date:** 2026-08-24  
**Scope:** repository reconnaissance; no runtime, workflow, database, or provider mutation

## Source-artifact framework

| File | Existing behavior | Production implication |
|---|---|---|
| `core/source_artifacts/artifact.py` | Canonicalizes seven columns, writes Zstandard Parquet, hashes members, derives semantic artifact ID, writes manifest and logical directory hash. | Preserve identity semantics; add deterministic archive hash as a separate transport identity. |
| `core/source_artifacts/validation.py` | Verifies manifest, hashes, schema, uniqueness, finite values, source, date bounds, lineage alignment, and Redfin config set. | Reuse after every remote download; expand production source-set/catalog checks separately. |
| `core/source_artifacts/storage.py` | Abstract exact resolver, local URI-to-directory resolver, skeletal publisher. | Correct storage-neutral seam; production publisher needs structured phases/results and a Release resolver. |
| `core/source_artifacts/source_set.py` | Creates/validates partial v1 with exact logical package hash and provider identity. Required equals included; every status is refreshed; empty config hashes accepted. | Introduce v2 rather than treating proof schema as complete production governance. |
| `core/source_artifacts/assembly.py` | Resolves/validates inputs, checks metric ownership/geographies, rejects cross-source duplicate keys, creates candidate DuckDB and metadata; prohibits live DB paths. | Keep acquisition/storage out of assembler. Generalize the explicit vertical-slice gate only after production acceptance. |
| `scripts/validate_source_set.py`, `scripts/build_market_from_artifacts.py` | Construct only `LocalArtifactResolver` from CLI URI mappings. | Future CLI should inject resolver configuration; no GitHub code belongs in assembly. |

## Source producers

### Redfin

`sources/redfin/ingest.py` governs seven-file registration and validation.
`sources/redfin/state.py` owns durable complete-state reconciliation, preserves
prior-only keys/lineage, hashes governed Redfin configs, and emits complete
artifacts. Storage/governance modules separately track raw drop publication,
promotion, baseline, history, quarantine, and retention. The future monthly
command must compose these existing stages and resume publication; it must not
replace the authoritative transformation or replay already committed state.

### FRED macro

`sources/fred_macro/artifact.py` acquires ordinary-current FRED data and uses the
shared preserve-prior behavior. `jobs/monthly_refresh/fred_macro.py` compares
canonical content and emits `refreshed` or `unchanged` reports.
`jobs/monthly_refresh/fred_prior_actions.py` enumerates successful workflow runs,
requires one non-expired evidence artifact, downloads and validates it, and
fails closed on ambiguity. `.github/workflows/fred-monthly-artifact.yml` grants
only contents/actions read, receives `FRED_API_KEY`, runs fixture acceptance,
and uploads 30-day evidence. It deliberately does not publish durably.

## Monthly orchestration and contracts

`jobs/monthly_refresh/orchestrator.py` is a local, lock/checkpoint-driven
pipeline. It inventories local `fact_timeseries`, waits for local Redfin
metadata, invokes source jobs, builds and atomically promotes a serving
candidate, runs Macro Regime/site work, creates a bundle, and optionally calls
`gh release create` plus Pages dispatch. Useful parity to preserve: exclusive
lock, persisted stages, explicit target month, scheduler-success Redfin wait,
fail-closed gates, checksum receipt, exact Pages inputs, and resume behavior.
Behavior to retire after source migration: direct mutation of a mutable full DB,
direct source refresh inventory, local serving promotion as canonical monthly
assembly, and interpreting raw-drop state as the cloud Redfin registry gate.

`config/monthly_refresh_policy.json` separates manual Redfin, monthly sources,
and slower-cadence sources, but still says refresh is verification-only until
legacy delete-first jobs are hardened. The revision v0.2 policy already requires
monthly checks, preserve-prior semantics, source-specific release/failure/stale
rules, immutable canonical retention, and tracked source config. Production
source-set v2 should consume these policies rather than duplicate a hard-coded
inventory.

## GitHub workflows and publication

* `deploy-macro-regime-site.yml` already demonstrates exact Release tag/asset
  download with `gh`, caller-provided SHA-256 verification, safe explicit
  extraction directory, and least-privilege read/Pages permissions.
* The local orchestrator already creates a Macro Regime release, but this is a
  publish bundle, not a source registry or canonical database lifecycle.
* No source-artifact Release publisher, remote source resolver, durable catalog,
  cloud source-set builder, or canonical DB publication workflow exists.
* Legacy `full_refresh.yml`, `refresh_bls.yml`, `refresh_census.yml`,
  `refresh_macro.yml`, and `schedule.yml` still run direct-to-DuckDB/full hosted
  refresh paths. Incremental CES/LAUS and provisional BPS workflows point at
  `data/market_serving.duckdb`; their schedules are commented out. Redfin's
  hosted workflow correctly stops at `waiting_for_manual_redfin`.
* No repository S3/R2/B2 publication pattern was found. No source artifacts are
  tracked through Git LFS. Actions artifacts are used only by the explicit FRED
  acceptance bridge and Pages packaging.

These legacy paths must remain until each source proves governed artifact parity
and receives an explicit retirement decision. They must not be silently run in
parallel as coequal writers after canonical cloud cutover.

## DuckDB lifecycle and downstream assumptions

`core/config.py` names local full and serving databases and fixes serving start
at 2015-01-01. `scripts/build_serving_snapshot.py` attaches the full DB
read-only, copies schema/tables, filters facts from the configured start date,
adds permitted supplemental series, creates indexes and a BPS overlay view, and
builds into a candidate. `jobs/publish_serving_snapshot.py` validates the
candidate and atomically replaces the local serving file; it explicitly does
not commit databases.

The Macro Regime pipeline and feature/config loaders default to
`data/market_serving.duckdb`. This establishes serving as a real downstream
contract, not proof that the 2015 cutoff is analytically required. The repository
contains post-2015 fixture/campaign windows, but no inspected production
contract proves older canonical history is safe to discard. Serving should
remain a curated derivative and its cutoff should be separately measured and
governed.

Observed workspace files at audit time:

| File/object | Bytes | Git tracked |
|---|---:|---|
| `data/market_serving.duckdb` | 15,478,784 | yes |
| `data/market_public.duckdb` | 9,973,760 | yes |
| `data/market.duckdb` | absent | no |
| Accepted Redfin canonical Parquet | approximately 1.23 MB (task acceptance evidence) | no |
| Accepted FRED canonical Parquet | approximately 40 KB (task acceptance evidence) | no |

The two tracked derivative databases are historical repository facts, not a
recommendation to commit future governed databases. The canonical candidate
byte size was not available and must be measured during cloud acceptance.

## Config provenance findings

Source artifacts already hash source-specific governed inputs. A production
source set should hash only files actually used for eligibility or assembly:
monthly refresh policy, revision policy, source metric ownership registry,
geography admission manifest, and a future consumed machine-readable canonical
assembly policy. Feature registries, regime scoring policies, Pages assets, and
unconsumed prose should not affect canonical source-set identity.

## Gaps before production

1. Deterministic safe archive contract and archive hash.
2. Structured two-phase Release publisher and immutable finalization check.
3. Remote exact resolver with numeric asset identity and run-local digest cache.
4. Tracked append-only catalog plus separately mutable accepted pointers and
   compare-and-swap workflow.
5. Production source-set schema with monthly outcomes, eligibility evidence,
   exact transport hashes, required inventory, and config provenance.
6. Cloud cycle coordinator with Redfin prerequisite independent of automated
   source refresh triggers.
7. Canonical DB artifact identity, validator, receipt, retention, and pointer.
8. Serving contract/history study and cloud derivative publication.
9. Per-source parity and explicit legacy retirement plan.
