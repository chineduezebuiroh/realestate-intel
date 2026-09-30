# Source Refresh / Revision Contract v0.2

## Status and authority

This is the review candidate for `source_refresh_revision_v0_2`. Its machine-readable policy is `config/source_refresh_revision_policy_v0_2.json`. Both explicitly remain `production_governed=false` and `runtime_consumed=false` until human promotion. This contract freezes the implementation target; it does not change runtime behavior.

It supersedes the policy recommendations—not the repository observations—in `docs/audit/source_refresh_revision_audit_v0_1.md`.

## Decided architecture

Git stores logic, small specifications, registries, and contracts. Durable release/object storage stores immutable governed canonical source artifacts. GitHub Actions provides disposable acquisition, reconciliation, database construction, analytics, and site-build compute. `market.duckdb` is rebuilt from an explicit source set; `market_serving.duckdb` is rebuilt from that canonical database. Neither database nor material source Parquet belongs in Git.

The monthly cloud gate is a validated Redfin canonical source artifact for the target month—not evidence that a laptop's `market.duckdb` was already mutated.

## Universal truth contract

1. Every external source has `check_cadence=monthly`; provider cadence remains independent.
2. Retrieval and truth are separate. Incremental retrieval does not imply append-only truth.
3. For a revision-capable source, a newer governed provider release wins every canonical key it returns.
4. A previously governed key absent from a newer release survives by default.
5. Retraction is exceptional: deletion requires explicit provider semantics, source-specific governance, and release evidence that distinguishes retraction from omission.
6. No-new-release means reuse the exact prior immutable artifact. It does not mean regenerate equivalent bytes under a new identity.
7. All releases are reconciled before artifact publication. Broad database `DELETE` behavior never defines source truth.

Default canonical key is `(geo_id, metric_id, date, property_type_id)`. `source_id` owns a governed metric namespace through the source registry; it is lineage, not part of the fact primary key. Artifact production must reject unauthorized metric ownership.

## Final source taxonomy

* **Type A — `revisionary_current_truth`:** CES, LAUS, FRED macro, FRED unemployment, BEA quarterly/annual, and Census NRC/FRED. Returned overlap becomes current truth; absence preserves prior; deep reconciliation follows policy.
* **Type B — `immutable_vintage`:** ACS1 and ACS5. A release adds a distinct immutable provider vintage. A prior published vintage is not retroactively mutated under the governed assumption.
* **Type C — `rolling_full_snapshot_manual`:** Redfin. Each rolling snapshot reconciles into durable per-key latest-vintage state; missing old keys survive.
* **Type D — BPS family:** final is `revisionary_compiled_snapshot`; provisional is `provisional_overlay`. Final owns matching eligible keys, provisional fills governed gaps.

## Final source policy matrix

| Source | Type | Monthly check / cadence | Availability | Normal retrieval | Deep reconciliation | Overlap / absence | Artifact behavior / cloud generation | Blocking policy | Open issue |
|---|---|---|---|---|---|---|---|---|---|
| Redfin | rolling manual snapshot | required / monthly | exact target canonical artifact in durable storage | seven-family local rolling snapshot reconciled with durable state | every release | newest vintage wins / preserve prior | local complete canonical artifact, immutable upload; no cloud fetch | missing=`waiting`; invalid blocks analytics/publication | artifact backend/credential convention only; preservation model decided |
| CES | revisionary current truth | required / monthly | governed latest-release/period probe | ≈3-year overlap | annual deep/full benchmark reconciliation; no hardcoded max depth | newer wins / preserve prior | cloud complete canonical snapshot; reuse on no release | detected-release failure or stale lag blocks publication | exact probe and lag |
| LAUS | revisionary current truth | required / monthly | release/period + series-map identity | ≈3-year overlap | annual ≥5 years, full preferred; reconcile geo/series remaps | newer wins / preserve prior | cloud complete canonical snapshot | detected-release failure or stale lag blocks publication | exact probe/lag and remap evidence |
| BPS final | revisionary compiled snapshot | required / monthly | compiled release ID discovery | latest compiled full snapshot | every new compiled release | newer wins / preserve prior pending retraction proof | cloud complete final artifact | detected-release failure blocks publication | suppression/retraction semantics |
| BPS provisional | provisional overlay | required / monthly | common per-level release ID | current per-level files reconciled with prior provisional | every release until final ownership | newer provisional wins / preserve prior | cloud complete provisional artifact; BPS-family resolver consumes it | warn if final current, otherwise block combined BPS freshness | suppression/retraction and discovery stability |
| ACS1 | immutable vintage | required / annual | latest published ACS1 vintage/year | one new vintage | new-vintage validation only | prior vintage immutable; missing expected rows fail | separate cloud artifact per ACS1 release | due release failure/stale annual lag blocks publication | exact lag/probe |
| ACS5 | immutable vintage | required / annual | latest published ACS5 vintage/year | one new vintage | new-vintage validation only | prior vintage immutable; missing expected rows fail | separate cloud artifact per ACS5 release | due release failure/stale annual lag blocks publication | exact lag/probe |
| FRED macro | revisionary current truth | required / series-dependent | ordinary-current observation probe | full ordinary FRED histories | monthly full retrieval | current value wins / preserve prior | cloud complete canonical snapshot | missing secret or required-release failure blocks publication | exact series lags |
| FRED unemployment | revisionary current truth | required / monthly | ordinary-current observation probe | full ordinary FRED histories | monthly full retrieval | current value wins / preserve prior | separate cloud canonical snapshot | same as FRED macro | exact lag |
| BEA quarterly | revisionary current truth | required / quarterly release | governed BEA release/version | `Year=ALL` | every detected release | newest release wins / preserve prior | cloud complete canonical snapshot | detected-release failure/stale lag blocks publication | exact release API and lag |
| BEA annual | revisionary current truth | required / annual release | governed BEA release/version | `Year=ALL` | every release including benchmark reconciliation | newest release wins / preserve prior | cloud complete canonical snapshot | detected-release failure/stale lag blocks publication | exact release API and lag |
| Census NRC/FRED | revisionary current truth | required / monthly | ordinary-FRED current observation probe | full current histories | monthly full retrieval | newest returned wins / preserve prior | cloud complete canonical snapshot | detected-release failure/stale lag blocks publication | release identity and lag |

## Source-specific semantics

### Redfin durable latest-vintage state

The immutable July baseline remains unchanged. Local `canonical_redfin_state` is a compact, deterministic canonical table with one row per fact key plus `value`, `property_type`, `latest_source_vintage`, `latest_source_hash_or_drop_id`, and `promoted_at`.

Bootstrap materializes the immutable baseline into this separate state. A validated new drop is reconciled transactionally:

* new returned key → insert with new vintage lineage;
* returned overlap → replace value and lineage, even if the value is unchanged;
* prior key absent → retain the existing row unchanged;
* deletion → prohibited without a later governed Redfin retraction contract.

After reconciliation, the complete state is validated and emitted as the canonical Redfin source artifact. A raw drop cannot become retention-eligible until the published artifact and durable state prove that all its canonical contributions remain reconstructible. Parquet is preferred for published interchange; a local DuckDB or atomic Parquet state is acceptable implementation detail if deterministic, transaction/candidate-safe, and independently backed up. The baseline itself is never rewritten.

### Redfin managed inbox

The future local surface is `data/redfin/raw/incoming/`. The operator overwrites it with seven current exports and runs one command. Candidate logic must:

1. identify exactly one nation/state/metro/county/city/neighborhood/ZIP family;
2. reject missing, duplicate, and unknown families;
3. inspect each endpoint and require one common latest source month;
4. derive `drop_id=YYYY-MM` without operator input;
5. hash files and compare with any registered drop;
6. return idempotently for identical registration and fail closed on conflict;
7. atomically copy/move into immutable `drops/<drop_id>/`, write metadata, validate, reconcile durable state, and emit the canonical artifact.

Inbox contents remain local and mutable; registered drops, state, and published artifacts are immutable/governed. Inbox implementation is explicitly deferred.

### CES and LAUS

CES annual benchmark revisions may extend materially beyond ordinary preliminary revisions; v0.2 deliberately does not claim a maximum depth. LAUS annual reconciliation must cover at least approximately five years and preferably full history. LAUS reconciliation treats series/geography mapping changes as first-class changes: mappings, retired/replacement series, and coverage must be reviewed before value reconciliation.

### ACS

ACS1 and ACS5 remain separate identities. A new provider year/vintage creates a new immutable artifact. The canonical representation must retain enough vintage identity to prevent accidental replacement of a prior vintage. Under the accepted assumption, prior published API vintages are frozen. A future provider correction policy requires a contract change rather than generic absence/revision handling.

### FRED

Ordinary FRED is the governed current-truth interface. Monthly full retrieval supplies current revised history. ALFRED is not required by v0.2; it becomes relevant only if point-in-time historical-vintage reconstruction is separately approved.

### BPS family precedence boundary

Final and provisional remain separate source artifacts for independent provenance and reuse. A **BPS-family canonical resolver**, executed after both source-specific reconciliations but before the generic market assembler, produces a complete resolved BPS family artifact/view:

* final owns an eligible matching canonical key;
* provisional fills only keys not owned by final;
* each lineage remains traceable in a sidecar/row source field;
* absence within either source preserves its own prior state until explicit suppression/retraction semantics are governed.

The generic assembler validates and loads the resolved family result; it does not implement provider-specific precedence. This keeps source-family logic out of generic assembly while preserving both input artifacts for audit.

## Monthly outcomes

Allowed source statuses are `refreshed`, `reused`, `no_new_release`, `no_new_release_expected`, `waiting_for_manual_source`, `availability_check_failed`, `refresh_failed`, `validation_failed`, and `stale_beyond_governed_lag`. The source-set manifest records both the check result and resolved artifact. A check failure may warn only while the prior artifact remains inside an approved lag; exact lags remain unavailable until governed.

## Decided versus open

### DECIDED

* Target is cloud source-artifact generation plus reproducible from-scratch canonical assembly.
* Monthly checks, source-specific publication cadence, newest-governed-overlap, preserve-prior absence, exceptional retractions, source-level immutable artifacts, exact artifact reuse, and no material datasets in Git.
* Redfin rolling-state preservation model, ACS immutable-vintage model, ordinary FRED current-truth model, and BPS final/provisional precedence boundary.
* Serving remains downstream packaging with no provider reconciliation.

### OPEN / EXTERNAL VERIFICATION

* BPS suppression/retraction meanings and provisional discovery stability.
* Exact availability mechanisms and governed publication-lag thresholds for BLS, ACS, FRED, BEA, NRC, and BPS.
* Durable storage backend, retention duration, asset-size threshold, and credentials.
* Provider release-ID fallback where no durable native release ID exists.
* Exact LAUS series/geography remap evidence format.

Open questions cannot weaken preserve-prior or the approved Redfin preservation model. They must be resolved before runtime promotion of affected policy fields.
