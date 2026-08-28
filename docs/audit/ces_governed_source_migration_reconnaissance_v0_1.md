# CES governed-source migration reconnaissance v0.1

**Status:** design evidence only; no runtime policy, artifact, pointer, database, or
workflow behavior is changed by this document.  Reconnaissance date: 2026-08-27.

## 1. Executive conclusion

CES **can and should be migrated primarily as a thin, source-specific adaptation
of the proven FRED governed-source pattern**.  It needs no new registry,
publication mechanism, catalog, package format, cohort result schema, or barrier.
The reusable spine is prior durable resolution, canonical artifact construction,
immutable GitHub Release publication, catalog registration, publication
re-resolution, fail-closed result construction, and sibling cohort execution.

The thin adaptation is nevertheless substantive at the provider boundary:

* CES makes many batched BLS v2 POST requests rather than one client call per
  small national series set.  Its tracked 672-row specification currently owns
  50 state/metro geographies and 26 physical SA/NSA metric IDs.
* The intended governed scope is only three **SA** canonical metrics
  (`ces_total_nonfarm_sa`, `ces_total_private_sa`, and
  `ces_construction_sa`), while the legacy loader currently persists every
  available supersector/seasonal combination.  That scope mismatch must be
  resolved explicitly in CES-A; it must not be inherited accidentally.
* BLS ordinary API results are current truth, not immutable vintages.  Routine
  revisions and annual benchmark revisions mean monthly overlap reconciliation
  plus periodic full-history reconciliation are required.  Missing response
  keys are omissions, not governed retractions, and must preserve prior truth.
* The API response used by the repository has no governed release identifier or
  retrieval-vintage field.  The artifact target should be inferred from the
  maximum canonical monthly observation, independently of the Redfin catalyst.
  The release identity should therefore be a deterministic hash of the governed
  request specification and canonical acquired response, not an invented BLS
  release date.
* Neither checked-in DuckDB is a sufficient accepted bootstrap authority.  The
  serving DB is mutable legacy state (50 geographies, max 2026-05); the public DB
  is a narrower, older serving export (7 geographies, max 2025-08); there is no
  CES catalog record or accepted pointer.  The safest choice is bootstrap **B**:
  one controlled full provider reconstruction, reconciled and equivalence-audited
  against the legacy serving slice, followed by separate governed publication and
  human activation before monthly execution is enabled.

No live BLS request was necessary or made.  Provider limits and benchmark/release
details below are repository-derived where possible and are explicitly marked for
official external verification where the repository does not prove them.

## 2. Existing execution graph and authority

### 2.1 Current production-shaped path

```text
config/ces_series.generated.csv (tracked series/geo/metric mapping)
       │
       ▼
BLS public API v2 POST /timeseries/data/
       │  chunks of 50 series × one or more <=20-year windows
       ▼
sources.bls_ces.ingest.fetch_series / to_df
       │  M01..M12 -> calendar month-end; M13 normally discarded
       │  series prefix -> SA/NSA suffix; property_type_id="all"
       ▼
mode-dependent DELETE + key upsert (not one explicit transaction)
       │
       ├─ full job -> data/market.duckdb (not checked in here)
       └─ incremental job -> data/market_serving.duckdb
       ▼
sources.bls_ces.validate (post-mutation coverage/continuity checks)
       ▼
serving/app, canonical-resolution, feature, Demand/Supply consumers
```

The only directly runnable CES Actions entry point is the manually dispatched,
schedule-commented `.github/workflows/refresh_bls_ces.yml`.  It runs the
incremental job against the checked-in serving database.  The full-refresh job is
also called by the broader full-refresh workflow.  Neither path creates a
governed source artifact, retains provider response/vintage evidence, validates a
candidate before mutation, or participates in the production cohort.

### 2.2 Authoritative inventory

| Role | Authoritative/current file | Finding |
|---|---|---|
| series generation | `sources/bls_ces/expand_spec.py` | Downloads `sm.series` and AllData, selects current series from geo manifest; AllData is downloaded but not used in generation. |
| tracked source specification | `config/ces_series.generated.csv` | 672 unique BLS series, 50 geographies, 13 bases, SA/NSA availability. |
| acquisition/canonicalization/mutation | `sources/bls_ces/ingest.py` | Current executable implementation; directly mutates DuckDB. |
| legacy validation | `sources/bls_ces/validate.py` | Post-mutation, permissive by default; expected membership is reduced incorrectly to only total-nonfarm SA/NSA. |
| full job | `jobs/full_refresh/run_refresh_bls_ces.py` | Targets `data/market.duckdb`; delete-all CES behavior. |
| incremental job | `jobs/incremental_refresh/run_refresh_bls_ces.py` | Targets `data/market_serving.duckdb`; three-year overlap by default. |
| CES workflow | `.github/workflows/refresh_bls_ces.yml` | Manual legacy mutation path; schedule is disabled; `BLS_API_KEY` secret convention exists. |
| broader legacy workflow | `.github/workflows/refresh_bls.yml`, `.github/workflows/full_refresh.yml` | Orchestration references; not governed CES boundaries. |
| geography identity | `config/geo_manifest.generated.csv` | Static included CES membership and hard-coded BLS combined state/area code. |
| governed logical metrics | `config/source_metric_registry.csv` | Three enabled SA metrics at state/CBSA level. |
| revision target contract | `config/source_refresh_revision_policy_v0_2.json`, `docs/contracts/source_refresh_revision_v0_2.md` | CES is `revisionary_current_truth`; preserve prior omissions; three-year monthly overlap and annual deep reconciliation. Candidate remains non-runtime policy. |
| cohort intent | `config/monthly_refresh_policy.json` | CES is required and dependency-free, but execution boundary is pending. |
| downstream selection | `config/metric_dimension_registry.csv`, `config/indicator_regime_registry.csv`, `config/feature_registry.csv` | Total nonfarm is active Demand input; private and construction are diagnostic/other-policy inputs. |
| serving/UI | `app.py`, `scripts/build_serving_snapshot.py`, `scripts/make_public_db.py` | CES labels and serving copies consume physical CES rows. |
| existing CES-adjacent tests | smoke 49, 50, 56, 57 and 106 | Prove downstream CES/LAUS precedence and features, not acquisition or governed artifacts. |

No abandoned alternative CES adapter or governed CES artifact runner was found.
The experimental/legacy Redfin filenames containing “ces” are false filename
matches.  Existing CES audit prose is historical evidence, not executable code.

## 3. Current CES data contract

### 3.1 Provider series generation and metrics

`expand_spec.py` selects BLS State and Area CES (`SM`) series with data type `01`
(all employees), seasonality `S` or `U`, and these supersectors:

| supersector | metric base | physical canonical IDs |
|---:|---|---|
| 00 | `ces_total_nonfarm` | `_sa`, `_nsa` |
| 05 | `ces_total_private` | `_sa`, `_nsa` |
| 10 | `ces_mining_logging` | `_sa`, `_nsa` where available |
| 20 | `ces_construction` | `_sa`, `_nsa` where available |
| 30 | `ces_manufacturing` | `_sa`, `_nsa` where available |
| 40 | `ces_trade_transport_utilities` | `_sa`, `_nsa` where available |
| 50 | `ces_information` | `_sa`, `_nsa` where available |
| 55 | `ces_financial_activities` | `_sa`, `_nsa` where available |
| 60 | `ces_prof_business_services` | `_sa`, `_nsa` where available |
| 65 | `ces_education_health_services` | `_sa`, `_nsa` where available |
| 70 | `ces_leisure_hospitality` | `_sa`, `_nsa` where available |
| 80 | `ces_other_services` | `_sa`, `_nsa` where available |
| 90 | `ces_government` | `_sa`, `_nsa` where available |

The generated BLS series ID is selected from `sm.series`, not calculated at
ingest time.  Structurally it encodes `SMS`/`SMU`, state, area, industry, and data
type, but the tracked CSV must remain the request authority.  The series-prefix
seasonality wins over its CSV seasonality column.

The values are monthly **employment levels**, not month-over-month changes,
rates, or indexes; no aggregation or scale conversion is performed.  The source
registry calls the unit “Jobs,” metric metadata calls it “people,” and legacy dim
creation sometimes calls unknown metrics “value.”  BLS CES employment series are
normally expressed in thousands, so CES-A must confirm the official unit and
choose one governed representation (preferably retain provider numeric scale and
declare `thousands_of_jobs`, or deterministically multiply by 1,000 and declare
`jobs`).  Publication must fail until this ambiguity is resolved.

The settled governed logical scope presently names only:

```text
BLS selected total-nonfarm SA series -> ces_total_nonfarm_sa -> ces_total_nonfarm
BLS selected total-private SA series -> ces_total_private_sa -> ces_total_private
BLS selected construction SA series  -> ces_construction_sa  -> ces_construction
```

Availability is asymmetric: total-nonfarm SA exists for all 50 configured geos,
but the current specification contains total-private SA for only 5 and
construction SA for only 4.  Consequently “expected membership” must be based on
the exact governed series registry, not a metric × geography Cartesian product.
The 23 extra physical SA/NSA metrics in the mutable DB must not silently enter the
governed artifact merely because legacy acquisition can fetch them.

### 3.2 Geography

Membership is static and registry-driven at run time.  The generator combines
BLS `state_code + area_code`, strips non-digits, and performs an exact lookup
against `bls_ces_area_code` for rows with `include_ces=1` in the generated geo
manifest.  It does not dynamically accept provider areas.

The tracked series spec currently contains 50 canonical geographies: 6 state
identities and 44 CBSA-metro identities (672 series rows comprise 126 state and
546 metro rows).  CES has no current nation, county, ZIP, city, or neighborhood
scope in the generated spec.  Some legacy docstrings/examples mention national
CES; they do not describe the tracked result.  Crosswalk changes are semantic
configuration changes and require review, not provider auto-discovery.

### 3.3 Time

* Provider frequency: monthly, plus optional annual average `M13` because the
  request sets `annualaverage=true`.
* Canonical date: last calendar day of the observation month.  `M13` is dropped
  whenever that year contains monthly observations; its fallback to December 31
  should be removed from the governed adapter because an annual average is not a
  December monthly observation.
* Legacy full range: wall-clock current year minus 59 through current year in
  non-overlapping 20-year windows.  Incremental: current year minus three through
  current year.  This wall-clock-derived request is not a sufficient deterministic
  governed request identity and can truncate older history.
* Partial current year is normal; partial/current observation **month** means the
  latest released monthly estimate can be present before later revision.  There
  are no intra-month dated facts—the canonical key remains month-end.
* Repository snapshots prove differing maxima: serving CES ends 2026-05-31;
  public CES ends 2025-08-31.  These are observations of mutable checked-in state,
  not a freshness promise or a provider release identity.

### 3.4 Governed canonical adapter

The current `to_df` is close but not yet a governed adapter.  It must emit exactly
the source-artifact fact schema:

```text
geo_id              <- tracked series row.geo_id
metric_id           <- tracked metric_base + series-derived "_sa"/"_nsa"
date                <- month-end(year, M01..M12); reject M13
property_type_id    <- "all"
value               <- finite numeric under the decided unit/scale contract
source_id           <- "ces"
property_type       <- "all" (artifact compatibility column)
```

It must reject unknown/duplicate series, missing metadata, invalid months, null or
non-finite values, rather than silently skipping them.  Canonical artifact key is
`(geo_id, metric_id, date, property_type_id)`.

## 4. Provider acquisition semantics

The operational endpoint is authenticated-or-public BLS API v2 JSON POST:
`https://api.bls.gov/publicAPI/v2/timeseries/data/`.  `BLS_API_KEY` is optional in
legacy code and is passed as `registrationkey`; the existing workflow sources it
from GitHub secret `BLS_API_KEY`.  Governed hosted execution should require that
secret because the current 50-series batches depend on registered limits.

Current request facts:

* up to 50 series per repository batch;
* one request per batch per year window; 672 rows imply 14 batches per window,
  hence 14 incremental requests and 42 full requests at the current three-window
  full depth;
* at most 20 inclusive years per window;
* `annualaverage=true` although governed monthly output does not need M13;
* 60-second per-request timeout, 0.5-second sleep between series batches;
* no pagination; requested series and years are the partitioning mechanism;
* `raise_for_status`, JSON status `REQUEST_SUCCEEDED`, then direct
  `Results.series` access;
* no retry/backoff in `ingest.py`; the spec-file downloader separately tries
  HTTPS/HTTP three times and may silently use stale local files, behavior that is
  unacceptable for governed acquisition;
* no persisted raw response, response hash, provider release timestamp, request
  manifest, or response completeness report.

The repository does not establish authoritative public/registered daily quotas.
Commonly documented BLS v2 constraints (registered versus unregistered series,
year, and daily-query limits) and whether 50 × 20 remains supported must be
verified against official BLS documentation during CES-A.  The hosted design is
otherwise suitable: 42 bounded requests and compact normalized output fit a
GitHub runner, provided the registered key, explicit retry, and completeness
checks are used.

Governed `source_request_identity` must hash a canonical request plan containing
the sorted selected series IDs with geo/metric mapping, explicit start/end years,
window and chunk plan, endpoint identity, `annualaverage=false`, adapter contract
version, and relevant config hashes.  It must never contain the secret.

The response fields used by this repository (`status`, `message`,
`Results.series[].seriesID/data[]`) do not expose a governed CES release/vintage
identifier.  Observation footnotes/catalog metadata, if present in BLS responses,
are currently ignored and cannot be claimed as release identity without fixture
and official-contract verification.

## 5. Revision, history, and reconciliation semantics

CES is correctly classified as `revisionary_current_truth` in the candidate
source-refresh contract.  Repeated ordinary BLS retrieval can change an existing
monthly observation.  Routine preliminary revisions affect recent estimates;
annual benchmarking/re-estimation can revise materially deeper history, and the
repository contract intentionally states there is no fixed maximum revision
depth.  Exact revision windows and release timing require official BLS
verification before production acceptance.

Legacy behavior is unsafe as truth governance:

* incremental DELETE removes the complete staged global date envelope for staged
  series, then inserts returned rows; an omitted observation inside that envelope
  disappears;
* full mode deletes every CES row before insert, so omitted history or series is
  lost;
* returned overlap overwrites prior values, but there is no revision diagnostic,
  vintage archive, explicit transaction, or pre-publication candidate validation;
* three-year incremental retrieval detects only revisions inside that window;
  annual benchmark changes outside it are invisible until a full run.

### Recommended governed model

Use **current provider truth + reconciliation against the prior governed
artifact**, with CES-specific rules:

1. Canonicalize and validate the entire acquired response before reconciliation.
2. For every returned canonical key, the newer successful governed acquisition
   wins, including changed historical values.
3. Preserve every prior-only key by default.  CES has no implemented, governed
   provider-retraction evidence; omission must never delete.
4. Report unchanged, revised, new, and prior-only keys per metric and series;
   include earliest/latest revised date and maximum absolute/relative change.
5. Monthly normal acquisition uses an explicit overlap of at least the governed
   three-year policy, but constructs a complete artifact by reconciliation.
6. Run a controlled full-history/deep reconciliation at bootstrap and at least
   annually around benchmarking.  Because legacy “full” is only 60 years, the
   governed start bound must be explicit and sufficient for each selected series,
   not derived from today.
7. A series missing entirely, an observation-max regression, unexpected history
   truncation, or revision outside allowed review policy is quarantine/review—not
   silent carry-forward presented as an ordinary clean refresh.
8. Deletion is permitted only under a future governed retraction contract with
   source evidence and explicit review.

`core.source_artifacts.reconciliation.preserve_prior` supplies the universal
overlap/absence behavior.  CES needs diagnostics and policy gates around it, not
a different reconciliation engine.

## 6. Source target and vintage recommendation

The Redfin cycle `target_month` must not be passed as the CES artifact target.
After successful acquisition and canonical validation:

```text
ces target_month = YYYY-MM of max(canonical acquired CES observation date)
observation_max  = exact max canonical acquired month-end
```

This is deterministic from acquired provider truth and matches FRED's inferred
source-target behavior.  Require all mandatory total-nonfarm series (or an
explicit governed release-completeness subset) to agree on that maximum; do not
let one early/partial series advance the source target.  Optional series may lag
only under an explicit per-series availability rule.  A maximum behind the prior
artifact is quarantine/terminal; an unchanged maximum can yield `unchanged` or
`provider_still_stale` only within a governed lag policy.

Do not use the Actions run date, retrieval date, Redfin catalyst month, or an
unverified BLS press-release date as target.  BLS API response content used here
has no proven release ID.  Define:

```text
provider_release_id = "ordinary-current:" + sha256(canonical acquired response
                      + canonical governed request plan)
```

This content-addressed ordinary-current identity acknowledges that it is not a
BLS vintage label.  If CES-A proves a stable official BLS release identifier,
prefer `bls-release:<official-id>:<response-hash-prefix>` while retaining the
response hash.  A changed historical response at the same observation maximum
then receives a different provider release identity and artifact identity.

## 7. FRED-versus-CES reuse matrix

| Concern | FRED pattern | CES requirement | Classification |
|---|---|---|---|
| hosted acquisition | authenticated provider fetch | BLS v2 POST, many deterministic chunks/windows | REUSE WITH SOURCE-SPECIFIC ADAPTATION |
| provider authentication | required secret | require existing `BLS_API_KEY`; never hash value | REUSE WITH SOURCE-SPECIFIC ADAPTATION |
| retry policy | 3 transient-only attempts, bounded backoff | same policy around each BLS request/batch | REUSE AS-IS |
| target-month resolution | explicit override or observation max | infer observation max; manual override debug-only and validated | REUSE WITH SOURCE-SPECIFIC ADAPTATION |
| observation-max semantics | maximum canonical FRED date | mandatory-series common maximum/lag checks | CES-SPECIFIC IMPLEMENTATION REQUIRED |
| canonical transformation | FRED aggregation/derived spreads | BLS M-period mapping, series crosswalk, unit decision | CES-SPECIFIC IMPLEMENTATION REQUIRED |
| geography mapping | one governed nation | exact static 50 state/metro series crosswalk | CES-SPECIFIC IMPLEMENTATION REQUIRED |
| reconciliation | returned overlap wins, prior-only preserved | identical universal rule | REUSE AS-IS |
| historical revision detection | joined key diagnostics | add per-series plus benchmark-depth gates | REUSE WITH SOURCE-SPECIFIC ADAPTATION |
| artifact creation | `create_artifact` / canonical schema | same, `source_id=ces` | REUSE AS-IS |
| validation | generic artifact plus FRED ownership | generic plus exact CES series/geo/frequency/completeness | REUSE WITH SOURCE-SPECIFIC ADAPTATION |
| durable publication | GitHub Release immutable publisher | identical | REUSE AS-IS |
| catalog registration | CAS immutable source record | identical, source parameterization needed | REUSE AS-IS |
| prior durable resolution | accepted catalog, Actions bootstrap only | accepted CES catalog; controlled bootstrap, no arbitrary JSON | REUSE AS-IS |
| result builder | fail-closed monthly result | same schema, thin CES labels/evidence paths | REUSE WITH SOURCE-SPECIFIC ADAPTATION |
| transient/terminal classification | typed transport only retryable | include `requests` timeout/connection and HTTP 408/429/5xx | REUSE WITH SOURCE-SPECIFIC ADAPTATION |
| workflow structure | call/dispatch, prior/acquire/publish/result | identical shape with BLS secret and CES modules | REUSE WITH SOURCE-SPECIFIC ADAPTATION |
| cohort participation | dependency-free sibling | dependency-free sibling after resolve-cycle | REUSE AS-IS |
| resume/replay | exact durable successful cycle pin; otherwise run | identical | REUSE AS-IS |
| idempotent publication | resolve cataloged semantic candidate bytes | identical Phase 3B compatibility behavior | REUSE AS-IS |

## 8. Bootstrap recommendation

Choose **B — existing state is insufficient**.

Evidence:

* `config/artifact_catalog.json` has accepted pointers only for `redfin` and
  `fred_macro`; it has no CES immutable record or `accepted.source.ces`.
* No CES source-artifact package, normalized response archive, or cycle pin was
  found.
* `data/market_serving.duckdb` contains 161,711 CES rows, 50 geographies, 26
  metric IDs, 2006-05 through 2026-05.  It is the best legacy comparison state,
  but was mutated by the delete/upsert path and is not an accepted source artifact.
* `data/market_public.duckdb` contains 53,368 CES rows, only 7 geographies, 26
  metrics, 1966-01 through 2025-08.  It is an older/narrower serving export and
  cannot arbitrate conflicts with the serving DB.
* The desired three-metric governed scope differs from both DB slices, and unit
  metadata is inconsistent.

Bootstrap procedure (a later, separately authorized production mutation):

1. Freeze the CES-A metric/unit/geography/request contract and build deterministic
   provider fixtures.
2. Make one controlled registered-key full-history acquisition into an isolated
   workspace; do not mutate either DB.
3. Normalize only the approved governed series and compare every overlapping key
   with the serving CES slice.  Explain revision differences, scope differences,
   missing keys, maximum, and unit scale.  Use the public DB only as secondary
   history evidence.
4. Reconcile provider truth against extracted known canonical keys only after a
   human-approved equivalence report.  Preserve legacy prior-only keys only when
   they belong to the frozen governed scope and identity; quarantine unexplained
   series remaps.
5. Construct, validate, publish, durably re-resolve the first CES artifact.  In a
   separate catalog governance commit, establish `accepted.source.ces`.
6. Enable ordinary CES monthly execution only after that accepted pointer exists.

Failure leaves databases, catalog, and pointers untouched.  Do not use the legacy
Actions artifact fallback as an indefinite CES bootstrap mechanism.

## 9. Proposed CES artifact identity

| Field | Proposed rule |
|---|---|
| `source_id` | `ces` (matches existing canonical source ownership and policy) |
| `source_family` | `BLS Current Employment Statistics — State and Metro Area` |
| `source_type` | `revisionary_current_truth` |
| provider/channel | `U.S. Bureau of Labor Statistics`; `BLS Public Data API v2` |
| `provider_release_id` | `ordinary-current:<sha256(request-plan + canonical acquired response)>`; replace prefix only if official release ID is proven |
| `target_month` | acquired mandatory-series common `observation_max` as `YYYY-MM`, never Redfin month |
| `revision` | `r1` under existing artifact identity contract; changed provider response/config yields changed semantic hash/ID, not volatile execution revision |
| `refresh_contract` | `source_refresh_revision_v0_2` / CES policy entry, once explicitly promoted for runtime use |
| `source_request_identity` | `bls-ces-v2:<sha256(canonical request plan)>` |

Semantic `config_hashes` must include the actual inputs:

* `config/ces_series.generated.csv` — exact BLS series/geo/metric/seasonality;
* `config/geo_manifest.generated.csv` — canonical geography identity/crosswalk;
* `config/source_metric_registry.csv` — governed physical/logical ownership;
* `config/source_refresh_revision_policy_v0_2.json` — refresh/reconciliation policy,
  but only after the CES entry is promoted as runtime authority.

If unit/transform rules remain inline code, `git_sha` supplies code lineage but is
execution-volatile in v1.  Prefer a small CES source contract (created only if
needed to settle the real ambiguity, not to mimic FRED) that explicitly records
metric scope, units, M13 rejection, required-series maximum, overlap depth, and
deep-reconciliation cadence; its path must then join `config_hashes`.

Reuse FRED durable candidate resolution exactly: before publication, if the
catalog already owns the semantic artifact ID, resolve and validate authoritative
immutable bytes and report reuse.  Retrieval time, Actions IDs, attempt number,
and current executing SHA are execution evidence and must not cause a normal
same-ID/different-bytes collision.  Do not redesign manifest v1 here.

## 10. Validation and failure rules

### 10.1 Pre-publication gates

| Gate | Required behavior | Failure class |
|---|---|---|
| schema/key/source | exact artifact columns; source `ces`; `property_type_id=all`; unique canonical key | terminal |
| request/response identity | every requested series exactly once at series-block level; no unexpected series | terminal or quarantine for provider drift |
| null/numeric | no null identity/value; finite numeric; decided unit/scale | terminal |
| dates/frequency | M01–M12 only, month-end, no future/impossible months, no duplicate month | terminal |
| metric ownership | exact approved physical metrics; no legacy extra metrics | terminal |
| geography | exact configured series-to-geo mapping; no unknown provider geography | terminal |
| expected membership | exact configured series set, with explicit asymmetric availability | quarantine/review; never silent |
| completeness | mandatory series present through common max; detect missing provider observations | quarantine/review |
| row-count drift | per-series and aggregate bounds against prior/request range | quarantine/review |
| history truncation | prior-only counts and earliest date regression reported; prior preserved | quarantine/review if unexplained |
| observation max | no regression; acceptable release lag enforced; common mandatory max | regression terminal/quarantine; stale per policy |
| revisions | counts/date range/magnitude by metric and series; normal recent changes allowed | deep/large/unexpected changes quarantine/review |
| new keys | configured new periods allowed; new series/geo requires config change | unexpected terminal |
| prior-only keys | preserve; distinguish request range from unexplained omissions | quarantine/review if abnormal |
| config drift | manifest hashes exactly current approved inputs | terminal |
| generic artifact | `validate_artifact`, package hash, immutable identity checks | terminal |
| durable proof | uploaded asset and catalog record re-resolve to same validated data | transport retryable; mismatch terminal |

The existing architecture has retryable versus terminal results, but no first-
class quarantine status in `monthly_source_execution_result_v1`.  Implement
quarantine/review as a fail-closed terminal result with detailed evidence until a
governed review status is added; do not weaken the common result schema.

### 10.2 Retry classification

Retry only failures explicitly caused by transport/provider availability:
`requests.Timeout`, connection errors, HTTP 408, 429, and 5xx, temporary BLS
transport status/messages proven retryable, and typed transient GitHub Release or
catalog API failures.  Apply FRED's three-attempt bounded backoff per request; a
failed batch must fail the acquisition, not produce a partial candidate.

Malformed JSON/schema, `REQUEST_FAILED` caused by request parameters/quota policy,
missing secret, unknown series/geography, duplicate observations, invalid metric
mapping, unit ambiguity, deterministic validation failure, content collision,
catalog governance/CAS invariant failure, and durable hash mismatch are terminal.
HTTP 400/401/403/404 are terminal unless official BLS semantics prove otherwise.

Refactor the FRED retry classifier/result-builder into source-neutral helpers only
if that reduces duplication without changing proven FRED behavior.  Otherwise a
thin `ces_result.py` wrapper is safer for the first migration.

## 11. Workflow and cohort integration design

Create `.github/workflows/ces-monthly-source.yml` with both `workflow_call` and
`workflow_dispatch`, shaped like FRED:

* inputs: required deterministic `cycle_id`; optional
  `source_target_month` for controlled debug/backfill only; optional governed
  acquisition mode (`monthly_overlap`/`deep_reconciliation`) only if backed by
  policy—not arbitrary year or artifact JSON;
* secret: required `BLS_API_KEY`;
* permissions: `contents: write` for Release publication and catalog CAS;
* Python 3.11 plus `requirements.txt`, `PYTHONPATH=.`;
* steps: checkout → dependencies → resolve accepted durable CES prior → acquire,
  canonicalize, reconcile, validate → publish/re-resolve/catalog (without
  activation) → always build result → upload 90-day run evidence;
* output: only `result_json`, conforming exactly to
  `monthly_source_execution_result_v1`;
* evidence: request plan/hash, sanitized provider diagnostics, run report,
  revision/completeness diagnostics, manifest/validation, publication receipt or
  classified failure.  Never upload the secret or caller-supplied trust state.

Modify `.github/workflows/monthly-refresh-production.yml` only after standalone
hosted acceptance:

```text
resolve-cycle
  ├─ redfin (existing candidate/readiness)
  ├─ fred    (existing governed workflow)
  └─ ces     (new governed workflow; secrets inherit)
          \      |      /
             barrier
```

CES has no Redfin/FRED data dependency and should run as a sibling.  Add `ces` to
`REQUIRED_SOURCES`, barrier needs/inputs, result and reused-result plumbing, and
resume pins.  Barrier remains read-only and must continue to assert
`source_set_created=false`, `accepted_pointers_advanced=false`, and
`redfin_consumption_committed=false`.  No Source Set, canonical database, serving
database, Macro Regime, or schedule activation belongs in this phase.

## 12. Normal, resume, and replay

* **Normal:** resolve the accepted CES prior, acquire current governed BLS truth,
  infer CES target from its observation maximum, reconcile, validate, publish or
  resolve the exact existing candidate, and return a durable result.  `unchanged`
  may reuse the exact prior/candidate according to the artifact contract; it must
  still record successful provider checking.
* **Resume:** derive the plan from prior validated cohort evidence.  Reuse CES only
  if the exact cycle has a successful `monthly_source_execution_result_v1` and
  every candidate pin resolves through catalog/Release with matching artifact,
  content, package, publication, and provider-release identity.  If CES failed or
  has no valid pin, rerun CES without rerunning successful siblings.  Never trust
  caller-supplied artifact JSON.
* **Replay:** require explicit cycle identity.  Baseline behavior should match
  FRED: reacquire current provider truth unless an exact successful durable cycle
  pin is intentionally selected by the production replay contract.  Reuse, when
  selected, resolves authoritative bytes; it does not reconstruct trust from
  arbitrary inputs.  Because ordinary BLS truth can revise after the original
  cycle, evidence must say whether replay was `execution_reused` or
  `provider_reacquired`; a reacquisition may validly produce a new CES candidate
  for the same Redfin cycle.

## 13. Legacy retirement plan

| Component | Migration classification | Eventual action |
|---|---|---|
| `sources/bls_ces/expand_spec.py` | RETAIN AS DEBUG/RECOVERY ENTRYPOINT | Harden deterministic spec regeneration; never regenerate silently in monthly runs. |
| `sources/bls_ces/ingest.py` | REFACTOR INTO GOVERNED RUNNER | Extract pure acquisition/canonical adapter; retain direct DB CLI only temporarily. |
| `sources/bls_ces/validate.py` | REFACTOR INTO GOVERNED RUNNER | Replace permissive DB checks with candidate/source-specific gates. |
| full CES job | KEEP DURING MIGRATION | Retire direct mutation after bootstrap and hosted acceptance; preserve a governed deep-reconciliation command. |
| incremental CES job | KEEP DURING MIGRATION | RETIRE AFTER HOSTED ACCEPTANCE and downstream cutover. |
| `refresh_bls_ces.yml` | KEEP DURING MIGRATION | RETIRE AFTER HOSTED ACCEPTANCE; governed workflow supplies manual debug dispatch. |
| broader `refresh_bls.yml` / `full_refresh.yml` CES invocation | KEEP DURING MIGRATION | Remove CES mutation leg only after governed CES acceptance and canonical assembly later consumes artifacts. |

Do not run old and governed CES mutation/publication paths indefinitely.  Nothing
is retired during this reconnaissance.

## 14. Smallest smoke/test plan

Extend the existing numbered smoke sequence (after current 175), reusing fixtures
and core artifact tests rather than creating a CES framework:

1. **CES adapter smoke:** fixture multiple BLS series/windows; prove deterministic
   request plan, response ordering independence, month-end transformation, unit,
   M13 rejection, exact metric and geography mapping, duplicates/malformed values.
2. **CES reconciliation smoke:** prior/current fixture proves overlap revisions,
   new keys, prior-only preservation, deep revision diagnostics, max regression,
   truncation and new-series quarantine.
3. **CES artifact runner smoke:** inferred target/provider content identity,
   explicit target validation, config hashes, deterministic rerun and generic
   artifact validation.
4. **CES durable registry smoke:** fixture Release/CAS proves initial publication,
   idempotent same semantic candidate resolution, collision rejection, prior
   accepted resolution, and no pointer activation.
5. **CES result/failure smoke:** transient timeout/408/429/5xx then success,
   exhausted retryable result, deterministic terminal result, successful exact
   `monthly_source_execution_result_v1`, and malformed evidence fail-closed.
6. **Three-source cohort smoke:** Redfin + FRED + CES normal reaches `ready`;
   resume reuses exact successful pins and reruns only failures; replay accounting
   is explicit; barrier proves no accepted pointer movement, Redfin consumption,
   Source Set creation, canonical assembly, or serving build.

Existing downstream smoke 49/50 should remain as regression coverage for CES/LAUS
employment precedence.  Existing source artifact/FRED/cohort smokes 164–175 are
the templates to extend.

## 15. Phased implementation plan

| Phase | Likely files | Required tests | Production mutation | Fail-closed / rollback | Hosted evidence |
|---|---|---|---|---|---|
| **CES-A — freeze source contract and adapter** | new `sources/bls_ces/artifact.py` or adapter module; narrowly update CES config/contract; refactor `ingest.py` without changing legacy behavior | adapter/acquisition fixtures, membership, unit, request identity, retry | none | fixtures only; reject ambiguity/drift | optional no-publication workflow fixture |
| **CES-B — bootstrap accepted durable state** | bootstrap command under `jobs/monthly_refresh/`; catalog changes occur through hosted governance, not hand edit | full equivalence, reconciliation, artifact validation | publish first CES artifact then separate accepted pointer (explicitly authorized run only) | isolated workspace; no DB mutation; pointer only after durable proof | provider request/equivalence report, receipt, catalog commit, resolver proof |
| **CES-C — governed monthly runner** | `jobs/monthly_refresh/ces.py`, CES diagnostics/validation | normal/unchanged/revised, target, transient/terminal | none | no candidate on partial/invalid response | run report and validated local artifact |
| **CES-D — durable publication/resolution** | thin source-neutral refactor or `ces_durable.py`, `ces_result.py` | idempotency, collision, prior, results | immutable release/catalog candidate only; no activation | catalog is commit point; resolve proof required | receipt, catalog record, re-resolution |
| **CES-E — reusable hosted workflow** | `.github/workflows/ces-monthly-source.yml` | YAML/static plus hosted normal/failure | candidate publication only | always emit classified result; no pointer | Actions run/evidence artifact/result JSON |
| **CES-F — cohort sibling** | production workflow, `cohort.py`, policy execution boundary | three-source normal/barrier and missing/failure cases | none at barrier | incomplete/terminal cohort blocks | ready cohort with all three durable pins |
| **CES-G — hosted normal/resume/replay acceptance** | fixes only if evidence finds defects | hosted normal, retry, resume, replay/idempotency | immutable candidates/catalog records only | exact pin validation; siblings retained | run IDs, attempts, receipts, cohort evidence; three false side-effect flags |
| **CES-H — legacy retirement** | legacy workflows/jobs and execution docs | no legacy scheduler refs; governed debug/deep run | stop old DB mutation path; no source deletion | retire only after acceptance/cutover; revert removal if downstream not ready | hosted governed success plus repository search proof |

CES-B is intentionally separated from ordinary implementation because it is the
only phase that establishes accepted production state.  CES-F/G still stop at the
candidate barrier; complete Source Set v2 and canonical/serving publication occur
only after all intended production sources migrate.

## 16. Unresolved questions and risks

1. **Metric scope:** confirm that governed CES truly includes only the three SA
   metrics despite 26 physical legacy metrics and sparse SA availability for two.
2. **Unit:** officially confirm BLS returned scale and decide whether canonical
   values remain thousands or convert to individual jobs; reconcile conflicting
   repository metadata.
3. **BLS limits:** verify current registered 50-series/20-year request and daily
   quota rules, message-level transient codes, and GitHub-hosted acceptability.
4. **Release identity:** determine whether an official stable State and Metro CES
   release/benchmark identifier can be obtained cheaply without scraping.  Until
   then use honest content-addressed `ordinary-current` identity.
5. **Completeness/lag:** define which series must share observation max and the
   governed acceptable lag for sparse metro sector series.
6. **Benchmark depth/timing:** confirm official annual benchmark schedule and
   affected historical depth; define the deep-run trigger rather than relying on
   wall-clock intuition.
7. **Series remaps:** specify human review for discontinued/replaced area or
   industry series and whether prior-only old-series facts remain under the same
   canonical metric.
8. **Bootstrap divergence:** explain serving/public history differences and any
   provider revisions before accepting initial CES truth.
9. **Legacy validator defect:** it collapses every generated series to only total
   nonfarm SA/NSA expectations and is not adequate evidence for three-metric scope.
10. **Contract authority:** `source_refresh_revision_v0_2` is currently marked
    `production_governed=false` and `runtime_consumed=false`; promotion must be an
    explicit governance decision, not an implementation side effect.

Subject to resolving metric scope, unit, and official provider limits, no genuine
dependency prevents parallel CES cohort execution.  The migration is therefore
primarily a thin adaptation of FRED, with CES-specific acquisition,
canonicalization, geography/completeness, and revision-depth policy at its edge.

## 17. Reconnaissance safety record

Read-only repository searches, file inspection, CSV summaries, and DuckDB
connections opened with `read_only=True` were used.  The live BLS API was not
called.  No source artifact was built or published; no Release or catalog record
was created; no accepted pointer, readiness record, Redfin drop, Source Set,
DuckDB, canonical/serving artifact, schedule, Macro Regime run, or LAUS path was
changed.
