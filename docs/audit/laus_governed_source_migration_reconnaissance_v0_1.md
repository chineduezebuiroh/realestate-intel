# LAUS governed-source migration reconnaissance v0.1

**Status:** reconnaissance and proposed contract evidence only, 2026-08-30. No
provider run, database mutation, artifact publication, accepted-pointer change,
workflow integration, or production-policy change occurred.

## 1. Executive findings

LAUS can use the accepted common governed-source framework without redesign. Its
legacy path is a direct, mutable BLS loader with API-to-flat-file fallback and a
generated 840-series registry. The proposed v1 governs all 820 NSA series: 615
required observations (labor force, employment, unemployment rate across 205
geographies) and 205 diagnostic unemployment levels. The 20 SA state series are
legacy-only. The controlling month is the common latest month of the 615 required
series, never the latest individual result.

The checked-in serving and public databases are inconsistent legacy comparison
evidence, not governed priors. The artifact catalog has neither LAUS records nor
an LAUS accepted pointer. Bootstrap must therefore reconstruct provider truth,
equivalence-audit it in isolation, publish durably, and activate LAUS alone only
after review.

The API-limit and revision-methodology gates are now closed with externally
supplied reviewer evidence because official BLS pages remained unreachable through
the task network proxy. Official provenance URLs are retained below and must be
captured with LAUS-B implementation evidence. Remaining OPEN items are only the
exact annual deep-reconciliation production month (pending verified BLS release
calendar) and authenticated remote LAUS Release/tag inventory.

## 2. End-to-end repository trace

| stage | file/function | observed behavior |
|---|---|---|
| geography config | `config/geo_manifest.generated.csv` | `include_laus` plus `bls_laus_area_code` selects 205 generated geographies. |
| metadata/spec generation | `sources/bls_laus/expand_spec.py` `ensure_bls_files`, `load_laus_areas_from_manifest`, `load_lookup`, `pick_latest_series`, `main` | downloads BLS LA flat metadata/data files, selects measures 003--006 and NSA for substate plus SA/NSA for states, writes generated CSV. |
| tracked physical spec | `config/laus_series.generated.csv` | preferred runtime input: 840 unique series; hand-maintained `config/laus_series.csv` is fallback only. |
| provider request | `sources/bls_laus/ingest.py` `fetch_series` | BLS API v2 POST; chunks caller list by 50; partitions 20-year windows; requests annual averages; optional API key. |
| fallback | `fetch_series_any`, `fetch_lau_from_files` | missing/short API responses may be replaced from mutable flat files; unacceptable for governed execution. |
| transform | `to_df`, `base_from_sid`, `suffix_from_sid` | series-tail metric mapping; prefix adjustment mapping; month-end conversion; floats; M13 may become Dec 31 only if no monthly row. |
| dimensions/write | `ensure_dims`, `upsert`, `main` | creates dims; full deletes keys, incremental deletes staged date range, inserts directly into DuckDB. No isolated prepublication candidate. |
| jobs | `jobs/full_refresh/run_refresh_bls_laus.py`, `jobs/incremental_refresh/run_refresh_bls_laus.py` | target full/serving DB respectively, then post-write validation. |
| validation | `sources/bls_laus/validate.py` | expects SA+NSA only for IDs ending `_state`, NSA otherwise; warns by default; does not validate registry-exact governed scope. |
| legacy Actions | `.github/workflows/refresh_bls_laus.yml`, `.github/workflows/schedule.yml` | mutable incremental path; not a governed source/cycle producer. |
| serving/public | `scripts/build_serving_snapshot.py`, `scripts/make_public_db.py`, `app.py` | 20-year serving cap, public copy, LAUS display metadata. |
| canonical downstream | `config/source_metric_registry.csv`, `config/metric_dimension_registry.csv`, `config/feature_registry.csv` | four logical metrics resolve to NSA; labor force/employment/rate are weighted production Demand; unemployment count is diagnostic/zero weight. |
| diagnostics/tests | LAUS experiments and smoke 44, 49--50, 108--127 | establish downstream MA9/B3 and source precedence; none proves governed acquisition. |
| revision intent | `config/source_refresh_revision_policy_v0_2.json` | calls LAUS revisionary current truth, ~3-year normal overlap, annual at-least-five-year/prefer-full reconciliation and preserve omissions. |
| cohort intent | `config/monthly_refresh_policy.json` | LAUS required but explicitly `pending_governed_source_workflow`. |

The old hand CSV has 49 non-comment unique series across legacy aliases and
reuses DC state series for `dc_state`, `dc_county`, and `dc_city`; malformed
seasonality text also exists. It is neither coherent identity authority nor safe
fallback for governed v1. The generated spec has no duplicate series IDs.

## 3. Current physical scope

The generated inventory has 840 unique BLS series and 205 geographies:

| geography level | geographies | NSA series | SA series |
|---|---:|---:|---:|
| state | 5 | 20 | 20 |
| CBSA metro | 37 | 148 | 0 |
| county | 163 | 652 | 0 |
| **total** | **205** | **820** | **20** |

Each NSA metric has exact 205-geography membership. Each SA metric has exact
five-state membership. Physical IDs are eight: four concept bases times SA/NSA.
There are no generated cities/places, metro divisions, nation, or ZIPs. The full
row-level mapping, units, classes and target flags is in
`config/audit/laus_series_inventory_v0_1.csv` rather than duplicated here.

The apparent overlap is semantic, not a duplicate series: NSA state and county
identities include separate DC IDs, while the discarded hand config maps one DC
state series to three aliases. Every generated series is consumed by the legacy
loader; governed downstream production requires only NSA labor force, employment,
and unemployment rate. NSA unemployment is diagnostic; all SA and old aliases are
legacy-only for v1.

## 4. Proposed logical and geography contract

The proposed classifications are:

* **required:** `laus_labor_force_nsa`, `laus_employment_nsa`, and
  `laus_unemployment_rate_nsa`, persons/persons/percent, no scaling, all 205 geos;
* **diagnostic:** `laus_unemployment_nsa`, persons, no scaling, all 205 geos;
* **legacy-only:** all four SA state metrics and every hand-config alias.

This matches actual production use and the current physical mappings rather than
metric names/descriptions (whose descriptions incorrectly say “seasonally
adjusted” while the IDs and seasonality columns say NSA). Completeness remains
registry-driven so later sparse/discontinued membership cannot silently become a
Cartesian assumption.

Canonical IDs are the generated `geo_slug` values bound to provider area codes.
DC state and county remain distinct. One provider series must bind exactly once;
ambiguous binding fails. Series redesign is a reviewed registry migration.

## 5. Provider/API and units

Current ingestion uses BLS Public Data API v2, not a LAUS-specific API. The wire
request contains `seriesid`, `startyear`, `endyear`, `annualaverage`, and optional
`registrationkey`; success is `status=REQUEST_SUCCEEDED` with
`Results.series[].data[]`. The repository operates with 50-series chunks and
20-inclusive-year windows, optional key, 60-second timeout, and no API retry.
Fallback flat files use `la.data.2/3` for states and `la.data.60/61/63/64/65` for
substate areas, omit M13, and can replace “short” API series based on a hard-coded
2010 test.

Governed execution must reuse only generic BLS transport shapes proven by CES:
POST, deterministic partitions, status/schema checks and bounded retry. LAUS owns
its much larger registry, response membership, completeness and target rule. It
must remove flat-file fallback, `date.today()`, annual-average requests, silent
skips, and permissive seasonal fallback.

Repository ingest dimension metadata establishes level units as persons and rate
as percent; the metric registry and metadata agree (“People”/“people” and
“Percent”/“percent”). No multiplier occurs in transformation or legacy rows.
The proposed contract preserves provider numeric scale. Official unit confirmation
remains part of the external evidence capture gate, rather than inference from
names alone.

Externally supplied reviewer evidence freezes registered BLS API v2 at 500
queries/day, 50 series/query, 20 years/query, and 50 requests/10 seconds. Local
fetch of the [official BLS API v2 page](https://www.bls.gov/developers/api_signature_v2.htm)
was blocked, so this is not locally verified evidence. `BLS_API_KEY` remains
required. Exactly 840 series produce 17 series batches. Ordinary three-year
acquisition makes 17 requests. Deep/bootstrap make
`17 * ceil((end_year-1976+1)/20)` requests: for explicit end year 2026, three year
windows and 51 requests. Counts are deterministic and timing-independent.

## 6. Revision, reconciliation and target

Repository policy classifies LAUS as current truth subject to monthly preliminary
revision, annual re-estimation reaching approximately five years, and possible
series/geography remaps. The legacy incremental loader requests three wall-clock
lookback years and lets staged overlap replace database values. It has no explicit
retraction evidence.

Externally supplied reviewer evidence freezes that annual revisions normally may
revise five prior years through updated population controls, revised inputs and
model re-estimation; substate estimates are revised and controlled to revised
state totals; NSA and SA modeled data can both change. Exceptional changes can go
materially deeper, including recent approximately 2016--2025 substate revision
and selected modeled-series reconstruction to beginnings such as 1976, 1990 or
1994. Official provenance URLs retained for capture are the
[LAUS methods](https://www.bls.gov/lau/laumthd.htm) and
[LAUS notices](https://www.bls.gov/lau/notices/) pages.

Frozen operational policy has three cases: (A) routine monthly
`ordinary_overlap`, explicit `end_year-2..end_year`; (B) scheduled annual
`deep_reconciliation`, full `1976..explicit reviewed end_year`, at least once each
calendar year after annual revision/re-estimation becomes available; and (C) an
exceptional reviewed full-history reconciliation after methodology, geography,
series, unusually deep history, or registry-identity change. The exceptional path
blocks silent substitution and requires registry/method review, equivalence
revision diagnostics, and reviewed acceptance. Bootstrap remains full
`1976..explicit reviewed end_year`.

Returned overlap wins, new rows append and prior-only rows persist. Ordinary mode
can revise only its requested window. Annual/exceptional full history refreshes
older truth; prior-only preservation prevents deletion but does not certify
indefinite provider freshness. The exact annual production month remains **OPEN**
pending verified release-calendar evidence.

Target is the minimum per-series maximum across all 615 required series, with a
required row for each at that month. A required lag/omission blocks; diagnostic
lag only diagnoses; partial months and advanced outliers cannot advance. A
missing/discontinued required geography blocks until reviewed registry change.
Target regression is fatal.

## 7. Canonical artifact rows and deterministic identity

Rows are exactly `geo_id, metric_id, date, property_type_id, value, source_id,
property_type`; source is `laus`, both property fields are `all`, dates are month
ends, numbers are finite doubles in persons or percent, M13 is discarded, and key
sorting is deterministic. Nulls, invalid values, duplicates, unknown series and
out-of-bounds periods fail closed.

`source_request_identity` hashes canonical plan JSON as `bls-laus-v2:<sha256>`.
`provider_release_id` is
`laus-ordinary-current:<sha256(plan bytes + newline + stable canonical provider
observation bytes)>`. Hash input includes exact registry mappings, bounds,
partition plan, endpoint/contract/unit metadata and config hashes, plus sorted
M01--M12 provider tuples. It excludes secret, retrieval/order/timing, run ID,
wall clock and publisher SHA.

## 8. Checked-in database footprint

| database | rows | min | max | geos | physical metrics | duplicates | null values | source/property |
|---|---:|---|---|---:|---:|---:|---:|---|
| `data/market_serving.duckdb` | 215,148 | 2006-05-31 | 2026-05-31 | 220 | 8 | 0 | 0 | `laus` / `all` |
| `data/market_public.duckdb` | 43,408 | 1976-01-31 | 2025-08-31 | 20 | 8 | 0 | 0 | `laus` / `all` |

Serving contains 52,587 rows/220 geos for each NSA metric and 1,200 rows/5 geos
for each SA metric. Public contains 9,064 rows/20 geos for each NSA metric and
1,788 rows/3 geos for each SA metric. The serving 20-year cap explains its later,
shorter history; public is older and narrower. Serving has 15 more identities
than the generated registry, proving legacy alias/drift. Both are comparison
evidence only; neither is an accepted governed prior and neither was mutated.

## 9. Artifact registry state

`config/artifact_catalog.json` has accepted pointers only for CES, FRED macro and
Redfin, and immutable records only for those sources. It contains no `laus`
immutable record, metadata, release tag, receipt, asset, or
`accepted.source.laus` key. Repository search found no separate accepted LAUS
pointer. No partially migrated LAUS catalog artifact exists. Remote orphan
Releases cannot be proven from the local catalog and are **OPEN** for an
authenticated remote tag-prefix inventory in LAUS-B; no pointer was altered.

## 10. Bootstrap and common reuse

Recommended bootstrap is full provider reconstruction to isolated candidate,
read-only equivalence against both legacy DB slices, review, durable publication,
explicit LAUS-only CAS activation, then fresh durable resolution. Exact/provider
revision/provider newer/provider historical-only/legacy prior-only classifications
are reviewable; identity ambiguity, unit-scale or unexplained numeric mismatch,
required incompleteness, regression, and nondeterminism are hard failures.

Reuse the accepted artifact builder/validator, deterministic packaging, Release
publisher, catalog registration and remote verification, pointer invariants,
common execution result, automatic durable recording, semantic-equivalence rule,
normal/resume/replay, partial-success recovery, successful-source reuse and common
barrier. Only the pure registry/request/transform/completeness/target/reconciliation
and equivalence adapter is LAUS-specific. No common redesign is needed.

## 11. CES versus LAUS

| Concern | CES | LAUS v1 proposal |
|---|---|---|
| provider | BLS | BLS |
| API transport | v2 POST, 50-series/20-year deterministic partitions | reuse same generic transport shape |
| series structure | State and Area Employment supersector/data type | area + measures 03--06 |
| geography | 5 states + 44 metros in accepted contract | 5 states + 37 metros + 163 counties |
| required cohort | total-nonfarm SA across its configured membership | LF/employment/rate NSA across 205 geos |
| units | thousands of jobs | persons for levels; percent for rate |
| SA/NSA | governed SA; NSA legacy | governed NSA; SA legacy-only |
| target | common maximum of mandatory total-nonfarm | common maximum of all 615 required series |
| revision | ordinary three years; full deep | ordinary three years; full 1976 deep |
| bootstrap | full provider reconstruction/equivalence | same process, LAUS-specific classifications |
| provider identity | content-derived ordinary-current | LAUS-namespaced content-derived identity |
| common reuse | accepted full lifecycle/cohort | unchanged full lifecycle/cohort |

## 12. Decision summary

| decision | frozen proposal / status |
|---|---|
| governed metrics | three required NSA bases plus diagnostic unemployment NSA |
| required/diagnostic | LF, employment, rate required; unemployment count diagnostic |
| geography | exact registry: 5 state, 37 CBSA metro, 163 county |
| seasonal policy | NSA governed; SA legacy-only |
| units | levels persons; rate percent; no scaling |
| target | common latest month of all 615 required series |
| ordinary overlap | explicit `end_year-2..end_year` |
| deep | full `1976..explicit end_year` |
| bootstrap | `1976..explicit reviewed end_year` |
| overlap | returned provider key wins after complete acquisition |
| prior-only | preserve; no implicit retraction |
| M13 | always discard |
| provider identity | hash canonical plan plus sorted provider observations |
| equivalence | explained revisions/coverage reviewable; identity/unit/unexplained mismatch hard fail |
| pointer bootstrap | publish/verify, then explicit LAUS-only CAS and re-resolve |
| common reuse | all accepted lifecycle/result/cohort components unchanged |
| API limits | frozen from reviewer evidence: registered 500/day, 50 series/query, 20 years/query, 50 requests/10 seconds |
| revision depth | normal up to five prior years; exceptional revisions/redesign can reach materially deeper/series beginnings |
| annual deep cadence | required at least once/calendar year after annual revisions are available; exact production month **OPEN** |
| exceptional reconciliation | reviewed identity/method update plus explicit full-history diagnostics and acceptance |
| remote orphan releases | **OPEN:** authenticated LAUS tag-prefix inventory in LAUS-B |

## 13. Historical Smoke 49 disposition

`scripts/smoke_tests/40_49/49_laus_ma9_production_contract.py` is named as a
production contract, but its broad non-LAUS isolation fixture still hard-codes
superseded BPS, Inventory, and Permit Intensity weights. It is current-contract
test debt rather than intentional historical evidence. Correcting that unrelated
regime-policy fixture belongs in a separate maintenance change; LAUS-A neither
depends on nor modifies it.

## 14. Exact next phase

After human review of this hardened contract, execute **LAUS-B: pure governed
adapter plus controlled bootstrap/equivalence tooling**. Its preflight must perform
the authenticated remote LAUS Release/tag inventory and capture official BLS pages
for provenance; annual scheduling may retain its month as a later operational
choice. Then create an implementation-owned deterministic registry, pure BLS
plan/acquire/transform/validate adapter, fixture tests, isolated full candidate and
read-only equivalence report. Stop before routine hosted workflow, master
orchestration integration, publication, or pointer activation unless a separate
reviewed authorization explicitly includes those steps.
