# BPS-A governed source reconnaissance and migration decision

**Date:** 2026-09-01

**Migration state:** `RECON_COMPLETE_IMPLEMENTATION_BLOCKED`
**Production integration:** not performed

> **BPS-B correction:** the textual unavailable-token vocabulary in BPS-A was
> fixture-based rather than provider-proven. BPS-B removes those admitted
> strings and fails closed until an official pinned payload is available; see
> `bps_b_pinned_release_verification_v0_1.md`.

## Existing footprint

The full job runs `sources.census_bps.ingest` and then `sources.census_bps.transform`; the provisional job separately retrieves current state/county/CBSA text files and inserts `census_bps_provisional`. Serving's `fact_timeseries_bps` view gives compiled rows precedence over provisional rows for the same fact identity. Both legacy transforms mutate DuckDB by deleting their entire source slice before insert, which is not the governed target.

Compiled acquisition scrapes the Census master directory, downloads the latest `BPS_Compiled_File_YYYYMM.zip`, reads its first CSV, keeps `period=Monthly`, maps through `config/geo_manifest.generated.csv`, and writes mutable CSV intermediates. It materializes 15 metrics: five authorized-unit measures, five building-count measures, and five valuation measures. Dates are month-start. Legacy aggregate construction coerces unavailable components to zero, an unsafe behavior that governed v1 does not reuse.

The best current local truth is `data/market_serving.duckdb`: compiled `census_bps` has 447,600 rows, 168 geographies, 15 metrics, and dates 2006-04-01 through 2026-04-01; provisional has 3,255 rows, 217 geographies, 15 metrics for 2026-05-01. `data/market_public.duckdb` contains an older/narrower compiled truth: 71,715 rows, 14 geographies, 15 metrics, 1988-01-01 through 2025-08-01. These histories vary by geography. Neither is a governed immutable BPS artifact and the catalog has no accepted `bps` pointer.

Actual direct downstream consumption is only physical `census_bp_total_units`, mapped by `config/source_metric_registry.csv` to `bps_total_units`, then canonical `permit_activity`. It supplies the promoted Supply metric at weight 0.30 and also parents `derived_permit_intensity` with population. This flows through feature engineering, normalization, Supply scoring/regimes, diagnostics/review, serving/dashboard surfaces, and forecasting design inputs. Narrowing away total units or needed county identities would break those contracts. Buildings, size bands, and valuation have no direct governed source-metric registry consumer.

## Provider contract evidence and limits

The authoritative interface chosen for v1 is the Census BPS compiled master ZIP, not an API/package. It is public URL/file acquisition and no API key or secret is used. The deterministic release URL and evidence locations are recorded in the companion contract. No Census API response or API request limit applies. Ordinary HTTP hosting constraints and bounded retry handling do.

Repository and provider layout evidence establish a monthly publication family containing monthly, annual, and year-to-date records; multiple geography levels; units/buildings/valuation split by residential structure size; and publication/reporting metadata. V1 admits only monthly NSA total authorized housing units.

Live official verification was attempted with `curl` against the Census program, methodology, definitions, technical-documentation, master-directory, and county-directory URLs. The environment proxy returned `CONNECT tunnel failed, response 403`; the web-search facility also returned HTTP 401. Therefore exact release timing, official unavailable/suppression tokens, omission/retraction meaning, historical revision depth, compiled “final” guarantees, and annual revision/rebenchmark rules remain unresolved. No unsupported provider claim is promoted into runtime governance.

## Governed v1 decision

### Metric classification

| Class | Existing metric IDs |
|---|---|
| required | `census_bp_total_units` |
| diagnostic | none |
| legacy-only / out-of-scope | `census_bp_1_unit`, `census_bp_2_units`, `census_bp_3_4_units`, `census_bp_5plus_units`; all five `*_bldgs`; all five `*_value` |
| unresolved | provider valuation unit/scaling semantics (metric remains out of scope); no additional governed metric |

The exact definition, unit, adjustment, frequency, scaling, and consumers are tabulated in `docs/contracts/bps_governed_source_v1.md`.

### Geography and cardinality

V1 reuses the generated geography authority but freezes only provider identities proven in local production truth: 1 nation + 5 states + 162 counties = **168 geographies**. With one required metric there are **168 configured metric × geography identities**. Coverage by month/history is explicitly sparse rather than asserted as a full temporal Cartesian product. The 64 generated CBSAs, places from the older manifest, and unsupported combined/division levels are out of v1. `franklin_city_county_va__county` remains excluded because it is configured generally but absent from local BPS truth.

### Canonical/reconciliation decision

Governed facts conform to the common seven-column schema with `source_id=bps` and both property fields `all`; numeric values are unscaled float64 housing-unit counts; dates remain observation-month first day. Duplicate or unexpected identities fail. Known unavailable values are omitted/diagnosed; zero remains zero.

The compiled file is planned as a complete snapshot on each release. Returned overlap wins, returned new keys append, and prior-only governed rows persist. Provider omission is not retraction. Consequently there is no separate routine rolling overlap window. A separate annual/deep subsystem is not justified: **outcome D**, defer deep-refresh automation until hosted documentation/payload verification; ordinary complete-snapshot acquisition is already full-depth.

BPS target month derives from the canonical provider observation maximum, not the master/Redfin cohort month. Observation dates remain month-start while the manifest target uses `YYYY-MM`.

## Existing truth comparison

A non-destructive DuckDB inventory established the two local truths and their scope above. A fresh compiled comparison could not be performed because Census transport was blocked. The adapter and fixture classify the comparison surfaces needed by BPS-B: exact match, provider revision, provider new observation, legacy-only/out-of-scope row, governed identity/geography conflict, unit/scale mismatch, provider unavailable/suppressed, and unexpected numeric conflict. No database or pointer was mutated.

## Migration/lifecycle plan

1. Hosted BPS-B verifies official technical documentation and downloads a pinned compiled release.
2. Validate the 168 frozen bindings, suppression vocabulary, component/total field behavior, release identity, and observation coverage; perform read-only equivalence against serving truth.
3. Build the first complete immutable `bps` candidate through common `create_artifact`, validate and durably publish/verify it, but leave acceptance to explicit cohort promotion.
4. Only after reviewed bootstrap acceptance, add a thin routine runner/result/workflow using the existing normal/resume/replay and durable cycle-result contracts. Do not add provisional overlay or annual-processing complexity until evidence requires it.

BPS-A adds only the frozen registry, pure planning/canonicalization/reconciliation adapter, contract/audit, and deterministic fixture smoke. It adds no workflow, cron, production fan-out, pointer update, source set, market promotion, or publication.

## Authentication, failure, and state

No secret is required for the compiled URL. Retry HTTP 408/429/5xx and transport faults with bounded common behavior. Treat provider/schema/identity/numeric contradictions, validation failures, durable resolution failures, and immutable conflicts as terminal. Suppression is diagnosed rather than zeroed; required-coverage gates decide whether its extent blocks a later candidate.

Before and after candidate-only fixture construction, SHA-256 of `config/artifact_catalog.json` is identical. Accepted FRED, CES, LAUS, and Redfin state is unchanged; no BPS accepted pointer exists.

## Material open questions

1. What exact tokens/blank rules does the current compiled file define for unavailable, suppressed, or nonreported unit observations, and can omission ever prove retraction?
2. Does the compiled payload contain an authoritative `total_units` field for all governed rows, or must total be derived only when every component has a verified available value?
3. Does hosted current payload coverage prove all 168 frozen identities, and should configured-but-absent Franklin City be added or remain excluded?
4. What do official Census documents guarantee about compiled revision/finality and annual historical revisions? This determines whether outcome D can be simplified to “complete monthly snapshot is sufficient indefinitely.”

## Single next BPS step

Run **BPS-B hosted pinned-release verification and read-only bootstrap equivalence** against the 168-identity/one-metric contract; do not publish or accept until the four questions above are resolved by official documentation and payload evidence.
