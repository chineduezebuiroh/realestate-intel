# NRC-A governed-source migration reconnaissance and contract freeze v0.1

## 1. Status and disposition

**Status:** reconnaissance complete for repository and legacy DuckDB behavior; the
production migration is intentionally not implemented. Direct live-provider retrieval was
attempted on 2026-09-18, but this hosted environment's outbound proxy rejected both
`www.census.gov` and `fred.stlouisfed.org` with HTTP tunnel 403. Consequently, the
provider contract below freezes the stable publication semantics that can be established
from the provider product/documentation and repository evidence, but freezes neither a
specific latest observation nor an exact direct-download request. A successful recorded
Census response is an explicit NRC-B entry gate.

**Disposition: B.** The future governed physical source should be `census_nrc`, acquired
directly from the Census Bureau's New Residential Construction (NRC) product. Census is
the statistical agency, publisher, reviser, and owner of the definitions; FRED is a
secondary distributor and the repository uses its unauthenticated CSV graph endpoint as a
legacy transport convenience. FRED lineage must remain recorded on legacy parity
evidence, but a second logical resolver or permanent `census_nrc_fred` production identity
would add indirection without adding authoritative information. This is a recommendation
for NRC-B, not a source-ID mutation in NRC-A.

The frozen analytical scope is two monthly, seasonally adjusted annual-rate **total**
series at the United States and four Census regions. Existing metric IDs remain stable.
The native values are **thousands of housing units at SAAR**, not literal individual
`units_saar`; NRC-B must correct the physical unit metadata without numerically scaling or
renaming the metrics.

## 2. Existing repository contract (observed legacy behavior)

* `sources/census_nrc_fred/ingest.py` retrieves ten full ordinary-current FRED CSV
  histories. Its inline map contains five starts and five completions series and assigns
  `source_id=census_nrc_fred`.
* `sources/census_nrc_fred/transform.py` drops malformed/null values, assigns both
  property fields `all`, and delete/inserts returned keys into `fact_timeseries`. It does
  not reconcile absent keys, use a transaction, retain a provider response/version, or
  perform a source-specific post-write validation.
* `jobs/incremental_refresh/run_refresh_census_nrc_fred.py` is named incremental but runs
  the complete ingest and transform against the serving database. The Make target runs
  the same modules against the full database.
* The registries describe monthly, SA, nation/region, `Units SAAR`; the canonical metric
  IDs are `census_housing_starts_total_saar` and
  `census_housing_completions_total_saar`.
* The inline ingest geography IDs (`united_states__nation`,
  `northeast_region__region`, etc.) disagree with the IDs hard-coded by the feature
  loader and present in the legacy database (`us_nation`, `us_region_northeast`, etc.).
  This is a real legacy contract split, not authority for either spelling.

These facts describe the mutable legacy path only. They do not authorize a governed
candidate, pointer update, Source Set, canonical market, or serving mutation.

## 3. Authoritative provider and product

### Verified product facts

The authoritative product is the U.S. Census Bureau/HUD **New Residential Construction**
release, based on the Survey of Construction. Census publishes monthly estimates for
building permits, housing starts, housing completions, and housing under construction.
The relevant provider landing/publication surfaces are:

* NRC product: `https://www.census.gov/construction/nrc/`
* current release PDF: `https://www.census.gov/construction/nrc/pdf/newresconst.pdf`
* historical-data/download surface:
  `https://www.census.gov/construction/nrc/historical_data/index.html`
* Census economic time-series API dataset family: `timeseries/eits/resconst`

Census offers direct machine-readable/tabular distribution (economic time-series API and
downloadable historical workbooks/tables). Therefore governed routine acquisition need
not depend on FRED. NRC-B must verify one exact Census surface, schema, table/series codes,
and response hashes before selecting it; this audit deliberately does not invent an
untested query.

FRED series pages identify the source as the U.S. Census Bureau and expose Census-derived
values through ordinary-current series. `fredgraph.csv` supplies observation date/value,
but it does not carry the originating Census release artifact, preliminary/revised flag,
Census table cell identity, or vintage. Ordinary FRED is mutable current truth; ALFRED is
a separate vintage product. No repository evidence proves byte-for-byte equivalence or
zero publication lag between a Census release and FRED refresh. Therefore “FRED mirrors
Census” is a parity hypothesis to test, not the governed provenance contract.

### Live verification result

Bounded GETs were attempted for both Census product/release/history pages and the FRED
`HOUST`/`COMPUTSA` pages. All were blocked before reaching the providers by the hosted
proxy. No claim in this artifact treats the repository's January 2026 data as a live
September 2026 provider response. This limitation is the only blocker to closing the
exact acquisition/pin choice.

## 4. Source identity recommendation

Use one governed physical source, `census_nrc`, with provider `US Census Bureau`, product
`New Residential Construction`, and exact direct Census input identifiers in every
manifest. Keep `census_nrc_fred` only while the legacy path is required for comparison and
rollback. On cutover, downstream source-ID consumers must change atomically with Source
Set inventory/assembly contracts; do not silently alias the two physical sources.

A logical NRC resolver and provider family are unnecessary: the two metrics share one
provider product and neither requires precedence across competing physical artifacts.
If operational testing proves the direct Census surfaces unsuitable, stop and revisit
this disposition; only then should option C (logical `census_nrc` plus explicit FRED
transport) be considered.

## 5. Frozen metric contract

| Contract field | Housing starts | Housing completions |
|---|---|---|
| metric ID | `census_housing_starts_total_saar` (retain) | `census_housing_completions_total_saar` (retain) |
| concept | Total privately owned housing units started | Total privately owned housing units completed |
| product/table identity | NRC/SOC, total-units monthly starts table/cells; bind exact direct code in NRC-B | NRC/SOC, total-units monthly completions table/cells; bind exact direct code in NRC-B |
| legacy FRED series | `HOUST`, `HOUSTNE`, `HOUSTMW`, `HOUSTS`, `HOUSTW` | `COMPUTSA`, `COMPUNETSA`, `COMPUMWTSA`, `COMPUSTSA`, `COMPUWTSA` |
| frequency | monthly | monthly |
| adjustment | seasonally adjusted | seasonally adjusted |
| annualization | annual rate (SAAR) | annual rate (SAAR) |
| native numeric unit | thousands of housing units at SAAR | thousands of housing units at SAAR |
| first periods represented by legacy FRED | 1959-01 for region starts in the checked DB; direct/national boundary to re-verify | 1979-01 for region completions in the checked DB; Census/national boundary to re-verify |
| missing behavior | missing/suppressed/non-numeric cells are not observations; never coerce to zero | same |
| revisions | preliminary recent values and historical seasonal/benchmark changes are possible | same |

The IDs correctly encode Census concept, total scope, and SAAR and should not be renamed.
However, `Units SAAR`/`units_saar` is dimensionally misleading for native values such as
1,500 (meaning 1.5 million annualized units). NRC-B must choose and register a precise
unit such as `thousands_of_units_saar`, retaining numeric values. This is a metadata
correction recommendation only.

The exact first and latest periods are response facts, not hard-coded contract constants.
NRC-B validation must record them per metric/geography from the pinned Census input. In
particular, do not infer national coverage from the region-only legacy database.

## 6. Frozen geography/applicability contract

The natural NRC publication scope for these seasonally adjusted total starts/completions
series is:

1. United States;
2. Northeast Census Region;
3. Midwest Census Region;
4. South Census Region; and
5. West Census Region.

This five-geography scope is the final governed applicability set. NRC does not directly
publish these monthly SOC estimates as a complete state, metro, county, place, or local
series. Census divisions and subnational detail available from other Census housing
products are not these NRC metric observations. They remain absent. No allocation,
aggregation, or other synthetic state/local observation is permitted.

NRC-B must select canonical geography IDs from the governed geography manifest and
explicitly map provider labels to them. It must not inherit either conflicting legacy ID
set merely because it already exists.

## 7. Frozen date/period semantics

A provider month label identifies the **observation/reference month**, not the press
release date. The legacy FRED observations and legacy database use the first calendar day
(e.g. `2026-01-01`) as a month label. The repository's broader analytical alignment uses
month end.

For governed NRC candidates, freeze canonical `date` as the final calendar day of the
observation month. Preserve separately in lineage: provider period label, provider-native
observation date (if first-of-month), retrieval timestamp, release/publication date, and
target month. Never substitute release date for observation date. NRC-B parity must
normalize both legacy first-of-month keys and Census labels to month end before comparing
values.

## 8. Revision semantics

NRC is revisionary current truth. A monthly release can revise recently published
estimates; updated seasonal factors or methodological/benchmark work can revise deeper
history. Census's current historical tables/API and ordinary FRED series can be changed in
place. Neither surface, as used by the repository, supplies an immutable NRC release ID.
FRED's current graph endpoint is not vintage evidence.

This audit does **not** freeze a maximum revision window: no successful live provider
capture was available to substantiate one, and a bounded recent-month assumption would
miss occasional deeper revisions. The governed policy is therefore full-history
acquisition/reconciliation on each detected content change, overlap replacement from the
new pinned input, preservation of prior-only keys unless Census explicitly documents a
retraction, and full key/value diff evidence. If future Census documentation proves a
bounded window, changing this policy requires a new governed decision.

Census does not provide an immutable release/vintage ledger through the legacy path.
ALFRED could describe FRED vintages but would make FRED, rather than Census, the governed
release boundary and is not recommended for the direct migration.

## 9. Release discovery and input pinning

Publication is monthly and release-calendar driven. Treat the current Census endpoint or
workbook as mutable, including stable URLs/filenames. A period string or filename alone
is not an immutable release identifier.

Recommended NRC-B/C pin:

1. discover the provider's latest reference month and published/revised state from a
   bounded Census metadata/table request;
2. fetch the complete governed table/input bytes plus response headers and request URL;
3. store immutable raw bytes (or a losslessly canonicalized response where provider
   packaging is nondeterministic);
4. record retrieval UTC, product/table/query identifiers, reference maximum, schema,
   byte SHA-256, normalized canonical-content SHA-256, row/key inventory, and Git/config
   hashes;
5. define `provider_release_id` from product + observed maximum + canonical content hash,
   not from “latest,” retrieval time, or filename;
6. use the exact stored/hash-verified input to build the immutable candidate.

Unchanged canonical content means `no_new_release`/reuse of the prior exact artifact.
Same maximum period with a different canonical hash is a same-period revision and creates
a new immutable revision/candidate identity. New-period or revised-content fetch/validate
failure blocks publication once governed release lag is exceeded. The lag threshold and
exact Census query remain NRC-B decisions after live evidence.

## 10. Legacy DuckDB inventory (read-only observation)

`data/market_serving.duckdb` was queried read-only on 2026-09-18. The public database had
zero NRC rows; `data/market.duckdb` was absent.

* source: exactly `census_nrc_fred`
* rows: **5,480**; metrics: **2**; geographies: **4**
* canonical-key duplicates (including property type): **0**
* null numeric values: **0**; DuckDB column type is `DOUBLE`
* property fields: every row has `property_type_id='all'` and
  `property_type='all'`
* nation: **absent**, despite the ingest map and registry promising it

| Metric | Geography | Rows | First | Last |
|---|---|---:|---|---|
| completions | `us_region_midwest` | 565 | 1979-01-01 | 2026-01-01 |
| completions | `us_region_northeast` | 565 | 1979-01-01 | 2026-01-01 |
| completions | `us_region_south` | 565 | 1979-01-01 | 2026-01-01 |
| completions | `us_region_west` | 565 | 1979-01-01 | 2026-01-01 |
| starts | `us_region_midwest` | 805 | 1959-01-01 | 2026-01-01 |
| starts | `us_region_northeast` | 805 | 1959-01-01 | 2026-01-01 |
| starts | `us_region_south` | 805 | 1959-01-01 | 2026-01-01 |
| starts | `us_region_west` | 805 | 1959-01-01 | 2026-01-01 |

The 4-geography/region-only state and January 2026 maximum make the checked database
stale/incomplete relative to the declared five-geography contract. They also prove that
running current ingest code against a blank target is not equivalent to reproducing this
legacy database.

## 11. Provider-versus-legacy parity

No honest value-level live parity classification is possible in this environment because
both provider domains were blocked. The frozen comparison algorithm for NRC-B is:

* map both provider and legacy identities to the five governed geographies;
* normalize period labels to month end and property type to `all`;
* outer join on metric/geography/period/property type;
* classify equal numeric values (exact match), provider-only, legacy-only, and unequal
  overlap (revised value); and separately report identity/date mapping failures;
* compare provider decimals exactly after documented parsing—do not use rounding to hide
  differences.

Known classifications before a live join are: nation keys will be provider-only if the
direct product supplies the governed national cells; the legacy database has no
nation rows; and its post-January-2026 periods will be provider-only if currently
published. Unequal overlaps are not automatically defects: corroborate against the pinned
Census input and, optionally, FRED before classifying them as expected revisions. A
first-day versus month-end mismatch is normalization, not provider-only data.

## 12. Downstream compatibility inventory

| Surface | Dependency class | Migration implication |
|---|---|---|
| `config/source_metric_registry.csv` | source ID, metric ID, geography, units | atomic source-ID/unit/geography update; keep metric IDs |
| `config/feature_registry.csv` | logical registry IDs (`nrc_*`) | no change if source registry IDs remain stable |
| `config/indicator_regime_registry.csv` | logical registry IDs | no physical source-ID change required |
| `config/metric_dimension_registry.csv` | logical registry IDs; descriptive | remains diagnostic-only; no scoring promotion |
| `forecast/features/feature_loader.py` | hard-coded source ID, metrics, geography IDs | must consume governed identity/geographies; remove legacy hard-coded mismatch in later scope |
| `forecast/models/xgb/backtest_selector_runner.py` | hard-coded source ID, debug only | update with cutover or derive from registry |
| `scripts/build_serving_snapshot.py` | source-ID history policy | replace physical key at cutover |
| `scripts/validate_serving_snapshot.py` | expected source ID and metric IDs | update source expectation; retain metric expectations |
| `scripts/audit_data_inventory.py` | source-ID cadence | update physical key |
| `jobs/monthly_refresh/orchestrator.py` | source-ID execution inventory | must change only in governed integration phase, not NRC-A/B |
| `config/source_refresh_revision_policy_v0_2.json` | source identity/provider semantics | supersede the review candidate with direct-Census contract later |
| canonical assembly/monthly contracts | required source inventory, descriptive architecture | change atomically with integration, never alias silently |
| Makefile/incremental job/source modules | legacy source and refresh path | retain through parity; retire only after accepted cutover |
| forecast indicator/inventory docs and data inventory | descriptive | update when migration becomes production truth |

The regime normalizer recognizes the `nrc_` logical prefix and is not coupled to the
physical source ID. The registry metrics are diagnostic-only in the production dimension
registry, although the indicator/feature registries and selector universe still consume
them. Thus changing only `source_metric_registry.csv` would break discovery/selector and
serving expectations; cutover must be a bounded coordinated change.

## 13. Fit with shared monthly architecture

NRC fits the common path directly:

`dynamic Census discovery -> exact content-addressed input pin -> immutable census_nrc candidate -> durable cycle result -> common barrier -> direct Source Set entry -> canonical market -> promotion`

One physical source is sufficient. There is no final/provisional provider-family
precedence analogous to BPS and no logical resolver is needed. NRC should be a direct
Source Set input. Current mutable FRED ingestion must remain outside that governed path
until parity and rollback acceptance are complete.

## 14. Explicit non-goals and negative contracts

NRC-A does not fetch/publish a production candidate, enable a hosted workflow, mutate the
execution registry, update pointers, create/promote a Source Set or canonical/serving
market, consume Redfin readiness, change metric/source IDs, retire legacy code, or change
regime scoring. It does not manufacture state, division, metro, county, or local NRC
observations. It does not assert that ordinary FRED is a vintage archive or that unchanged
maximum month means unchanged content.

## 15. Open questions / implementation gates

1. **Blocking NRC-B gate:** capture a successful direct Census API/download response and
   freeze exact URL/query, product table/cell codes, schema, sentinel handling, earliest
   and latest period per geography, and license/terms metadata.
2. Capture the same instant's ten FRED series and prove value/date parity or explain every
   difference, including update timing.
3. Select the canonical five geography IDs from the governed manifest and resolve the two
   conflicting legacy naming schemes.
4. Govern a release-lag threshold using the official release calendar; do not guess one.
5. Confirm from current Census methodology how far annual seasonal-factor/benchmark
   revisions can reach. Full-history hashing remains the safe policy meanwhile.
6. Decide whether raw-provider-byte retention is stable for the chosen surface or whether
   lossless canonical response retention is required for nondeterministic workbook files.

These are bounded input-contract questions, not justification to implement an ambiguous
production path.

## 16. Recommended NRC-B scope

NRC-B should be a read-only, deterministic direct-Census provider-contract verifier. It
should (a) pin one explicit input, (b) validate exact table/schema/cell identities and the
two-by-five applicability matrix, (c) normalize native thousands-SAAR values and month-end
periods without publishing, (d) emit hashes/key inventories, (e) compare against both the
read-only legacy DuckDB and a same-time FRED capture, and (f) produce focused fixtures and
tests for malformed, missing, revised, and same-period-changed inputs. It must stop before
candidate publication or monthly cohort integration. NRC-C can then implement immutable
candidate production; a later bounded integration phase can change source inventory and
downstream consumers atomically.

## Evidence and confidence labels

* **Observed repository/legacy facts:** Sections 2, 10, and the dependency inventory.
* **Provider product facts:** Sections 3, 5-9; stable Census/FRED publication semantics,
  with exact current response fields deliberately gated.
* **Recommendations/frozen future contract:** Sections 1, 4-9, 13, and 16.
* **Unresolved due to blocked live access:** exact Census request/codes, current latest
  periods/values, contemporaneous FRED equivalence, revision depth, and release lag.
