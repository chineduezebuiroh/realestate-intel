# BEA-A governed-source migration reconnaissance v0.1

**Status:** reconnaissance and proposed physical contract only, 2026-09-15.
No BEA call, ingestion, database write, durable pin, candidate publication,
accepted-pointer change, Source Set/canonical/serving/cohort mutation, or workflow
dispatch was performed. Both checked-in databases were opened read-only. Official
BEA pages could not be retrieved in this environment (web facility HTTP 401;
direct HTTPS tunnel HTTP 403). The bounded current-geography conclusions below
are grounded in the first-party BEA documents identified by the reviewer; live
API behavior and every other unverified provider detail remain BEA-B work.

## 1. Executive conclusion

The repository's actual BEA need is narrow: **real all-industry GDP**, delivered
by the BEA Regional dataset as quarterly table `SQGDP9`, line 1, for the nation
and five governed states, and annual table `CAGDP9`, line 1, for the nation,
those states, and governed counties. The current physical source IDs and metric
IDs are already precise and should be retained:

* `bea_gdp_qtr` / `bea_qgdp_real_total_chained2017_saar`;
* `bea_gdp_ann` / `bea_agdp_real_total_chained2017`.

They are different statistical products and frequencies, not interchangeable
provider releases. The downstream registry prefers quarterly GDP where it is
naturally available and retains annual GDP as county market context/fallback.
That existing downstream choice does not justify a new logical `bea` source:
BEA-A recommends two physical governed sources and no new family resolver.

The legacy acquisition is not governed. It makes mutable `Year=ALL` requests,
prints all parameters (including the credential), overwrites two CSVs, discards
unparseable/provider-sentinel values silently, and delete/inserts directly into
DuckDB. It records no response hash or provider release identity. Since a stable
API request can return revised history, an immutable pin must include sanitized
request identity, captured response/normalized-content hashes, response metadata
and retrieval evidence. A changed content hash for the same maximum observation
period must generate a new source revision/candidate, never overwrite one.

The established common lifecycle is sufficient. BEA needs only an adapter for
credential-safe Regional metadata/data calls, table-specific geography planning,
BEA value/status parsing, observation-date conversion, full-history revision
comparison, and immutable response evidence. BEA-B must verify live table/line
metadata, exact geography membership, units/scaling, response metadata and
release detection before this proposal is frozen.

## 2. Repository footprint

### Active legacy runtime

| concern | exact repository evidence | assessment |
|---|---|---|
| Acquisition | `sources/bea/ingest_gdp.py`: `bea_get`, `fetch_regional_qgdp_raw`, `fetch_regional_agdp_raw` | Calls `https://apps.bea.gov/api/data`, dataset `Regional`, tables `SQGDP9`/`CAGDP9`, line 1, `Year=ALL`; writes mutable normalized CSVs under `data/bea/`. It performs no DB write. |
| Credential | `sources/bea/ingest_gdp.py`: module-level `BEA_API_KEY` | Requires `BEA_API_KEY`, falling back to `BEA_API_USER_ID`. `bea_get` currently prints `UserID`; this is a credential-leak defect that a future adapter must not copy. |
| Geography | `load_bea_geo_targets`; `scripts/build_geo_manifest_from_hierarchy.py::apply_bea_resolver` | Reads `config/geo_manifest.generated.csv`; maps nation to `00000`, states to two-digit state FIPS + `000`, counties to five-digit state+county FIPS. Quarterly is enabled only for nation/states; annual also for counties. |
| Parsing | `parse_quarter_to_month_end`, `parse_year_to_year_end` | Converts `YYYYQn` to calendar quarter end and `YYYY` to December 31. Values are comma-stripped floats; failures are silently skipped. Provider availability/suppression markers are not classified. |
| Transform | `sources/bea/transform_gdp.py`: `ensure_dims`, `upsert_fact_timeseries` | Registers two dimensions, deduplicates by canonical four-column key, then delete/inserts directly. No transaction, pin, candidate, release manifest, or dedicated validation. |
| Job | `jobs/incremental_refresh/run_refresh_bea_gdp.py` | Despite its name, fetches `Year=ALL`; targets `SERVING_DB_PATH`, then runs legacy ingest and transform. |
| Full orchestration | `jobs/run_refresh_all.py`; `Makefile` | Refresh-all includes BEA. `Makefile::refresh-bea` is stale/broken at this revision because it calls absent `sources.bea_qgdp.ingest` and `.transform` rather than `sources.bea.*`. |
| Serving checks | `scripts/build_serving_snapshot.py`, `scripts/validate_serving_snapshot.py` | Both physical sources are selected/required; validation maps them to `include_bea_qgdp` and `include_bea_agdp`. This is legacy snapshot behavior, not governed publication. |

### Registry and downstream footprint

* `config/source_metric_registry.csv` is the authoritative current mapping of two
  logical metric keys to the two physical sources/metrics, units, frequencies,
  and geography scopes.
* `config/metric_dimension_registry.csv` assigns quarterly GDP to Demand and
  annual GDP to market context, with `primary_else_fallback` priority 1/2.
  `config/indicator_regime_registry.csv`, `config/feature_registry.csv`, and
  `config/normalization_registry.csv` define their downstream transforms.
* `regime/_02_feature_normalizer.py` recognizes the `bea` source family;
  `regime/_04_asof_aligner.py` explicitly preserves quarterly versus annual
  chronology. GDP/ACS diagnostic and freeze tests are downstream consumers, not
  provider acquisition.
* `config/source_refresh_revision_policy_v0_2.json` already classifies each BEA
  product as revisionary current truth: monthly checks, release-driven full
  history reconciliation, preserve-prior absence semantics and content hashes.
  The exact release probe/lag is deliberately unresolved.
* `jobs/monthly_refresh/orchestrator.py` inventories both as slower-cadence
  sources, and `config/monthly_refresh_policy.json` lists them likewise. Neither
  appears in `config/monthly_source_execution_registry.json`; there is no BEA
  monthly runner, pin, cycle result, candidate, accepted artifact, or BEA hosted
  workflow. Thus BEA is **not yet a governed cohort producer**.

### Dormant/debug, tests, and presentation-only material

* `sources/bea_qgdp/bea_debug_list_regional_tables.py`,
  `bea_debug_list_linecodes_sqgdp9.py`, and
  `bea_debug_list_geofips_sqgdp9.py` are manual metadata probes. They also require
  the key; the table-list script does not strip it, and all are diagnostic only.
* `app.py` primarily reflects the old public snapshot: it labels
  `gdp_real_total`, recognizes only quarterly BEA in several display branches,
  and documents quarter-end dates. It neither retrieves nor governs BEA.
* `.vscode/tasks.json`, data inventories, indicator/feature documents, the v1.0
  release note, and forecast inventories are documentation/task-launch surfaces.
* Smoke tests reference BEA feature and Demand policies, but there is no BEA
  ingestion, pin, artifact, candidate, or provider-contract smoke test.
* No raw BEA CSV is checked in. No separate archived BEA implementation exists;
  the `sources/bea_qgdp` directory contains only the three probes. Git history is
  shallow/grafted here, so historical deletion claims cannot be established.

## 3. Legacy data footprint

The following SQL-equivalent diagnostics were run against both DuckDB files with
`duckdb.connect(path, read_only=True)`: grouped counts/ranges, distinct geography
and property counts, duplicate canonical keys, nulls, non-finite values, and date
month/day distributions. `property_type_id` is always literal `all` (meaning the
GDP observation is not housing-property-type-specific); `property_type` is SQL
NULL. No mixed-frequency physical metric was found.

### `data/market_serving.duckdb`

| source_id | exact metric_id | evidenced interpretation | provider unit | frequency/date meaning | natural/canonical geography | min--max date | rows | geographies |
|---|---|---|---|---|---|---|---:|---:|
| `bea_gdp_qtr` | `bea_qgdp_real_total_chained2017_saar` | Regional `SQGDP9`, line 1, real all-industry GDP | Millions of chained 2017 dollars, SAAR (registry adds SAAR; stored dimension says millions of chained 2017 dollars) | quarterly; calendar-quarter end | nation 1, states 5 | 2005-03-31--2026-03-31 | 510 | 6 |
| `bea_gdp_ann` | `bea_agdp_real_total_chained2017` | Regional `CAGDP9`, line 1, annual real all-industry GDP | Millions of chained 2017 dollars | annual; calendar-year label stored as Dec. 31 | nation 1, states 5, counties 123 | 2001-12-31--2024-12-31 | 3,096 | 129 |

Each annual geography has 24 observations. Each quarterly geography has 85
observations. There are zero duplicate `(geo_id, metric_id, date,
property_type_id)` keys, null values, non-finite values, or null property IDs.
Observed values are positive. The serving annual footprint has only 123 of the
163 counties now enabled in the generated manifest; absence is legacy coverage
drift, not evidence that 40 counties are provider-unsupported.

The generated governed universe contains 375 rows. Its BEA flags select:

| product | nation | governed states | governed counties | CBSA/metro | total |
|---|---:|---:|---:|---:|---:|
| quarterly flag | 1 | 5 (CA, DC, MD, NJ, VA) | 0 | 0 | 6 |
| annual flag | 1 | 5 | 163 | 0 | 169 |

### `data/market_public.duckdb` (older compatibility snapshot)

The public database contains only `bea_gdp_qtr` / old metric
`gdp_real_total`: 328 rows, four old identities (`us_nation`, `dc_state`,
`md_state`, `va_state`), 2005-03-31--2025-06-30, quarterly end dates, zero
duplicate keys/null/non-finite values, and `property_type_id='all'`. It omits CA
and NJ, uses pre-generated-manifest geo IDs, and contains no annual BEA data.
It has no dimension tables. It is compatibility/display evidence, not an
accepted governed prior.

Neither database contains raw response fields, retrieval dates, release IDs, or
multiple vintages, so revisions cannot be reconstructed. The serving/public
overlap can be compared in BEA-B, but different maximum dates and identity names
mean differences alone cannot identify a provider revision.

## 4. Provider product/API mapping

The repository proves the following intended API requests (credential omitted):

```text
method=GetData&DataSetName=Regional&TableName=SQGDP9&LineCode=1
&Year=ALL&GeoFips=<00000,SS000...>&ResultFormat=JSON

method=GetData&DataSetName=Regional&TableName=CAGDP9&LineCode=1
&Year=ALL&GeoFips=<00000,SS000,SSCCC...>&ResultFormat=JSON
```

Repository metric names and table comments identify line 1 as the all-industry
total and distinguish **real GDP** from current-dollar GDP, personal income,
population, and price/index products. Nothing in current acquisition or governed
registries consumes BEA personal income, population, implicit price deflators,
industry detail, national NIPA, International, FixedAssets, or InputOutput data.
Those products are explicitly out of initial scope.

The response fields consumed are `GeoFips`, `TimePeriod`, `DataValue`, `CL_UNIT`
(or `Unit`), and `LineDescription`. `table_name` and `linecode` are written to
mutable CSV but discarded before the fact table. BEA-B must capture the complete
response envelope/notes and metadata rather than assume these are all relevant
release fields.

Primary provider references to capture and hash in BEA-B are:

* [BEA API user guide (PDF)](https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf)
* [BEA API key registration](https://apps.bea.gov/API/signup/)
* [BEA API endpoint](https://apps.bea.gov/api/data)
* [BEA FAQ 101: geographic availability of GDP](https://www.bea.gov/help/faq/101)
* [BEA FAQ 1481: discontinued county-aggregate statistics](https://www.bea.gov/help/faq/1481)
* [BEA discontinued and delayed statistics](https://www.bea.gov/discontinued-and-delayed-statistics)
* [BEA news release schedule](https://www.bea.gov/news/schedule)
* API `GetDatasetList`, Regional `GetParameterList`, and table-scoped
  `GetParameterValues` for `TableName`, `LineCode`, `GeoFips`, and `Year`.

Because network retrieval failed during BEA-A, the current official titles,
units returned for line 1, table-scoped geography lists, metadata fields, and
release/update wording must be verified live. The exception is the narrow 2026
publication-policy evidence supplied for this correction: FAQs 101 and 1481 and
BEA's discontinued-statistics page establish that GDP continues for states and
counties but county-aggregate GDP, explicitly including `CAGDP9`, is discontinued.

## 5. Metric contract matrix

| disposition | physical source | physical metric | dataset/table/line | unit/frequency | canonical date | geography |
|---|---|---|---|---|---|---|
| **KEEP, verify** | `bea_gdp_qtr` | `bea_qgdp_real_total_chained2017_saar` | Regional / `SQGDP9` / 1 | millions chained-2017 dollars, seasonally adjusted annual rate; quarterly | last calendar day of represented quarter | nation + five states only |
| **KEEP, verify** | `bea_gdp_ann` | `bea_agdp_real_total_chained2017` | Regional / `CAGDP9` / 1 | millions chained-2017 dollars; annual | Dec. 31 of represented calendar year | nation + five states + 163 governed counties, subject to direct provider support |
| **DEPRECATE compatibility alias** | `bea_gdp_qtr` | `gdp_real_total` | intended `SQGDP9` / 1 | same intended concept | quarter end | old public DB only |

No adjacent BEA metric is proposed. BEA-B must fail contract freeze if line 1's
live description/unit contradicts the repository meaning, rather than choosing a
different line by name similarity.

## 6. Geography analysis

### Canonical relevance and existing direct mapping

`apply_bea_resolver` performs only identifier translation, not synthesis:

* nation -> `00000`;
* state -> Census state FIPS plus `000`;
* county/county equivalent -> two-digit state FIPS plus three-digit county FIPS.

This creates direct request identities for the one nation, five states, and 163
counties. The legacy data confirms successful direct observations for every
quarterly identity and 123 annual counties. DC is intentionally present both as
a state-level `11000` and county-equivalent `11001`; these are distinct concepts,
not duplicates. The **canonical contract asks for 163 counties**, not the 123
county observations inherited from the legacy snapshot. BEA-B must test every
governed county through the ordinary provider path and explain every absence.

### Generic API capability versus current table publication

These are separate questions and must not be conflated:

1. **Canonical relevance:** no BEA flag or code is assigned to governed CBSAs,
   Metropolitan Divisions, micropolitan areas, Census regions, places, or CSAs.
2. **Generic Regional API capability:** the Regional API/user guide may expose
   general `GeoFips` selectors or area types such as MSA. A generic selector is
   not evidence that a particular table currently publishes that geography.
3. **Current GDP-table publication:** first-party [BEA FAQ
   101](https://www.bea.gov/help/faq/101) states that GDP is published for states
   and counties. [BEA FAQ 1481](https://www.bea.gov/help/faq/1481) and the
   [discontinued-statistics page](https://www.bea.gov/discontinued-and-delayed-statistics)
   establish a 2026 discontinuation of GDP and personal-income statistics for
   county-aggregate geographies, explicitly affecting `CAGDP9`.

The discontinued county-aggregate set includes Metropolitan Statistical Areas,
Micropolitan Statistical Areas, Combined Statistical Areas, Metropolitan
Divisions, and metropolitan/nonmetropolitan portions. Accordingly, MSA, MD,
micropolitan and other county-aggregate GDP are **provider-discontinued for the
initial contract** unless live first-party evidence in BEA-B contradicts the
current documentation. `SQGDP9` remains proposed only for its direct
nation/state scope; `CAGDP9` remains proposed only for directly returned nation,
state and county/county-equivalent observations.

### No synthesis and verification boundary

BEA supplies aggregation methodology/tools with which users may estimate
aggregate geographies from county data. That is not provider publication of a
governed observation. This repository must not use those tools to manufacture
MSA, micropolitan, CSA, Metropolitan Division, metropolitan/nonmetropolitan
portion, or any other aggregate GDP row.

BEA-B should nevertheless inventory table-scoped `GeoFips` metadata by provider
concept (nation, state, county/equivalent, MSA, Metropolitan Division,
micropolitan, other), then compare metadata with actual `SQGDP9`/`CAGDP9`
responses. This documents generic-selector versus table-publication mismatches,
confirms the 2026 discontinuation behavior where possible, and guards against an
accidental code collision; it must not add geographies to the contract. No county
aggregation, metro decomposition, state allocation, interpolation, provider
aggregation-tool output, or crosswalk substitution is permitted.

Historical county-equivalent changes, independent-city status, OMB area
definitions and provider labels may change over a long revised history. The
canonical identity remains repository-governed; unknown/ambiguous returned codes
must fail verification. BEA-B must determine whether `GeoFips` is stable for the
full history or current geography is backcast, and preserve the metadata evidence
used for that decision.

## 7. Temporal and revision semantics

* `TimePeriod=YYYYQn` is represented as the last day of that calendar quarter.
  The value is an SAAR level, not GDP produced during only the final day and not
  a release-date observation. This matches the registry and current parser.
* `TimePeriod=YYYY` is represented as December 31 of that calendar year. It is a
  deterministic annual period label, not the provider publication date.
* Retrieval/publication time is separate lineage and must never replace the
  canonical observation date.
* The repository's `Year=ALL` policy and revision policy treat Regional GDP as
  revisionary current truth. Current raw CSVs and same-key facts are overwritten,
  proving that legacy operation expects same-period replacement, but not which
  values changed or why.
* Routine updates, annual updates and benchmark/comprehensive updates can change
  overlap according to the repository's existing policy. Exact BEA terminology,
  revision windows, coordinated state/county timing, and whether all history is
  reissued must be captured from official release notes in BEA-B. Until then,
  absence is not deletion and full returned history is the defensible comparison
  scope on a detected release.

## 8. Discovery and pinning analysis

“Latest available” must mean the newest **complete provider-current response for
the exact physical contract**, not the current calendar year and not merely the
largest `TimePeriod`. The quarterly and annual products are discovered and pinned
independently. Discovery can query table-scoped `Year`/metadata and a bounded
latest-period request; it must not hardcode today's observed maximum period.

A proposed immutable pin contains:

1. adapter/schema version and physical `source_id`;
2. endpoint origin (without query credential), dataset `Regional`, table, line,
   frequency and `Year` selection;
3. sorted exact canonical-to-`GeoFips` request plan and hashes of governing
   manifest/metric configuration;
4. discovered maximum period and exact provider metadata/result notes available;
5. retrieval UTC, HTTP/result status, response schema/field inventory and row/key
   inventory;
6. SHA-256 of each byte response (or deterministically segmented responses) and
   normalized content, plus a package/root hash;
7. explicit unavailable/sentinel classifications and sanitized request evidence.

Dataset/table/line/frequency/geography/request identity is **not sufficient**:
the API URL is mutable and the same request can yield revised values. Unless
BEA-B finds a documented immutable release/version token covering the data, the
content hashes are the effective release-revision identity. Even if metadata
exposes a release timestamp, retain content hashes.

Normal mode may discover. Once a cycle pin exists, resume/replay must load it and
must not call metadata discovery again. An identical content/root hash reuses the
prior governed artifact (`no_new_release_expected`). A new maximum period or
changed full-history content hash creates a new immutable physical candidate.
A same-maximum-period hash change is an explicit revision candidate; diagnostics
must enumerate changed keys/values before cohort eligibility.

## 9. Credential handling

The intended production API path is treated as requiring a BEA API `UserID`.
Retain `BEA_API_KEY` as the canonical runtime environment variable; support for
legacy alias `BEA_API_USER_ID` may be transitional, but pins must name only the
credential requirement, never its value. Secrets must never be printed,
persisted, hashed, included in URLs/request identities, exception bodies, HTTP
debug logs, artifacts, or PR output. Sanitized request identity must exclude the
`UserID` parameter entirely. Missing credentials must fail a required discovery
or acquisition explicitly. BEA-A did not inspect or request a key.

## 10. Legacy-equivalence strategy

BEA-B should normalize the live response to the proposed keys, then outer-join
each product to serving legacy facts and emit:

* exact key/value matches (with a documented numeric comparison rule);
* provider revisions: same key, finite values differ, provider concept/unit is
  unchanged, and the full response is internally consistent;
* legacy-only and provider-only keys, partitioned by date and geography;
* identity conflicts: one provider code maps to zero/multiple canonical IDs or
  returned label/type conflicts with the manifest;
* unavailable/suppressed/sentinel/text/nonfinite/null rows by exact raw token;
* duplicate provider and canonical keys;
* unexpected period format/date/frequency/unit/line-description changes;
* manifest-required missing and out-of-scope returned geographies;
* serving-versus-old-public overlap after explicit old-to-current geo/metric alias
  mapping, kept separate from provider parity.

Exact equality across all history is not required. A difference is an explained
provider revision only when identity, table/line, unit/scaling, period, geography
and parser contract all agree and the raw response proves a different finite
provider value. Unexplained scale shifts, unit/line drift, duplicate identity,
date misalignment, malformed/silently dropped rows, missing required current
geographies, or inability to reproduce the response hash is a contract failure.
Legacy-only keys are not deletions without governed provider evidence.

## 11. Proposed governed contract

### Physical `bea_gdp_qtr`

* **Product:** BEA Regional `SQGDP9`, line 1, real all-industry GDP.
* **Metric:** only `bea_qgdp_real_total_chained2017_saar`.
* **Unit/frequency/date:** millions of chained 2017 dollars, SAAR; quarterly;
  calendar-quarter end.
* **Geography:** direct `00000` nation and `SS000` for the five governed states.
* **Discovery:** monthly metadata/content check; acquisition on detected change;
  dynamically resolve newest complete quarter.
* **Revision:** full returned history reconciled; changed overlap is a new
  immutable candidate revision; absence preserves prior pending explicit evidence.
* **No synthesis:** no counties, CBSAs, divisions or micropolitan derivation.

### Physical `bea_gdp_ann`

* **Product:** BEA Regional `CAGDP9`, line 1, annual real all-industry GDP.
* **Metric:** only `bea_agdp_real_total_chained2017`.
* **Unit/frequency/date:** millions of chained 2017 dollars; annual; represented
  calendar year at Dec. 31.
* **Geography:** direct nation, five states and the complete 163-governed-county
  universe, contingent on BEA-B exact provider support and an explanation for
  every ordinary-path absence. The legacy 129-geography union is not the contract.
* **Discovery:** monthly metadata/content check; dynamically resolve newest
  complete annual release and all revised history.
* **Revision:** full reconciliation on each detected ordinary or comprehensive
  update; same-period content change produces a new candidate revision.
* **No synthesis:** do not construct missing counties or use BEA aggregation
  methods/tools to estimate discontinued MSA, micropolitan, CSA, Metropolitan
  Division, metropolitan/nonmetropolitan-portion, or other aggregate concepts.

No logical `bea` family is warranted initially. The existing metric-level
primary/fallback policy is a downstream canonical-market rule across distinct
frequencies and geographic applicability; it is not evidence that the provider
products should share one accepted pointer or release clock.

## 12. Common-lifecycle fit

The common sequence—discover, persist immutable pin, execute from pin,
canonicalize, publish immutable candidate, persist durable result—fits without
redesign. The provider adapter alone must:

1. call/snapshot Regional metadata and sanitize credentials;
2. construct deterministic, bounded request batches if URL limits require them;
3. execute the exact pinned request plan without rediscovery;
4. retain/hash responses and relevant provider envelope metadata;
5. parse BEA commas, status/sentinel tokens, units, line descriptions and periods
   fail-closed rather than silently skip;
6. map only pinned direct `GeoFips` identities;
7. compare a complete returned history and expose same-period revision metrics.

Potential gaps to verify, not solve here, are whether the shared pin schema can
represent multiple response parts plus one root hash, and whether cohort
freshness can express quarterly/annual release-driven expectations independently.
Prior source migrations strongly suggest both are adapter/configuration concerns,
not grounds for a BEA lifecycle.

## 13. Open questions and risks

1. What are the live official descriptions, units/scaling, available-year lists,
   and exact table-scoped geographies for `SQGDP9`/`CAGDP9` line 1?
2. Does the Regional response or another BEA endpoint provide a documented
   release/version timestamp or ID suitable for discovery, and what scope does it
   cover? Content hashes remain required either way.
3. What official release-note language governs routine, annual and comprehensive
   revisions, and can annual/quarterly histories change on different schedules?
4. Why are 40 currently governed counties absent from the serving annual union:
   later scope expansion, prior request omission, provider history, or mapping
   failure? Which are directly returned now? Every absence requires an
   explanation; the legacy 129-geography union is not a coverage precedent.
5. How does BEA encode unavailable/suppressed/zero/status values in these exact
   tables, and are `CL_UNIT`, `UNIT_MULT`, `NoteRef` or equivalent fields present?
6. Are nation totals in these Regional tables directly returned with exactly the
   intended concept, rather than a distinct national-account basis? Verify rather
   than assume comparability.
7. Does `Year=ALL` always return a complete current history, and what batching,
   row or URL limits apply to 169 annual targets?
8. Can discovery avoid downloading full history reliably? If no immutable release
   metadata exists, a normalized full-response hash may be the only sound probe.
9. Confirm the legacy SAAR unit mismatch: the quarterly registry/name says SAAR
   while the stored `dim_metric.unit` omits it.

## 14. Recommended BEA-B scope

BEA-B should be **live provider verification plus physical contract freeze only**:

1. With a runtime-only `BEA_API_KEY`, capture `GetDatasetList`, Regional
   parameter metadata, and table-scoped values for table, line, geography and
   year. Capture official API/release/revision documentation status and hashes.
2. Request line 1 for both tables over the complete relevant matrix: nation; all
   five states; **all 163 governed counties** for annual; plus explicit DC
   state/county-equivalent checks. Reconcile every governed county and explain
   every ordinary-provider-path absence. Do not inherit the serving legacy union
   of 129 total geographies as the governed coverage contract.
3. Verify the natural geography coverage of both `SQGDP9` and `CAGDP9` from live
   table-scoped metadata **and data**. Compare generic Regional `GeoFips`/MSA
   capabilities to table-specific results, and confirm the documented 2026
   county-aggregate discontinuation in live metadata/responses where possible.
   Treat MSA, Metropolitan Division, micropolitan and other county-aggregate GDP
   as provider-discontinued for this initial contract unless contrary live
   first-party evidence is captured. Do not publish, add, or synthesize them.
4. Include the latest discovered period, at least one preceding period, and a
   bounded old-period sample for response-field/unit/date testing; also perform
   one `Year=ALL` retrieval per product (deterministically batched if required)
   to measure complete legacy parity and revision behavior.
5. Emit the equivalence categories in section 10, per geography-level coverage,
   row/key inventories, raw sentinel/status inventory, unit/description/schema
   consistency, content hashes, maximum periods, and legacy revision deltas.
6. Repeat at least one identical sanitized request to test response/hash
   stability and determine whether ordering/metadata timestamps require
   canonical normalization before content hashing.
7. Freeze before BEA-C: source/metric IDs; dataset/table/line; units/scaling;
   period conversion; exact geography membership/mapping; credential name;
   sentinel policy; batching/canonical response hashing; discovery evidence;
   release identity; revision/absence/retraction rules; freshness/lag policy; and
   whether the common pin supports multipart evidence.

Stop after producing review evidence and the physical contract. BEA-B must not
publish a candidate, create an accepted pointer, mutate a database, or begin
production integration.
