# ACS-A governed-source migration reconnaissance v0.1

**Status:** reconnaissance and proposed provider contract only, 2026-09-08.
No provider ingestion, database write, artifact/candidate publication, accepted
pointer change, Source Set/canonical/serving change, or workflow dispatch was
performed.

## A. Executive conclusion

ACS fits the established governed lifecycle without a new shared abstraction.
The production-facing registry intentionally contains **four physical metrics**:
population and median household income from each of ACS 1-year and ACS 5-year.
All four are estimates from ACS **detailed tables** (`B01003_001E` and
`B19013_001E`); subject tables, profiles, comparison profiles, percentages, and
margins of error are not part of the legacy governed universe. ACS1 and ACS5
must remain distinct physical sources/releases because they have different
coverage and release dates. Canonical resolution already prefers ACS1 and uses
ACS5 as a same-year fallback.

The existing loader is not suitable as a governed adapter. It derives one
wall-clock vintage as `today.year - 2`, writes a mutable shared CSV, silently
skips several failures, converts Census numeric sentinels to real negative
observations, and mutates DuckDB delete-first. This is current legacy runtime,
not a pin or candidate implementation.

The proposed immutable release identity is product plus survey/vintage year,
exact API dataset and schema/variable identity, exact canonical geography
request plan, provider metadata evidence, and hashes of every normalized raw
response. A Census reissue at the same product/year is therefore a distinct
provider release revision rather than an unnoticed overwrite. Normal discovers
the latest *eligible and complete* year for each product; resume and replay use
the already-recorded pins. No year is derived from the wall clock after a cycle
pin exists.

There is one material **ACS-B decision gate**, not a shared-lifecycle blocker:
ten canonical rows called `cbsa_metro` carry five-digit Metropolitan Division
codes, not Metropolitan/Micropolitan Statistical Area codes. ACS5 has legacy
evidence for 223 of the 233 enabled manifest identities, and all ten absent
identities are those divisions because the legacy loader queried them using the
wrong geography concept. Census geography level 314 provides a direct
Metropolitan Division contract, so absence from the legacy footprint is not
evidence of provider non-support. ACS-B must query level 314 for both products,
correctly classify every identity, and freeze exact per-product membership.

## B. Legacy implementation inventory

### Current production/legacy behavior

| concern | repository evidence | actual behavior |
|---|---|---|
| identifiers | `sources/census_acs/expand_spec.py`, `transform.py` | Provider datasets `acs/acs1`, `acs/acs5`; physical sources `census_acs1`, `census_acs5`. An older public snapshot uses `census_acs`. |
| entry points | `jobs/full_refresh/run_refresh_census_acs.py`, `jobs/incremental_refresh/run_refresh_census_acs.py` | Both run expand -> ingest -> transform -> validate. Full targets `FULL_DB_PATH`; incremental targets `SERVING_DB_PATH`. |
| legacy workflow intent | `.github/workflows/refresh_census.yml` | Monthly cron on day 7 and manual dispatch, but it calls absent `jobs.run_refresh_census`; it is not a governed producer and is not demonstrably runnable at this revision. |
| provider transport | `sources/census_acs/ingest.py` | GET `https://api.census.gov/data/{year}/{dataset}` with `get=NAME,<variables>`, `for`, optional `in`, and `key`. One geography/year/product per request. |
| secret | `sources/census_acs/ingest.py` | Import fails unless `CENSUS_API_KEY` is set. The Census Data API is public and permits limited keyless use; the key is an operational quota credential, not part of release identity. Governed hosted acquisition should continue to require it because bootstrap/history exceeds anonymous limits. |
| plan | `sources/census_acs/expand_spec.py` | Reads enabled `census_code` values from the generated manifest; emits both products for every included geography; hard-codes product starts 2005/2009 and two variables. |
| vintage | same | Sets one plan vintage to current UTC/local date year minus two. This is neither discovery nor provider release evidence. |
| depth | `ingest.py` | Full requests start through plan vintage; incremental requests the last ten vintages. ACS1 2020 is naturally absent (no 2020 ACS1 release), but no explicit exception contract exists. |
| geography | `ingest.py` | Direct US, state, state+county, state+place, and combined metro/micro API clauses. CSA and Metropolitan Division are explicitly unsupported, although division IDs are mislabeled `cbsa_metro` in the generated manifest. |
| conversion | `ingest.py`, `transform.py` | Values parse to float; null/empty/text becomes null; transform drops nulls. Numeric Census special values are not recognized and would survive. |
| canonical rows | `transform.py` | Seven-column fact shape; dates `{year}-12-31`; `property_type_id='all'`; `property_type=NULL`; estimate metric IDs selected by source and variable. |
| database | `transform.py` | Creates dimensions/fact table, deletes any existing row sharing the four-column fact key regardless of source, then inserts directly. This is mutable legacy behavior. |
| validation | `validate.py` | Rejects null critical fields and non-December-31 dates after mutation; reports counts. It does not validate exact plan membership, sentinels, finite/domain values, product availability, or content identity. |
| cadence intent | `config/monthly_refresh_policy.json`, revision policy | ACS1/ACS5 are annual slower-cadence sources checked monthly. They are not yet members of the live seven-source governed cohort policy. |

The shared raw filename and plan filename say `acs5` while containing both
products. No download/file product is used. No ACS-specific derived provider
metric is created: population is a count estimate and income a currency estimate.

### Intended but not yet production

`config/source_refresh_revision_policy_v0_2.json` describes each product as an
immutable-vintage governed source with latest-year discovery, one-vintage normal
refresh, immutable artifacts, content/config hashes, and separate source
identities. `jobs/monthly_refresh/orchestrator.py` inventories both as annual
slower-cadence sources. Neither is wired into the current cohort source list,
input-pin registry, governed runner, artifact catalog, or hosted reusable source
workflow. These files express the intended migration direction, not a live ACS
governed path.

`config/artifact_catalog.json` contains no `acs` text, immutable ACS record, or
accepted ACS pointer. Its accepted physical-source keys are currently CES, FRED
macro, LAUS, and Redfin only. Consequently neither checked-in database is a
durably resolved governed ACS prior; ACS-B bootstrap must compare them as legacy
evidence without treating either as accepted state.

### Dead, stale, or presentation-only behavior

* The checked-in public database and `app.py` use old source `census_acs` and old
  metric IDs `census_pop_total` and `census_median_household_income`. The app's
  family map recognizes that old source while one comparison CASE recognizes
  only `census_acs5`; it does not accurately describe the serving ACS1/ACS5 pair.
* `config/metric_metadata.csv` and the app labels retain the two old generic IDs.
  They are display/catalog compatibility, not current regime source mappings.
* `.github/workflows/refresh_census.yml`, `jobs/run_refresh_all.py`, and
  `jobs/run_refresh_all_hosted.py` reference absent `jobs.run_refresh_census`.
  They are stale entry paths at this revision.
* ACS experiment modules are downstream diagnostics. They do not acquire ACS or
  define provider truth.

## C. Legacy data footprint (read-only evidence)

Both checked-in DuckDB files were opened with `read_only=True`.

### Current serving footprint

| source / physical metric | rows | geographies | years | distinct years | null values | property semantics |
|---|---:|---:|---|---:|---:|---|
| `census_acs1_pop_total` | 2,900 | 157 | 2005--2024, excluding 2020 | 19 | 0 | `all` / SQL NULL |
| `census_acs1_median_household_income` | 2,900 | 157 | 2005--2024, excluding 2020 | 19 | 0 | `all` / SQL NULL |
| `census_acs5_pop_total` | 3,544 | 223 | 2009--2024 | 16 | 0 | `all` / SQL NULL |
| `census_acs5_median_household_income` | 3,544 | 223 | 2009--2024 | 16 | 0 | `all` / SQL NULL |
| **total** | **12,888** | **223 union** | **2005--2024** | | **0** | |

There are zero duplicate four-column fact keys, zero non-finite values, and zero
negative ACS values. `source_id` identifies the physical ACS product, not merely
the Census provider. All values are `E` estimate variables; none is a margin of
error, percentage, or synthesized value. The serving geography footprint maps
exactly to the generated canonical manifest:

| product | nation | states | counties | manifest `cbsa_metro` | total |
|---|---:|---:|---:|---:|---:|
| ACS1 observed union | 1 | 5 | 103 | 48 | 157 |
| ACS5 observed union | 1 | 5 | 163 | 54 | 223 |

Membership varies by year: ACS1 has 148--156 geographies per released year and
ACS5 has 219--223. Thus union count is not a completeness expectation for every
vintage. The current transform cannot synthesize facts; every surviving fact is
one parsed API variable. It can, however, silently omit unavailable responses and
would misinterpret numeric sentinels if encountered.

### Older public footprint

`data/market_public.duckdb` contains 570 old rows: two generic metrics x 19
geographies x 15 years (2009--2023). The geography split is nation 1, states 3,
counties 8, metros 2, and cities 5. It has no null, negative, non-finite, or
duplicate-key values; `property_type_id='all'`, `property_type=NULL`, and
`source_id='census_acs'`. Names and dates identify this as the older ACS5-only
shape. It is serving/display comparison evidence, not a governed prior.

## D. Authoritative provider products

The exact products are:

1. **ACS 1-year Detailed Tables**, Data API dataset path `acs/acs1`.
2. **ACS 5-year Detailed Tables**, Data API dataset path `acs/acs5`.

This is proven by the repository's dataset paths and `B`-table variable codes.
The suffixes mean estimate (`E`); corresponding MOEs would end in `M`. There is
no subject (`S`), data-profile (`DP`), or comparison-profile (`CP`) variable in
the implementation. The products are intentionally mixed only at canonical
resolution: they must never be treated as one provider release.

Official contract references for ACS-B capture:

* [Census ACS 1-year API documentation](https://www.census.gov/data/developers/data-sets/acs-1year.html)
* [Census ACS 5-year API documentation](https://www.census.gov/data/developers/data-sets/acs-5year.html)
* [Census API dataset catalog](https://api.census.gov/data.json)
* [2024 ACS1 B01003 group metadata](https://api.census.gov/data/2024/acs/acs1/groups/B01003.json)
* [2024 ACS1 B19013 group metadata](https://api.census.gov/data/2024/acs/acs1/groups/B19013.json)
* [2024 ACS5 B01003 group metadata](https://api.census.gov/data/2024/acs/acs5/groups/B01003.json)
* [2024 ACS5 B19013 group metadata](https://api.census.gov/data/2024/acs/acs5/groups/B19013.json)
* [Census guidance on ACS 1-year and 5-year estimates](https://www.census.gov/programs-surveys/acs/guidance/estimates.html)

The task environment could not retrieve these official pages: direct `curl`
failed at the network tunnel with HTTP 403 and the web facility returned HTTP
401. The URLs are provider-primary evidence locations, but ACS-B must persist
their current response/status/hash in hosted verification before freezing code.

## E. Proposed metric/variable registry

| canonical physical `metric_id` | legacy name / metric key | product/table | variable | kind | unit/transform | geography applicability | usage | status |
|---|---|---|---|---|---|---|---|---|
| `census_acs1_pop_total` | ACS 1-Year Population / `acs1_population` | ACS1 detailed, B01003 | `B01003_001E` | estimate | people; none | exact verified ACS1 registry subset of nation/state/county/metro | canonical `population`, priority 1 | **KEEP** |
| `census_acs1_median_household_income` | ACS 1-Year Median Household Income / `acs1_median_household_income` | ACS1 detailed, B19013 | `B19013_001E` | estimate | current-year USD; none | same | canonical `median_household_income`, priority 1; affordability input | **KEEP** |
| `census_acs5_pop_total` | ACS 5-Year Population / `acs5_population` | ACS5 detailed, B01003 | `B01003_001E` | estimate | people; none | exact verified ACS5 registry subset of nation/state/county/metro | population fallback, priority 2; permit-intensity input | **KEEP** |
| `census_acs5_median_household_income` | ACS 5-Year Median Household Income / `acs5_median_household_income` | ACS5 detailed, B19013 | `B19013_001E` | estimate | current-year USD; none | same | income fallback, priority 2; affordability input | **KEEP** |

**Proposed count: four; KEEP 4, CORRECT 0, DEPRECATE 0,
NEEDS_DECISION 0.** This classifies the current governed registry. Separately,
the two pre-registry aliases `census_pop_total` and
`census_median_household_income` are **DEPRECATE compatibility IDs**, not extra
governed metrics. Geography membership correction is a contract gate rather than
a metric correction.

No MOE is consumed by current canonicalization, regime scoring, derivation, or
serving. Initial governance should not silently add the `M` variables. ACS-B may
pin their group-schema definitions for validation, but publishing MOEs requires
a separate consumer/schema decision because the common fact schema has no
estimate/MOE pairing field.

## F. Geography contract

The authority remains `config/geo_manifest.generated.csv`, not an ACS-only
universe. Of 375 canonical rows, 233 are `include_census=true`: nation 1, state
5, county 163, and nominal `cbsa_metro` 64, with no blank provider code.

Direct API identity is deterministic for:

* nation: `for=us:1` (`us_nation`; manifest code `00` is not sent);
* state: two-digit state FIPS in `for=state:SS`;
* county/county equivalent: five-digit state+county FIPS split into
  `for=county:CCC&in=state:SS`;
* actual metropolitan/micropolitan statistical area: five-digit CBSA code in
  `for=metropolitan statistical area/micropolitan statistical area:CCCCC`.

ACS5 legacy local evidence covers all configured nation, state, and county rows
and 54 actual CBSAs: **223 observed identities**. The other ten codes (`11244`,
`15804`, `23224`, `31084`, `35084`, `35154`, `36084`, `41884`, `42034`, and
`47894`) are Metropolitan Division identities mislabeled as CBSA metro. They are
not valid under the *legacy loader's* CBSA API clause, but official Census ACS
geography metadata exposes Metropolitan Division level 314. ACS-B must determine
direct level-314 coverage rather than exclude them. No aggregation from counties,
division-to-CBSA substitution, or crosswalk synthesis is permitted.

ACS1 local evidence covers 157 union identities (1 nation, 5 states, 103
counties, 48 CBSAs) and misses 76 enabled identities, including the ten divisions
and smaller published-universe gaps. ACS1 coverage is restricted to sufficiently
large geographies, while ACS5 provides broad small-area coverage. Exact coverage
can change with annual population eligibility and OMB/geographic definitions.

ACS-B must resolve against the selected year's official `geography.json` and
response membership, bind a provider ID to exactly one canonical row, freeze
separate per-product memberships, and fail closed on unknown, ambiguous,
unsupported, or absent required identities. Geographic vintage/OMB definition
evidence belongs in the pin; a changed definition is a reviewed registry change,
not automatic identity drift.

## G. Release, vintage, and observation semantics

* **Survey/data year and ACS vintage:** the year segment in
  `/data/{year}/acs/acs1|acs5`. For ACS5, that year labels the five-year period
  ending in that year; it is not five annual observations and not a point-in-time
  population/income measurement.
* **Provider publication date:** when Census releases that product/year and the
  detailed-table API becomes available. It is distinct from survey year and can
  differ between ACS1 and ACS5.
* **Canonical observation date:** preserve `{survey_year}-12-31`. This is a
  deterministic annual period-end label already assumed by downstream alignment;
  it must be documented as period end, not provider release date. Changing it
  would unnecessarily change consumers. Publication timestamp belongs in pin and
  lineage, not the fact date.
* **Cadence:** each product is annual; the monthly orchestrator performs a
  readiness check and normally reuses the prior accepted artifact when no newer
  eligible vintage exists.
* **2020 ACS1:** no ordinary 2020 ACS1 one-year data product was released. It is
  a product-level unavailable vintage, not a missing row to synthesize.

## H. Dynamic discovery contract

For each physical source independently:

1. Read the official API dataset catalog and select year-specific dataset entries
   whose path is exactly `acs/acs1` or `acs/acs5`; never infer `now - N`.
2. Retain releases whose provider publication availability is on or before the
   cohort discovery cutoff. Sort by numeric survey year and choose the newest.
3. Verify the exact dataset root, required group metadata, variables and estimate
   predicates, geography schema, and a deterministic small probe before declaring
   it available. An HTML release announcement alone is insufficient.
4. Resolve the canonical manifest to the product/year provider geography
   universe; unsupported identities fail or remain explicitly out of that
   product's reviewed registry.
5. Fetch the full frozen plan. Required request/response membership must be
   complete before a pin can be finalized.

**NORMAL** performs these steps once and records the resolved pins before source
execution. **RESUME** reads the existing cycle pins and does no latest-year
selection. **REPLAY** also requires those exact pins and verifies hashes. If a
new ACS release appears mid-cohort, it is ineligible for the already-pinned
cycle; only a later cycle's normal discovery can select it.

Because ACS1 and ACS5 may publish on different dates, “latest ACS” is invalid.
The cycle carries two exact physical release identities (or an explicitly
reviewed logical family record referencing both), never a single maximum year.

## I. Proposed immutable pin identity

Each `census_acs1` / `census_acs5` pin should contain at least:

* pin schema and ACS adapter-contract versions;
* source ID; product (`acs1` or `acs5`); detailed-table dataset path; numeric
  survey/vintage year; canonical period end; observed provider availability/
  release evidence timestamp and URL;
* exact HTTPS API base/root URL (host excluded from arbitrary substitution);
* ordered metric registry: table, variable, predicate/label/concept, estimate
  kind, unit and no-transform rule; hash of group metadata/variables schema;
* exact ordered geography request registry: canonical ID/type, provider type/code,
  `for`/`in`, membership class, and geography-definition/schema hash;
* exact deterministic request partition/query contract (including `NAME`, but
  excluding API key), request-plan/config/manifest hashes, and expected response
  columns/keys;
* raw HTTP status/content SHA-256 per request plus a stable aggregate hash over
  canonical response bytes; retrieval timestamp is lineage but not identity;
* provider release ID such as
  `census-acs-{product}-{year}:<aggregate-content-and-contract-sha256>`.

The candidate remains keyed by the common source/cycle/revision rules. Survey
year alone is insufficient: product is semantically essential, and content hash
distinguishes a same-year Census correction/reissue. API credentials, run ID,
wall clock, response arrival order, and publisher commit must not affect identity.

## J. Missing and special-value semantics

Legacy behavior maps `None`, empty string, and literal `null` to null, maps other
unparseable strings to null, and later drops null rows. HTTP 404/204, empty JSON,
non-JSON, exhausted retry, and missing geography can become silent omission.
This is too permissive for governance.

Census APIs can expose negative numeric special/sentinel values rather than JSON
nulls, including documented classes for null/not available, not applicable,
suppressed, controlled estimates, insufficient observations, and medians outside
reported interval bounds. The often-observed codes include `-999999999`,
`-888888888`, `-777777777`, `-666666666`, `-555555555`, `-333333333`, and
`-222222222`; **ACS-B must verify the exact current official ACS predicate/error
code table before admitting this list into production code**.

Proposed rule:

1. recognize only provider-documented tokens/codes pinned by contract;
2. never cast a sentinel to a legitimate observation and never coerce it to zero;
3. preserve reason and request/geography/variable lineage in diagnostics;
4. omit the canonical fact for a documented unavailable/suppressed value;
5. fail the candidate for unknown tokens, malformed/non-finite values, duplicate
   keys, response/schema mismatch, or unexplained missing required membership;
6. allow reviewed product/geography availability gaps only when encoded in the
   frozen product registry; do not synthesize ACS1 from ACS5 (canonical fallback
   remains the separate downstream resolution rule).

## K. Revision and immutability semantics

Published ACS year/product datasets are operationally vintage-addressable, but
the reconnaissance found no repository mechanism or locally retrievable official
guarantee that bytes for an old API URL can never change. Census can issue data
notes/corrections; metadata or API payloads could therefore change at the same
year URL. Treat “normally immutable” as an acquisition strategy, not proof.

Metadata/group/geography hashes identify schema revision; per-response and
aggregate content hashes identify data revision. A newly observed hash for an
existing product/year must not overwrite its artifact. It is a new revision
under the existing source revision policy, requiring comparison and explicit
acceptance. Absence is not retraction without provider correction evidence.
Candidate identity is product + survey year + content/contract revision hash;
the fact date remains period end.

This tightens the existing policy's “immutable under governed assumption” without
changing shared policy: immutable *pins and candidates* preserve exactly what was
observed even if the provider later changes its mutable endpoint.

## L. Downstream dependencies and consumer impact

* `regime/_01_feature_engine.py` joins fact source/metric IDs to
  `config/source_metric_registry.csv`; all four ACS physical metrics are live.
* `regime/canonical_metrics.py` and `config/metric_dimension_registry.csv` resolve
  ACS1 priority 1 then ACS5 priority 2 into canonical `population` and
  `median_household_income` per geo/date.
* Population and income are annual Market Context/Demand evidence with feature
  families and ACS expanding-percentile normalization. Current policy marks
  ACS5 required/broad and ACS1 preferred where available.
* Population parents derived permit intensity. Median household income parents
  `price_to_income` and `payment_burden`; the affordability pipeline forward
  fills annual income and applies explicit 548/730-day freshness thresholds.
* Canonical source artifacts feed regime scoring, contribution/cancellation,
  demand and affordability diagnostics, persisted macro runs, dashboards, and
  future forecast architecture.
* `app.py` directly reads the public database and exposes old ACS labels/family
  filtering. It already has source-ID inconsistencies that ACS-B must test during
  later canonical/serving integration, but ACS-A changes none of them.

If ACS-B preserves IDs, values, date labels, property fields, and ACS1-over-ACS5
resolution, lifecycle migration alone causes no intended consumer-visible
semantic change. Expected visible differences are limited to newly available
vintage facts, explicit rejection of sentinels, and correction of unsupported
geography claims. Any historical value difference must be classified as provider
revision, legacy omission/error, or blocker before acceptance.

## M. Comparison with governed sources

| source | similarity/difference to ACS |
|---|---|
| FRED | Closest overall: stable year/series-like API resources, exact release discovery, content pin, immutable candidate, and later canonical use. ACS differs through annual product vintages, geography matrices, and period estimates. |
| CES | Similar API-key transport, per-series/geography completeness and product coverage limits. ACS is not a rolling revisionary monthly series and must not use CES overlap semantics. |
| LAUS | Similar canonical FIPS/geography binding and uneven availability. ACS does not need routine rolling/deep revision windows; same-year content change becomes an explicit revision. |
| BPS | Same provider and importance of official release/schema evidence. BPS pins a monthly file snapshot; ACS pins two annual API dataset products and a request/response matrix. |

Use the FRED-style discovery/pin/candidate lifecycle with CES/LAUS-style exact
membership validation. Provider quirks—two products, annual period semantics,
variable metadata, API geography construction, 2020 ACS1 absence, and sentinel
parsing—belong behind the ACS adapter. No common lifecycle change is necessary.

## N. Open decisions and blockers

1. **BLOCKING ACS-B publication:** official hosted evidence must verify current
   dataset catalog, detailed-table variable predicates/units, geography schemas,
   release evidence, API-key/quota behavior, and exact special-value meanings.
   Network access in ACS-A was unavailable.
2. **BLOCKING geography registry:** approve exclusion/correction of the ten
   Metropolitan Division identities and freeze exact ACS1/ACS5 memberships for
   the selected bootstrap vintage. No synthesis is allowed.
3. The four physical metrics must produce exactly **two physical source
   candidates**, one per product and each containing two metrics. Determine how
   the existing cohort policy adds the two sources; do not collapse their pins.
4. Decide whether a future MOE sidecar/metric schema is desired. It is explicitly
   outside initial ACS governance and does not block estimate governance.
5. Read-only bootstrap equivalence must explain the serving union's varying
   annual geography membership and any same-vintage differences before a
   candidate can be accepted.
6. The stale public aliases/app family mappings need a later serving migration
   plan; they are not an ACS-A change and need not block provider adapter tests.

None requires redesign of normal/resume/replay, publication, catalog, accepted
pointers, cohort promotion, or canonical assembly.

## O. Recommended ACS-B scope

1. Capture and hash the official sources above from a hosted environment; record
   an explicit current product/year availability matrix and publication evidence.
2. Add a reviewed, implementation-owned registry for exactly four metrics and
   separate exact ACS1/ACS5 geography memberships derived from canonical IDs.
3. Implement pure discovery -> pin planning -> pinned acquisition -> sentinel-
   aware transform -> exact completeness validation behind the existing common
   artifact interface. Do not write DuckDB in the adapter.
4. Fixture-test dataset discovery, a newer release arriving after pin, 2020 ACS1,
   ACS1 eligibility gaps, county/state/CBSA query construction, division failure,
   schema drift, every verified sentinel, duplicate response keys, resume/replay,
   and same-year changed-content revision identity.
5. Acquire one explicit pinned ACS1 and ACS5 vintage read-only; compare canonical
   rows against both checked-in databases with exact/new/revised/legacy-only/
   missing/sentinel/geography-conflict classifications.
6. Stop for review before publication, acceptance, Source Set integration,
   canonical/serving mutation, workflow dispatch, or legacy retirement unless a
   separate ACS-B authorization explicitly expands scope.

## Evidence commands executed

The following read-only commands/queries produced the concrete findings:

```text
find sources/census_acs -maxdepth 3 -type f -print
rg -n -i 'ACS|American Community Survey|B250|B010|Census' . --hidden ...
sed -n ... sources/census_acs/{expand_spec,ingest,transform,validate}.py
rg -n -i 'acs|census_pop|median_household_income|price_to_income|payment_burden' config regime forecast app.py tests jobs sources
python (pandas counts over config/geo_manifest.generated.csv, read only)
python (duckdb.connect(path, read_only=True); grouped fact footprint, duplicate,
        null, finite, property, date, source, metric, and geography queries)
python (set differences between serving ACS source geographies and generated manifest)
curl -L https://api.census.gov/data.json and official Census documentation URLs
```

The DuckDB inspection did not issue `CREATE`, `INSERT`, `UPDATE`, `DELETE`,
`COPY`, or attachment statements. The failed official requests created only
temporary `/tmp` files outside the repository; no provider payload or generated
artifact was committed.
