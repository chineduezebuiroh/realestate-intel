# Governed Census BPS source contract v1

**Status:** Compiled provider contract verified; provisional read-only verification implemented. Publication, acceptance, source-set membership, master fan-out, and scheduling are not authorized.

## Provider and acquisition

The governed interface is the U.S. Census Bureau Building Permits Survey (BPS) **compiled master ZIP**, distributed from `https://www2.census.gov/econ/bps/Master%20Data%20Set/` with release-specific names `BPS_Compiled_File_YYYYMM.zip`. This is file/URL acquisition, not the Census Data API. It needs no API key. A deterministic request names one reviewed `YYYY-MM` release; discovery of “latest” is an availability operation and must not replace the release identity inside a request plan.

Official evidence locations reviewed or identified for hosted verification are:

* BPS program: `https://www.census.gov/construction/bps/`
* methodology: `https://www.census.gov/construction/bps/methodology/`
* definitions: `https://www.census.gov/construction/bps/definitions/`
* technical documentation: `https://www.census.gov/construction/bps/technical_documentation/`
* compiled distribution: `https://www2.census.gov/econ/bps/Master%20Data%20Set/`

Census hosts monthly, annual, and year-to-date records in the compiled distribution. V1 admits only records explicitly marked `Monthly`. The existing compiled columns expose units authorized, buildings, valuation, four structure-size bands, `survey_date`, and reporting metadata. The provider material is not seasonally adjusted at the governed local geography boundary; V1 is `NSA`.

The Codex environment could not reach Census (`CONNECT tunnel failed, response 403`) on 2026-09-01. Consequently, the following are deliberately unresolved rather than inferred: the authoritative token vocabulary for suppression/unavailability; whether omission represents suppression, nonresponse, or retraction; a bounded historical revision depth; and a provider guarantee for compiled-release finality. These items must be verified from the official technical documentation and a fresh hosted payload before publication.

## Governed v1 footprint

The exact geography registry is `config/bps_governed_geographies_v1.csv`. It freezes 168 identities already present in the best local production truth: one nation, five states, and 162 counties. Provider binding is exact `(location_type, provider identifier)` to canonical `geo_id`; name matching is prohibited. `franklin_city_county_va__county` is enabled in the broader generated manifest but absent from the accepted local BPS truth and is excluded pending provider verification. All 64 configured CBSA identities are out of v1 because compiled/local coverage was not proven. Thus v1 has one metric, 168 geographies, and 168 configured metric/geography identities (temporal missingness is legitimate and no per-month Cartesian completeness is asserted).

| Metric ID | Provider measure | Description | Unit / scaling | SA | Frequency | Class | Downstream consumers |
|---|---|---|---|---|---|---|---|
| `census_bp_total_units` | `total_units` (sum of `units_1`, `units_2`, `units_3_4`, `units_5plus` only when all component semantics are verified) | Housing units authorized by building permits | housing units; none | NSA | monthly | required | source metric `bps_total_units`; canonical `permit_activity`; `derived_permit_intensity`; Supply features, score/regime, dashboards and forecasting inputs |

Existing metrics classified **legacy-only/out-of-scope** are `census_bp_1_unit`, `census_bp_2_units`, `census_bp_3_4_units`, `census_bp_5plus_units`, `census_bp_total_bldgs`, `census_bp_1_unit_bldgs`, `census_bp_2_units_bldgs`, `census_bp_3_4_units_bldgs`, `census_bp_5plus_units_bldgs`, `census_bp_total_value`, `census_bp_1_unit_value`, `census_bp_2_units_value`, `census_bp_3_4_units_value`, and `census_bp_5plus_units_value`. None is a direct governed downstream registry input. There are no diagnostic v1 metrics. Provider valuation scaling remains unresolved, reinforcing its exclusion.

## Canonical rows

Column order is:

```text
geo_id, metric_id, date, property_type_id, value, source_id, property_type
```

* `source_id=bps`, `property_type_id=all`, and `property_type=all`.
* Provider `year`/`month` becomes the first calendar day of that observation month, preserving the established BPS chronology. It is not month-end shifted.
* `value` is finite float64 housing-unit count with scale transform `none`. Provider zero is retained as zero.
* Identity/uniqueness is `(geo_id, metric_id, date, property_type_id)`. Identical duplicate values collapse deterministically and are counted; different values for one key fail closed. No geography-specific exception exists. Rows sort stably on that key.
* Actual nulls are omitted and diagnosed, never made zero. No textual unavailable/suppression token is admitted until the hosted payload and official documentation establish its meaning; every nonnumeric string currently fails closed.
* Annual/year-to-date rows, unknown geography types/IDs, negative/nonfinite values, malformed dates, and schema/contract drift fail closed. There is no interpolation or carry-forward.

## Revision, target, and lifecycle policy

Each compiled release is provisionally planned as a complete acquisition, but
BPS-B has not yet obtained provider evidence proving that description. An
arbitrary rolling LAUS/CES-style overlap is therefore not introduced. The
fail-safe candidate policy remains: returned/provider-only values win or append;
prior-only governed rows persist; omission is not deletion without explicit
retraction evidence. This is not a final deletion-semantics decision.

Outcome **D** still applies to deep-refresh policy: automation is deferred until
hosted verification establishes whether each compiled release contains complete
history. No LAUS-style annual subsystem is warranted without that evidence.

`production cycle target_month` identifies the cohort. `BPS source target_month` is derived deterministically from the maximum canonical monthly BPS observation returned by the selected compiled release after required-identity validation. `observation_max` is the corresponding date (first of month). These values may lag or differ from the cohort catalyst month.

The future routine lifecycle reuses common source-artifact publication, durable verification, execution results, cycle results, and cohort barrier. It must resolve the prior accepted artifact durably; plan/acquire/canonicalize/reconcile/validate; build and publish or reuse an immutable candidate; verify it remotely; and persist `monthly_source_execution_result_v1`. It must never advance the accepted BPS pointer directly.

Modes use common semantics: `normal` runs/reuses the cycle candidate; `resume` reuses an eligible successful durable result or reruns missing/failed work; `replay` deterministically audits historical work without accepted-state/control-plane mutation.

## Failure taxonomy

Retryable: connection timeout/reset, HTTP 408/429, and HTTP 5xx, exhausted through the common bounded retry pattern. Terminal: malformed ZIP/CSV, unknown schema/geography/metric, missing required validated coverage, identity collision, unknown suppression token, invalid numeric/date/unit, immutable publication conflict, corrupt durable resolution, or local/artifact validation failure. A documented provider-unavailable value is diagnosed and excluded; it becomes terminal only when a later completeness gate proves a required controlling identity absent. Structural contradictions always fail closed.

## BPS-B pinned verification boundary

`jobs.monthly_refresh.bps_bootstrap` is the only BPS-B execution surface. It
requires an explicit `YYYY-MM`, derives or accepts the corresponding official
compiled URL, hashes the ZIP and sole CSV member, inventories the normalized
schema and nonnumeric total-field values, produces exact 168-binding coverage,
and compares read-only with compiled-only and compiled-plus-provisional local
truth. It has no Release publisher, catalog CAS, activation, or cohort code.

The manually dispatched `bps-pinned-verification.yml` workflow exists only
because Census access is blocked from the Codex environment. Its artifacts are
diagnostic and retained for 30 days; they are not durable governed source
artifacts. Until that workflow's evidence is reviewed, `total_units`, textual
missing-value tokens, geography presence, omission semantics, and full-history
behavior remain provisional rather than provider-proven.


## 2026-09-02 provider-evidence amendment

### OBSERVED / PROVEN

The official **Compiled Data Documentation** record layout establishes `TOTAL_UNITS` as “Total units” under “Estimates With Imputation” and separately defines `TOTAL_UNITS_REP` as reported-only data. The governed published estimate is therefore `TOTAL_UNITS`, not the reported-only field. Historical verification evidence (not a current default) is compiled release `2026-04`, ZIP `BPS_Compiled_File_202604.zip`, member `New_Master_python_m2604.csv`, ZIP SHA-256 `d39854daab95c7f2e2ba5a3ae49162bfc750ecb0933ef48a6632e64ad089a391`, member SHA-256 `ee47a942b7e1fdc0210b15033300e3d3ac89e1d82a996ee78241c4fb248ed680`, and 11,674,305 raw rows. Governed output spans 1988-01 through 2026-04.

All 168 configured identities occur in that release, but their first observations vary. Current-release presence and historical continuity are distinct. The 24 duplicate excess rows form 24 canonical keys with identical values; they may collapse deterministically. Conflicting duplicate values fail closed. Compiled-only legacy comparison was 29,840 exact, 0 revisions, 0 prior-only, 10,664 provider-only, and 0 identity conflicts. Serving-only May provisional effects are not compiled conflicts.

County identity resolves exactly through `county_fips` or `fips_county_5_digits`. `county_code` is only the three-digit component and is not an alias.

For provisional, state, county, and CBSA are separate files in one provider state. Historical evidence (never a production default) is coherent release `2607` / 2026-07: `st2607c.txt`, `co2607c.txt`, and `cbsa2607c.txt`. Each file has three descriptive heading lines followed by headerless comma-delimited records. State has survey date, two-digit state FIPS, region, division, name, then 24 measure positions. County additionally has a three-digit county component, safely combined with state FIPS. CBSA has CSA, five-digit CBSA, header marker, name, then the same measure positions. The positions comprise building/unit/value estimates for four structure bands and their reported-only counterparts.

### CONTRACT DECISION

The compiled artifact is a complete-history snapshot; no LAUS-style annual/deep-history path is appropriate. Provider omission is not zero, and no historical months are synthesized. The provisional governed value is the sum of all four estimate-side unit positions only when every component is numeric. The reported-only positions are not substituted. Numeric zero is legitimate. Release ID must agree across all three member filenames, and the common release month must agree with the sole survey month. Mixed releases, conflicting duplicates, malformed layouts, unsafe identifiers, and release/date contradictions fail closed. Identical duplicates collapse with explicit counts. CBSA identities outside the 168-identity registry remain a separate out-of-governance inventory.

`jobs.monthly_refresh.bps_provisional_verification` is read-only evidence tooling. It uses existing dynamic discovery or requires a complete explicit three-URL pin, records URLs, retrieval times, HTTP metadata, byte sizes, SHA-256 hashes, schemas/counts, coverage, token and duplicate diagnostics, canonical bounds, and optional read-only legacy comparison. It cannot publish or mutate a pointer/database.

### UNRESOLVED

No nonnumeric token is assigned suppression, nonresponse, or unavailable semantics without first-party evidence. Provisional omission cannot yet be classified as retraction; it must not be interpreted as zero. A retained successful live output is still needed to freeze exact 2607 counts, hashes, full geography footprint, governed coverage, out-of-governance CBSA inventory, token vocabulary, duplicates, and legacy equivalence. Direct Census transport in this implementation environment returned proxy HTTP 403, so those values are not invented.

### FUTURE PRODUCTION-INTEGRATION REQUIREMENT

BPS must not own a parallel lifecycle. A new cohort dynamically discovers compiled and provisional states independently and then immutably pins their release IDs, physical URLs, and hashes. Execution, resume, and replay consume those pins and never rediscover. Later production work must reuse common candidate publication, durable results, promotion, and barrier contracts while keeping compiled and provisional artifacts separate. This verification pass performs none of those production steps.
