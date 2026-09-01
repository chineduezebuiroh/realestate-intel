# Governed Census BPS source contract v1

**Status:** BPS-B verification target; still blocked on hosted provider evidence. Publication, acceptance, source-set membership, master fan-out, and scheduling are not authorized.

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
* Identity/uniqueness is `(geo_id, metric_id, date, property_type_id)`; duplicates are rejected, not selected heuristically. Rows sort stably on that key.
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
