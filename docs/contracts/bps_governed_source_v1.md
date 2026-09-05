# Governed Census BPS source contract v1

**Status:** Compiled and provisional provider contracts verified; provider-compatible CBSA subset promoted. Family resolution, acceptance, source-set membership, and scheduling changes are not authorized by this contract-closure pass.

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

The exact geography registry is `config/bps_governed_geographies_v1.csv`. It freezes 221 compiled-applicable identities: one nation, five states, 162 counties, and 53 provider-compatible canonical CBSA identities. Provisional logical applicability is the 220 state/county/CBSA identities; the nation is compiled/final-only because the provisional family has no national member. Provider binding is exact `(location_type, provider identifier)` to canonical `geo_id`; name matching is prohibited. `franklin_city_county_va__county` is enabled in the broader generated manifest but absent from the accepted local BPS truth and is excluded pending provider verification. Eleven configured `cbsa_metro` identities are intentionally unsupported as classified below. A provisional physical-release gate requires all five configured states and all 162 configured counties. Configured provider-compatible CBSAs may be absent from an individual provisional release, but each absence is explicitly inventoried and never synthesized. The gate never manufactures a national row and does not replace exact five-digit codes with names, fuzzy matches, parent metros, or Metropolitan Divisions. Temporal missingness remains legitimate and no per-month Cartesian completeness is asserted.

For exact-pinned provisional release `2607`, configured logical applicability is
220 while physical presence is 217: five states, 162 counties, and 50 CBSAs.
The three configured-but-absent identities are California, MD (`15680`), Madera,
CA (`31460`), and Ocean City, NJ (`36140`). Martinsville, VA (`32300`) and San
Luis Obispo, CA (`42020`) are present. These facts describe only the physical
provisional parent. The cross-parent intersection and union must be recomputed
from both exact physical parents before family resolution; no cross-parent
classification is inferred from provisional absence alone.

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

### Physical-candidate republication from an immutable pin

A promoted BPS transformation or geography change may require a new physical
candidate even though provider identity and bytes did not change.  This is an
explicit, manually dispatched republication, not normal, resume, or replay.  It
validates an existing successful monthly result and its cataloged `r1` object,
then validates and retrieves only that cycle/source's immutable input-pin URLs.
Every retrieved member must match its pinned SHA-256 before canonicalization.

The bounded v1 republication supports only revision `2` with an explicit valid
`r1` parent.  Its artifact records both `prior_artifact_id` (backward-compatible
reader terminology) and `supersedes_artifact_id` (correction semantics), plus
the parent content hash, deterministic republication ID, current
`bps_governed_source_v1` adapter identity, and governed configuration hashes.
The create-once record is stored separately at
`config/monthly_source_republications/<parent_cycle_id>/<source_id>/<republication_id>.json`.
It never replaces the monthly result, changes an accepted pointer, resolves the
BPS family, creates a Source Set, consumes Redfin readiness, or writes DuckDB.

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

### LIVE 2607 CLOSURE EVIDENCE

Dynamic discovery resolved coherently to official members `State/st2607c.txt`,
`County/co2607c.txt`, and `CBSA (beginning Jan 2024)/cbsa2607c.txt`, all release
ID `2607` and sole observation month 2026-07-01. The retained diagnostics found
167 of 168 compiled-governed identities: all five states and all 162 counties,
with only `united_states__nation` absent. Canonical output was 167 rows; 3,837
distinct provider geographies were out of governance. There were no observed
nonnumeric/unavailable tokens, identical duplicate keys, or conflicting
duplicate keys. The estimate-side four-unit-band encoding was consistent in all
members and numeric zero occurred legitimately.

The exact state member contains 14 provider aggregates: national `US / 0 / 0 /
United States`; regions `R1`–`R4`, with the matching region number, division
zero, and exact Census region name; and divisions `D1`–`D9`, with the expected
region and division numbers and exact Census division name. Verification uses
an explicit tuple inventory and classifies these as
`PROVIDER_NATIONAL_SUMMARY`, `PROVIDER_REGION_SUMMARY`, or
`PROVIDER_DIVISION_SUMMARY` in out-of-governance raw evidence before canonical
identity formation. They never emit measures, form canonical identities, or
count toward physical governed coverage. Prefix matching is prohibited: every
other nonnumeric state identifier and every contradiction in a recognized
aggregate tuple fails closed. Numeric state identifiers continue through exact
governed identity mapping; county and CBSA identifiers remain subject to their
strict bounded numeric validation. No analogous nonnumeric identifier occurs
in the county or CBSA member.

Legacy provisional comparison produced 0 exact matches, 167 prior-only May
rows, 167 provider-only July rows, 0 provider revisions, and 50 out-of-governance
CBSA rows. Because the governed months do not overlap, this is expected and is
not failed equivalence. Out-of-governance rows are labeled separately from
governed identity conflicts; governed identity-conflict checking remains
fail-closed.

### UNRESOLVED

No nonnumeric token is assigned suppression, nonresponse, or unavailable semantics without first-party evidence. The absence of such tokens in 2607 is not proof that they never occur. Provisional omission cannot yet be classified as retraction; it must not be interpreted as zero. These token meanings and omission/retraction semantics remain unresolved and must stay conservative.

### FUTURE PRODUCTION-INTEGRATION REQUIREMENT

BPS must not own a parallel lifecycle. A new cohort dynamically discovers compiled and provisional states independently and then immutably pins their release IDs, physical URLs, and hashes. Execution, resume, and replay consume those pins and never rediscover. Later production work must reuse common candidate publication, durable results, promotion, and barrier contracts while keeping compiled and provisional artifacts separate. This verification pass performs none of those production steps.

## 2026-09-03 CBSA canonical-concept closure

The exact-pinned hosted CBSA verification closes the deferred geography gate.
The canonical `cbsa_metro` namespace is intentionally heterogeneous; its 64
identities comprise 43 Metropolitan Statistical Areas, 11 Micropolitan
Statistical Areas, and 10 Metropolitan Divisions.  The machine-readable concept
classification and rationale are frozen in
`config/bps_cbsa_canonical_concepts_v1.csv`; a display label is never concept
evidence.

BPS admits the 53 exact-code identities observed in at least one pinned parent:
all 43 Metropolitan Statistical Areas and 10 current provider-supported
Micropolitan Statistical Areas.  Ten Metropolitan Divisions are unsupported
because BPS does not publish them as CBSA product identities.  Historical Big
Stone Gap code `13720` is also unsupported because it is absent from both exact
pins and is not a current governable BPS identity.  Unsupported concepts remain
absent: BPS performs no synthesis, aggregation, decomposition, fuzzy match, or
metro-division derivation.

Compiled provider code `9999`, normalized to `09999`, is
`NON_GOVERNABLE_PROVIDER_PLACEHOLDER`.  Its reuse across unrelated names and
values in every month from 1988 through 2003 proves that it cannot identify one
canonical geography.  It is excluded before canonical identity and duplicate
formation; no temporal crosswalk is created.  A differing-value duplicate for
any other, governable five-digit code continues to fail closed.

The final applicability is compiled nation/state/county plus the
provider-compatible canonical CBSA subset; provisional state/county plus that
CBSA subset (and no nation); and logical BPS nation/state/county plus that CBSA
subset.  `PROMOTE_CBSA` means exact-code mapping to a compatible concept,
settled `TOTAL_UNITS` semantics, no unresolved tokens, no conflict on a
governable provider code, explicit placeholder exclusion, and unsupported
canonical concepts left absent.  It does not require BPS to cover every concept
stored under the shared `cbsa_metro` level.
