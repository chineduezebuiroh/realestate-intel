# BEA-B live provider verification and physical contract freeze v0.1

**Status: LIVE FOLLOW-UP PENDING / contracts not frozen, 2026-09-15.** This is a
verification-only record. No production adapter, provider write, observation
synthesis, candidate, durable pin, pointer, Source Set, database, cohort,
schedule, or workflow was created or changed.

## 1. Executive conclusion

BEA-B could not perform the required live verification because neither
`BEA_API_KEY` nor the legacy fallback `BEA_API_USER_ID` was available in the
process environment. In accordance with the credential-safety stop rule, no BEA
request was attempted and no substitute/sample credential was used. Therefore
neither proposed contract is frozen: `bea_gdp_qtr` and `bea_gdp_ann` are
**REJECTED FOR FREEZE IN THIS RUN (insufficient live evidence)**, not rejected as
statistical products. BEA-C is blocked until BEA-B is rerun with `BEA_API_KEY`.

**Post-run harness update.** A reviewer subsequently demonstrated credential
availability outside Codex and successfully ran the initial verifier: both
physical-source requests returned HTTP success, the quarterly response contained
510 rows, the annual response contained 3,096 rows, and each source's two raw,
normalized-content, and key-inventory hashes matched. Those facts establish
transport and short-interval repeatability only. The initial artifact did not
retain metadata, actual geography/history inventories, sentinel evidence,
batch-equivalence evidence, or live-to-legacy parity, so it cannot freeze either
contract. The verifier and fixture tests are now expanded to collect those
diagnostics on a **second credentialed local run**. This document deliberately
does not anticipate that run's conclusions.

BEA-A remains the starting hypothesis, not live proof. Its proposed table, line,
unit, frequency, date, geography, sentinel, release, and acquisition claims are
all explicitly unfrozen here. The compatibility metric `gdp_real_total` remains
legacy/public-only and was not elevated.

## 2. Credential/authentication result

A name-only environment probe reported `BEA_API_KEY=UNSET` and
`BEA_API_USER_ID=UNSET`; no value was printed, logged, persisted, hashed, or put
in a URL. The exact variable required for a rerun is **`BEA_API_KEY`**. The
bounded verifier prefers it, permits `BEA_API_USER_ID` only as a legacy runtime
fallback, sends the secret in an HTTPS POST body, strips credential-bearing
fields from recorded request identity, and emits credential-free errors.
Authentication and usability could not be tested.

## 3. Live table/line metadata

**Live provider evidence retained here:** only the reviewer-reported
transport/row-count/repeat-hash result summarized above. No sufficient metadata
evidence was retained. Consequently Regional dataset identity,
`SQGDP9`/`CAGDP9` current titles, line 1
descriptions, returned units and multipliers, frequency, period parameters,
table-specific geography metadata, update/vintage metadata, and coverage remain
unverified. Generic Regional capability is not treated as table-specific
capability, and neither is treated as an observed row.

**BEA-A hypothesis:** Regional / `SQGDP9` / line 1 is quarterly real all-industry
GDP; Regional / `CAGDP9` / line 1 is annual real all-industry GDP. This run does
not confirm or falsify either hypothesis.

## 4. Governed metric contract

No metric contract is frozen. The provisional interpretations remain:

| source | metric | proposed provider unit | proposed frequency/date |
|---|---|---|---|
| `bea_gdp_qtr` | `bea_qgdp_real_total_chained2017_saar` | millions of chained 2017 dollars, seasonally adjusted annual rate | quarterly, represented calendar-quarter end |
| `bea_gdp_ann` | `bea_agdp_real_total_chained2017` | millions of chained 2017 dollars | annual, represented calendar-year end (Dec. 31) |

These are legacy/reconnaissance evidence only. Provider units and canonical
units cannot yet be separated authoritatively; no rescaling decision is made.

## 5. Geography verification

**Canonical repository evidence:** the generated manifest selects exactly one
nation and five states (6 identities) for quarterly, and one nation, five states,
and 163 counties/county-equivalents (169 identities) for annual. These are the
verification targets, not legacy coverage.

**Live counts by class:** unavailable for nation, state, county, CBSA/MSA,
Metropolitan Division, micropolitan, CSA, and metro/nonmetro portions (zero
classes verified; this is not a claim of zero provider rows). Natural
publication by either table remains unverified.

## 6. Complete 163-county reconciliation

Every governed county appears exactly once in Appendix A. Because no provider
request was allowed, the narrowly scoped run-state reason
`LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` applies to all 163; it is deliberately not
misrepresented as `PROVIDER_UNAVAILABLE`, `PROVIDER_SENTINEL`, mapping failure,
or request failure. Legacy serving contains 123 of them and lacks 40.

The 40 previously absent identities are: `albemarle_county_va__county`, `augusta_county_va__county`, `campbell_county_va__county`, `charlottesville_va__county`, `colonial_heights_va__county`, `danville_va__county`, `dinwiddie_county_va__county`, `fairfax_city_county_va__county`, `fairfax_county_va__county`, `falls_church_va__county`, `franklin_city_county_va__county`, `frederick_county_va__county`, `fredericksburg_va__county`, `harrisonburg_va__county`, `henry_county_va__county`, `hopewell_va__county`, `james_city_county_va__county`, `lynchburg_va__county`, `manassas_park_va__county`, `manassas_va__county`, `martinsville_va__county`, `montgomery_county_va__county`, `norton_va__county`, `petersburg_va__county`, `pittsylvania_county_va__county`, `poquoson_va__county`, `prince_george_county_va__county`, `prince_william_county_va__county`, `radford_va__county`, `roanoke_county_va__county`, `rockingham_county_va__county`, `salem_va__county`, `southampton_county_va__county`, `spotsylvania_county_va__county`, `staunton_va__county`, `waynesboro_va__county`, `williamsburg_va__county`, `winchester_va__county`, `wise_county_va__county`, `york_county_va__county`.
Read-only legacy evidence establishes only coverage drift; it contains no raw
response/status/lineage capable of proving why each was absent. Their provider
availability must be resolved by the credentialed rerun. Absence was not treated
as a provider fact.

## 7. County-aggregate discontinuation evidence

BEA-A records first-party documentation that county-aggregate GDP/personal-income
publication, including CAGDP9 aggregates, was discontinued for MSA,
micropolitan, CSA, Metropolitan Division, and metropolitan/nonmetropolitan
portions. BEA-B obtained no new live metadata or API behavior with which to
pressure-test that conclusion. The initial proposed contract therefore continues
to exclude all aggregate classes, provisionally and without synthesis; this is
not a frozen live finding.

## 8. Full-history acquisition/batching result

`Year=ALL` was not called. Earliest/latest period, period count, ordering,
pagination, response limits, truncation, completeness, repeat inventory, and
safe batch size remain unknown. The verifier encodes the exact provisional
one-request-per-physical-source plan over explicit canonical membership, but a
credentialed run must test smaller deterministic geography batches against the
combined request before BEA-C freezes batching. HTTP 200 alone will not suffice.

## 9. Temporal/unit semantics

Quarter-end and Dec. 31 mappings are supported by existing repository behavior
and BEA-A, not confirmed live here. Whether SQGDP9 is SAAR, its precise chained
dollar reference year/unit multiplier, CAGDP9's corresponding basis, and whether
BEA has changed either table must be verified. Observation date must remain the
represented period end and must not be confused with retrieval/release time.

## 10. Sentinel/missing-value behavior

No live `DataValue`, structured status, footnote, or annotation was observed.
Accordingly this run freezes **no sentinel token list**. The future parser must
accept only evidenced numeric syntax (including any live-evidenced comma
formatting), explicitly map only live/first-party-evidenced sentinels, retain
annotations, and fail closed on every unknown nonnumeric token.

## 11. Repeat-request hash diagnostics

No request was made, so raw-response, normalized-content, and key-inventory
SHA-256 values are unavailable; byte stability, semantic stability, metadata
mutation, and ordering cannot be assessed. The verifier computes these three
hashes separately and excludes credentials from semantic identity.

## 12. Revision/parity diagnostics

**Legacy database evidence (read-only):** quarterly has 510 rows, 6 geographies,
2005-03-31--2026-03-31; annual has 3,096 rows, 129 geographies (1 nation, 5
states, 123 counties), 2001-12-31--2024-12-31. The older public compatibility
metric `gdp_real_total` has 328 quarterly rows, 4 old identities, and
2005-03-31--2025-06-30. No data was changed.

With no current provider inventory, counts for `EXACT_MATCH`,
`PROVIDER_REVISION`, `LEGACY_ONLY`, `PROVIDER_ONLY`, and `IDENTITY_CONFLICT` are
**not computable** for either physical source, geography class, or period.
Provider extensions likewise cannot be identified. Zero is not reported because
that would falsely imply comparison occurred.

## 13. Provider release/pinning conclusion

Whether BEA exposes a sufficiently immutable release identity remains unanswered
by live evidence. BEA-A's risk analysis still makes provider metadata alone
insufficient as a provisional design assumption. The proposed content-addressed
pin identity to verify is: adapter/schema version; physical source; sanitized
HTTPS endpoint and ordered request/batch plan; Regional/table/line/frequency and
`Year=ALL`; exact sorted canonical-to-provider geography membership; complete
sanitized provider metadata/results notes; per-batch raw SHA-256; normalized
governed-content SHA-256; canonical key-inventory SHA-256; parser/sentinel
classifications; retrieval timestamp as lineage but not semantic content.
Credentials are excluded everywhere.

This is a **pin prototype**, not a production pin or frozen contract.

## 14. Same-period revision identity proof

**Synthetic/local identity test only:** unit tests deterministically alter one
normalized `DataValue` while retaining request/table/period identity and prove
the normalized-content digest changes. This establishes the mechanism, not a BEA
fact. The proposed immutable rule remains: changed governed content for the same
provider table/period creates a new candidate revision and never overwrites an
existing candidate.

## 15. Frozen physical contracts

Neither candidate passes the mandatory evidence gate:

| source | proposed identity | disposition |
|---|---|---|
| `bea_gdp_qtr` | Regional / SQGDP9 / line 1 / quarterly / nation + 5 states | **REJECTED FOR FREEZE IN THIS RUN**; live table, semantics, history, geography, sentinel, repeatability, revision and release evidence absent |
| `bea_gdp_ann` | Regional / CAGDP9 / line 1 / annual / nation + 5 states + 163 counties | **REJECTED FOR FREEZE IN THIS RUN**; same gaps plus complete live county reconciliation absent |

No logical `bea` resolver and no annual/quarterly best-available series is
created. No synthesis is allowed.

## 16. Common-lifecycle fit

BEA-A's provisional conclusion remains plausible: `discover -> persist immutable
pin -> execute from pin -> canonicalize -> immutable candidate publication ->
durable result` needs provider-specific credential-safe POST transport,
metadata/data discovery, geography batching, strict value/status parsing,
period-end conversion, response preservation/hashing, completeness/repeat checks,
and full-history revision comparison. No genuine common-architecture gap was
established, and none was implemented.

## 17. Remaining risks/open questions

All requested live questions remain open: authentication usability; current
table/line/title/unit/frequency; table-specific geography; all 163 county
outcomes and 40 legacy absences; aggregate discontinuation behavior;
`Year=ALL` completeness and limits; sentinels; revisions; release identity; and
repeat stability. **This is an unresolved blocker to BEA-C.** Rerun BEA-B with a
usable `BEA_API_KEY`; do not begin adapter implementation from this record.

## 18. BEA-C implementation scope

BEA-C scope is intentionally not authorized. Once a credentialed BEA-B freezes
both contracts, BEA-C may implement only the verified provider-specific pieces
listed in section 16 and the frozen pin/candidate contract. No implementation was
started here.

## Validation and non-mutation statement

The verification command's missing-credential failure was expected and clean.
Tests cover credential precedence/redaction, fail-closed absence, exact governed
counts, order-independent hashes, mutation sensitivity, and credential-free
request plans. Generated evidence is confined to an ignored directory; none was
generated in this run. Git review confirms no production data/state/workflow
file changed. No provider mutation is possible because the verifier uses GET-only
BEA methods transported by POST and no request ran.

## Appendix A — deterministic county accounting

`legacy` is a read-only footprint comparison. `run classification` records why
live disposition is unavailable and is not a provider classification.

| governed geo_id | name | BEA GeoFips target | legacy | run classification |
|---|---|---:|---|---|
| `alameda_county_ca__county` | Alameda County, CA | `06001` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `albemarle_county_va__county` | Albemarle County, VA | `51003` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `alexandria_va__county` | Alexandria, VA | `51510` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `allegany_county_md__county` | Allegany County, MD | `24001` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `amelia_county_va__county` | Amelia County, VA | `51007` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `amherst_county_va__county` | Amherst County, VA | `51009` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `anne_arundel_county_md__county` | Anne Arundel County, MD | `24003` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `appomattox_county_va__county` | Appomattox County, VA | `51011` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `arlington_county_va__county` | Arlington County, VA | `51013` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `atlantic_county_nj__county` | Atlantic County, NJ | `34001` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `augusta_county_va__county` | Augusta County, VA | `51015` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `baltimore_city_county_md__county` | Baltimore City County, MD | `24510` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `baltimore_county_md__county` | Baltimore County, MD | `24005` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `bedford_county_va__county` | Bedford County, VA | `51019` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `botetourt_county_va__county` | Botetourt County, VA | `51023` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `burlington_county_nj__county` | Burlington County, NJ | `34005` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `butte_county_ca__county` | Butte County, CA | `06007` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `calvert_county_md__county` | Calvert County, MD | `24009` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `camden_county_nj__county` | Camden County, NJ | `34007` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `campbell_county_va__county` | Campbell County, VA | `51031` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `cape_may_county_nj__county` | Cape May County, NJ | `34009` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `carroll_county_md__county` | Carroll County, MD | `24013` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `charles_city_county_va__county` | Charles City County, VA | `51036` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `charles_county_md__county` | Charles County, MD | `24017` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `charlottesville_va__county` | Charlottesville, VA | `51540` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `chesapeake_va__county` | Chesapeake, VA | `51550` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `chesterfield_county_va__county` | Chesterfield County, VA | `51041` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `clarke_county_va__county` | Clarke County, VA | `51043` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `colonial_heights_va__county` | Colonial Heights, VA | `51570` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `contra_costa_county_ca__county` | Contra Costa County, CA | `06013` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `craig_county_va__county` | Craig County, VA | `51045` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `culpeper_county_va__county` | Culpeper County, VA | `51047` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `cumberland_county_nj__county` | Cumberland County, NJ | `34011` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `danville_va__county` | Danville, VA | `51590` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `del_norte_county_ca__county` | Del Norte County, CA | `06015` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `dinwiddie_county_va__county` | Dinwiddie County, VA | `51053` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `district_of_columbia_dc__county` | District of Columbia, DC | `11001` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `dorchester_county_md__county` | Dorchester County, MD | `24019` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `el_dorado_county_ca__county` | El Dorado County, CA | `06017` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `essex_county_nj__county` | Essex County, NJ | `34013` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `fairfax_city_county_va__county` | Fairfax City County, VA | `51600` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `fairfax_county_va__county` | Fairfax County, VA | `51059` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `falls_church_va__county` | Falls Church, VA | `51610` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `fauquier_county_va__county` | Fauquier County, VA | `51061` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `fluvanna_county_va__county` | Fluvanna County, VA | `51065` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `franklin_city_county_va__county` | Franklin City County, VA | `51620` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `franklin_county_va__county` | Franklin County, VA | `51067` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `frederick_county_md__county` | Frederick County, MD | `24021` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `frederick_county_va__county` | Frederick County, VA | `51069` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `fredericksburg_va__county` | Fredericksburg, VA | `51630` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `fresno_county_ca__county` | Fresno County, CA | `06019` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `giles_county_va__county` | Giles County, VA | `51071` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `gloucester_county_nj__county` | Gloucester County, NJ | `34015` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `gloucester_county_va__county` | Gloucester County, VA | `51073` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `goochland_county_va__county` | Goochland County, VA | `51075` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `greene_county_va__county` | Greene County, VA | `51079` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `hampton_va__county` | Hampton, VA | `51650` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `hanover_county_va__county` | Hanover County, VA | `51085` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `harford_county_md__county` | Harford County, MD | `24025` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `harrisonburg_va__county` | Harrisonburg, VA | `51660` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `henrico_county_va__county` | Henrico County, VA | `51087` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `henry_county_va__county` | Henry County, VA | `51089` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `hopewell_va__county` | Hopewell, VA | `51670` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `howard_county_md__county` | Howard County, MD | `24027` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `humboldt_county_ca__county` | Humboldt County, CA | `06023` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `hunterdon_county_nj__county` | Hunterdon County, NJ | `34019` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `imperial_county_ca__county` | Imperial County, CA | `06025` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `isle_of_wight_county_va__county` | Isle of Wight County, VA | `51093` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `james_city_county_va__county` | James City County, VA | `51095` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `kern_county_ca__county` | Kern County, CA | `06029` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `king_and_queen_county_va__county` | King & Queen County, VA | `51097` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `king_william_county_va__county` | King William County, VA | `51101` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `kings_county_ca__county` | Kings County, CA | `06031` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `lake_county_ca__county` | Lake County, CA | `06033` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `lassen_county_ca__county` | Lassen County, CA | `06035` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `los_angeles_county_ca__county` | Los Angeles County, CA | `06037` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `loudoun_county_va__county` | Loudoun County, VA | `51107` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `lynchburg_va__county` | Lynchburg, VA | `51680` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `madera_county_ca__county` | Madera County, CA | `06039` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `madison_county_va__county` | Madison County, VA | `51113` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `manassas_park_va__county` | Manassas Park, VA | `51685` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `manassas_va__county` | Manassas, VA | `51683` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `marin_county_ca__county` | Marin County, CA | `06041` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `martinsville_va__county` | Martinsville, VA | `51690` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `mathews_county_va__county` | Mathews County, VA | `51115` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `mendocino_county_ca__county` | Mendocino County, CA | `06045` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `merced_county_ca__county` | Merced County, CA | `06047` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `mercer_county_nj__county` | Mercer County, NJ | `34021` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `middlesex_county_nj__county` | Middlesex County, NJ | `34023` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `monmouth_county_nj__county` | Monmouth County, NJ | `34025` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `monterey_county_ca__county` | Monterey County, CA | `06053` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `montgomery_county_md__county` | Montgomery County, MD | `24031` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `montgomery_county_va__county` | Montgomery County, VA | `51121` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `morris_county_nj__county` | Morris County, NJ | `34027` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `napa_county_ca__county` | Napa County, CA | `06055` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `nelson_county_va__county` | Nelson County, VA | `51125` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `nevada_county_ca__county` | Nevada County, CA | `06057` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `new_kent_county_va__county` | New Kent County, VA | `51127` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `newport_news_va__county` | Newport News, VA | `51700` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `norfolk_va__county` | Norfolk, VA | `51710` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `norton_va__county` | Norton, VA | `51720` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `ocean_county_nj__county` | Ocean County, NJ | `34029` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `orange_county_ca__county` | Orange County, CA | `06059` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `petersburg_va__county` | Petersburg, VA | `51730` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `pittsylvania_county_va__county` | Pittsylvania County, VA | `51143` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `placer_county_ca__county` | Placer County, CA | `06061` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `poquoson_va__county` | Poquoson, VA | `51735` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `portsmouth_va__county` | Portsmouth, VA | `51740` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `powhatan_county_va__county` | Powhatan County, VA | `51145` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `prince_george_county_va__county` | Prince George County, VA | `51149` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `prince_george_s_county_md__county` | Prince George's County, MD | `24033` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `prince_william_county_va__county` | Prince William County, VA | `51153` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `pulaski_county_va__county` | Pulaski County, VA | `51155` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `queen_anne_s_county_md__county` | Queen Anne's County, MD | `24035` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `radford_va__county` | Radford, VA | `51750` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `rappahannock_county_va__county` | Rappahannock County, VA | `51157` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `richmond_city_county_va__county` | Richmond City County, VA | `51760` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `riverside_county_ca__county` | Riverside County, CA | `06065` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `roanoke_city_county_va__county` | Roanoke City County, VA | `51770` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `roanoke_county_va__county` | Roanoke County, VA | `51161` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `rockingham_county_va__county` | Rockingham County, VA | `51165` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `sacramento_county_ca__county` | Sacramento County, CA | `06067` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `salem_va__county` | Salem, VA | `51775` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `san_benito_county_ca__county` | San Benito County, CA | `06069` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `san_bernardino_county_ca__county` | San Bernardino County, CA | `06071` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `san_diego_county_ca__county` | San Diego County, CA | `06073` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `san_francisco_county_ca__county` | San Francisco County, CA | `06075` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `san_joaquin_county_ca__county` | San Joaquin County, CA | `06077` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `san_luis_obispo_county_ca__county` | San Luis Obispo County, CA | `06079` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `san_mateo_county_ca__county` | San Mateo County, CA | `06081` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `santa_barbara_county_ca__county` | Santa Barbara County, CA | `06083` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `santa_clara_county_ca__county` | Santa Clara County, CA | `06085` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `santa_cruz_county_ca__county` | Santa Cruz County, CA | `06087` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `shasta_county_ca__county` | Shasta County, CA | `06089` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `solano_county_ca__county` | Solano County, CA | `06095` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `somerset_county_nj__county` | Somerset County, NJ | `34035` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `sonoma_county_ca__county` | Sonoma County, CA | `06097` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `southampton_county_va__county` | Southampton County, VA | `51175` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `spotsylvania_county_va__county` | Spotsylvania County, VA | `51177` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `st_mary_s_county_md__county` | St. Mary's County, MD | `24037` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `stafford_county_va__county` | Stafford County, VA | `51179` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `stanislaus_county_ca__county` | Stanislaus County, CA | `06099` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `staunton_va__county` | Staunton, VA | `51790` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `suffolk_va__county` | Suffolk, VA | `51800` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `sussex_county_nj__county` | Sussex County, NJ | `34037` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `sussex_county_va__county` | Sussex County, VA | `51183` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `sutter_county_ca__county` | Sutter County, CA | `06101` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `talbot_county_md__county` | Talbot County, MD | `24041` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `tehama_county_ca__county` | Tehama County, CA | `06103` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `tulare_county_ca__county` | Tulare County, CA | `06107` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `tuolumne_county_ca__county` | Tuolumne County, CA | `06109` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `union_county_nj__county` | Union County, NJ | `34039` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `ventura_county_ca__county` | Ventura County, CA | `06111` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `virginia_beach_va__county` | Virginia Beach, VA | `51810` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `warren_county_va__county` | Warren County, VA | `51187` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `washington_county_md__county` | Washington County, MD | `24043` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `waynesboro_va__county` | Waynesboro, VA | `51820` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `williamsburg_va__county` | Williamsburg, VA | `51830` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `winchester_va__county` | Winchester, VA | `51840` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `wise_county_va__county` | Wise County, VA | `51195` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `yolo_county_ca__county` | Yolo County, CA | `06113` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `york_county_va__county` | York County, VA | `51199` | absent | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
| `yuba_county_ca__county` | Yuba County, CA | `06115` | present | `LIVE_NOT_RUN_CREDENTIAL_UNAVAILABLE` |
