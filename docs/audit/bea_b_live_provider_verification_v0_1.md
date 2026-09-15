# BEA-B live provider verification and physical contract freeze v0.1

**Status:** annual contract frozen; quarterly contract awaiting one corrected
bounded live check, 2026-09-15. This is verification and contract-freeze work
only. No production adapter, candidate, durable pin, pointer, Source Set,
database, cohort, schedule, workflow, logical resolver, or synthesized
observation was created or changed.

## 1. Executive conclusion

Two credentialed local verifier runs established the physical statistical
contracts and all substantive coverage/parity findings. `CAGDP9` line 1 is
**FROZEN** as `bea_gdp_ann`. `SQGDP9` line 1's metric, geography, history, value,
parity, and repeatability evidence is confirmed, but its acquisition shape is
**NOT YET FROZEN**: the first bounded explicit-period probe incorrectly sent
quarter labels in the Regional `Year` parameter. The verifier now sends calendar
years and compares only the selected boundary quarters. A successful corrected
credentialed run is the sole remaining BEA-B gate. BEA-C is not yet authorized.

The compatibility metric `gdp_real_total` remains legacy/public-only. Annual and
quarterly products remain separate physical sources; no logical `bea` resolver
or best-available composite is justified.

## 2. Credential/authentication result

A real `BEA_API_KEY` was demonstrated usable in local runs. The verifier accepts
the modern variable first and legacy `BEA_API_USER_ID` only as fallback, sends it
in an HTTPS POST body, and removes credential fields from artifacts, request
identity, errors, and hashes. The credential participates in neither semantic
identity nor lineage.

## 3. Live table/line metadata

Evidence layers are deliberately separate:

* **Generic Regional capability:** dataset/parameter/table lists describe the
  API's general selectors. Generic geography selectors are not publication proof.
* **Table-specific metadata:** filtered metadata binds line, year, and geography
  parameter evidence to `SQGDP9` or `CAGDP9`.
* **Actual GetData evidence:** returned fields and periods establish observations,
  direct geography availability, line description, units, and frequency.

The verified products are Regional `SQGDP9`, **Real GDP by state**, line 1,
**All industry total**, quarterly; and Regional `CAGDP9`, **Real GDP by county**,
line 1, **All industry total**, annual. GetData reports millions of chained 2017
dollars for both; SQGDP9 is seasonally adjusted at annual rates. Available-period
metadata and actual periods agree with the histories below. Metadata/notes did
not expose a provider release identifier sufficient to make an acquisition
immutable by itself.

## 4. Governed metric contract

| source | metric | provider/canonical units | frequency | canonical date |
|---|---|---|---|---|
| `bea_gdp_qtr` | `bea_qgdp_real_total_chained2017_saar` | millions of chained 2017 dollars, SAAR; no rescaling | quarterly | last calendar day of represented quarter |
| `bea_gdp_ann` | `bea_agdp_real_total_chained2017` | millions of chained 2017 dollars; no rescaling | annual | Dec. 31 of represented calendar year |

Dates represent observation periods, never retrieval or release dates.

## 5. Geography verification

### SQGDP9 actual GetData

Requested 6 governed identities and returned exactly 6: nation 1 and states 5.
Requested-but-not-returned 0; returned-but-not-requested 0; mapping failures 0.
This proves the proposed direct nation/state scope. It does not prove county or
metro publication and the contract includes neither.

### CAGDP9 applicability versus availability

The canonical applicability universe is 169 identities: nation 1, states 5, and
163 counties/county-equivalents. Actual GetData returned 129: nation 1, states 5,
and counties/county-equivalents 123. Exactly 40 requested governed Virginia
county identities were not returned. There were no unexpected returns or mapping
failures.

Applicability is not narrowed to provider availability. All 163 counties remain
governed; 123 are `AVAILABLE_DIRECT` and 40 are `PROVIDER_UNAVAILABLE`. Future
discovery must preserve and re-evaluate that classification.

## 6. Complete 163-county reconciliation

Appendix A contains every governed county exactly once. All returned counties
have 24 observations from 2001 through 2024. All 40 unavailable identities are
Virginia counties/county-equivalents, have zero provider observations, and were
also absent from legacy serving. No legacy-absent governed county was newly
returned; no legacy-present county disappeared. The live result reproduces the
legacy 129-geography footprint, so the 40-county gap is current direct-provider
unavailability—not an evidenced legacy-ingestion defect.

No unavailable county may be synthesized, combined with neighbors, substituted
with a combination area, or silently removed from canonical governance.

## 7. County-aggregate discontinuation evidence

Table-scoped metadata did not establish current CAGDP9 publication for MSA,
micropolitan, CSA, Metropolitan Division, or metropolitan/nonmetropolitan
portions. Generic Regional selectors remain ambiguous rather than affirmative
publication evidence. This is consistent with the first-party discontinuation
documentation recorded in BEA-A. All aggregate classes remain excluded. No BEA
aggregation tool or county summation is permitted.

## 8. Full-history acquisition and batching

### Quarterly

`Year=ALL` returned 2005Q1--2026Q1, 85 continuous observations for each of six
identities, with no changing membership. Combined and separate-geography bounded
samples matched. The initial explicit probe failed only because it sent
`2005Q1,2026Q1` as `Year`; Regional expects calendar-year values. The corrected
probe sends `2005,2026`, then filters the returned full-year quarters to 2005Q1
and 2026Q1 for a like-for-like four-key comparison. Quarterly one-request
acquisition remains pending that corrected live result.

### Annual

`Year=ALL` returned 2001--2024, 24 continuous observations for all 129 returned
identities, with no changing membership. Combined-versus-small-geography and
`ALL`-versus-explicit-boundary-year comparisons were equivalent. One request for
the complete annual physical source is supported; deterministic year or
geography batching is not required by current bounded evidence.

Neither response exhibited pagination or silent truncation in the tested shapes.

## 9. Temporal and unit semantics

Each SQGDP9 quarter is a seasonally adjusted annual-rate level in millions of
chained 2017 dollars. Calendar quarter-end dates are correct. Each CAGDP9 annual
row represents its calendar year's real GDP level in millions of chained 2017
dollars; Dec. 31 is the deterministic canonical period date. No rescaling occurs.

## 10. Sentinel/missing-value behavior

SQGDP9 contained 510 numeric values; CAGDP9 contained 3,096 numeric values. No
blank or nonnumeric token was observed in either governed response. Comma
formatting is accepted as evidenced numeric formatting. No suppression or
unavailable `DataValue` token was observed, so none is assigned an invented
meaning. Provider-unavailable counties are represented by absent rows, not a
sentinel row. A future parser must fail closed on unknown nonnumeric tokens and
retain any future annotations.

## 11. Repeat-request hash diagnostics

| source | normalized governed content | key inventory | raw bytes |
|---|---|---|---|
| `bea_gdp_qtr` | stable | stable | stable |
| `bea_gdp_ann` | stable | stable | **not stable** |

The annual raw-envelope mismatch did not change normalized content or canonical
keys. Raw bytes can contain formatting or mutable non-semantic metadata and are
immutable lineage evidence only. A raw-only difference must not create a semantic
candidate revision.

## 12. Revision and legacy parity

| source | exact | revisions | legacy-only | provider-only | identity conflicts |
|---|---:|---:|---:|---:|---:|
| `bea_gdp_qtr` | 510 | 0 | 0 | 0 | 0 |
| `bea_gdp_ann` | 3,096 | 0 | 0 | 0 | 0 |

There is no revised period, absolute/relative difference, provider extension, or
coverage loss in this acquisition. Latest periods equal the legacy maxima:
2026Q1 (2026-03-31) quarterly and 2024 (2024-12-31) annual. These are observed
provider state, not dates to hardcode into production policy.

## 13. Provider release and pinning conclusion

BEA metadata and result notes provide useful lineage/update context but no
sufficient immutable release identity. Content addressing is therefore required.

**Semantic identity:** adapter/schema version; physical source; sanitized
endpoint and deterministic request/batch plan; Regional/table/line/frequency;
exact governed applicability membership; exact returned membership; the 40
explicit provider-unavailable classifications; sanitized provider/table/line/unit
metadata identity; normalized governed-content hash; canonical key-inventory
hash; and parser/sentinel contract version.

**Immutable non-semantic lineage:** retrieval timestamp, transport diagnostics,
per-request raw-response hashes, and mutable operational metadata. Credentials
participate in neither category.

## 14. Same-period revision identity proof

The deterministic local mutation proof changes one normalized value without
changing table, period, or request identity and produces a different governed
content hash. Therefore the frozen rule is: same table/period plus changed
normalized governed content creates a new immutable source candidate revision;
it never overwrites an existing candidate. Raw-only change with identical
normalized content and keys does not create a semantic revision.

## 15. Physical contracts

### `bea_gdp_qtr` — pending acquisition-shape freeze

Regional / SQGDP9 / line 1;
`bea_qgdp_real_total_chained2017_saar`; millions chained-2017 dollars SAAR;
quarterly quarter-end; directly returned nation plus CA, DC, MD, NJ, VA states;
`Year=ALL`; strict numeric/comma parser and fail-closed unknown tokens; no
synthesis; credential-safe POST; expected canonical fields `geo_id`, `metric_id`,
`date`, `value`, `property_type_id=all`, `source_id`, with pin lineage. All
contract elements are confirmed except the corrected explicit-year bounded
request-shape check. Status: **NOT FROZEN**.

### `bea_gdp_ann` — frozen

Regional / CAGDP9 / line 1; `bea_agdp_real_total_chained2017`; millions
chained-2017 dollars; annual Dec. 31; applicability nation + five states + all
163 governed counties; direct availability nation + five states + 123 counties;
40 explicit `PROVIDER_UNAVAILABLE` identities; one `Year=ALL` request; strict
numeric/comma parser and fail-closed unknown tokens; no synthesis/substitution;
credential-safe POST; same canonical schema and content-addressed pin contract.
Status: **FROZEN**.

## 16. Common-lifecycle fit

BEA fits `discover -> persist immutable pin -> execute from pin -> canonicalize
-> immutable candidate publication -> durable result`. Provider-specific work is
limited to credential-safe metadata/data calls, exact membership and availability
classification, strict parsing, period-end conversion, content hashing, and
full-history revision comparison. No common-architecture gap or logical family
resolver is required.

## 17. Remaining risk/blocker

The only blocker is a credentialed rerun of the corrected quarterly bounded
explicit-year comparison. It must report four reference keys, four candidate
keys, no missing/extra/changed keys, `equivalent=true`, and the overall
`ONE_REQUEST_PER_PHYSICAL_SOURCE_SUPPORTED_BY_BOUNDED_SAMPLES` conclusion. Until
then `bea_gdp_qtr` remains unfrozen, PR #241 must not merge, and BEA-C must not
start.

## 18. BEA-C scope

BEA-C is authorized next only after the corrected quarterly gate passes and this
report is updated to freeze `bea_gdp_qtr`. BEA-C may then implement the two
separate verified physical adapters and lifecycle contracts. BEA-B created no
production implementation or state.

## Appendix A — all governed counties

| canonical geo_id | label | provider GeoFips | returned | live observations | first | last | legacy present | legacy observations | classification |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| `alameda_county_ca__county` | Alameda County, CA | `06001` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `albemarle_county_va__county` | Albemarle County, VA | `51003` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `alexandria_va__county` | Alexandria, VA | `51510` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `allegany_county_md__county` | Allegany County, MD | `24001` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `amelia_county_va__county` | Amelia County, VA | `51007` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `amherst_county_va__county` | Amherst County, VA | `51009` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `anne_arundel_county_md__county` | Anne Arundel County, MD | `24003` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `appomattox_county_va__county` | Appomattox County, VA | `51011` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `arlington_county_va__county` | Arlington County, VA | `51013` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `atlantic_county_nj__county` | Atlantic County, NJ | `34001` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `augusta_county_va__county` | Augusta County, VA | `51015` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `baltimore_city_county_md__county` | Baltimore City County, MD | `24510` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `baltimore_county_md__county` | Baltimore County, MD | `24005` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `bedford_county_va__county` | Bedford County, VA | `51019` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `botetourt_county_va__county` | Botetourt County, VA | `51023` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `burlington_county_nj__county` | Burlington County, NJ | `34005` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `butte_county_ca__county` | Butte County, CA | `06007` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `calvert_county_md__county` | Calvert County, MD | `24009` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `camden_county_nj__county` | Camden County, NJ | `34007` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `campbell_county_va__county` | Campbell County, VA | `51031` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `cape_may_county_nj__county` | Cape May County, NJ | `34009` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `carroll_county_md__county` | Carroll County, MD | `24013` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `charles_city_county_va__county` | Charles City County, VA | `51036` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `charles_county_md__county` | Charles County, MD | `24017` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `charlottesville_va__county` | Charlottesville, VA | `51540` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `chesapeake_va__county` | Chesapeake, VA | `51550` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `chesterfield_county_va__county` | Chesterfield County, VA | `51041` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `clarke_county_va__county` | Clarke County, VA | `51043` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `colonial_heights_va__county` | Colonial Heights, VA | `51570` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `contra_costa_county_ca__county` | Contra Costa County, CA | `06013` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `craig_county_va__county` | Craig County, VA | `51045` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `culpeper_county_va__county` | Culpeper County, VA | `51047` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `cumberland_county_nj__county` | Cumberland County, NJ | `34011` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `danville_va__county` | Danville, VA | `51590` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `del_norte_county_ca__county` | Del Norte County, CA | `06015` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `dinwiddie_county_va__county` | Dinwiddie County, VA | `51053` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `district_of_columbia_dc__county` | District of Columbia, DC | `11001` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `dorchester_county_md__county` | Dorchester County, MD | `24019` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `el_dorado_county_ca__county` | El Dorado County, CA | `06017` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `essex_county_nj__county` | Essex County, NJ | `34013` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `fairfax_city_county_va__county` | Fairfax City County, VA | `51600` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `fairfax_county_va__county` | Fairfax County, VA | `51059` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `falls_church_va__county` | Falls Church, VA | `51610` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `fauquier_county_va__county` | Fauquier County, VA | `51061` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `fluvanna_county_va__county` | Fluvanna County, VA | `51065` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `franklin_city_county_va__county` | Franklin City County, VA | `51620` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `franklin_county_va__county` | Franklin County, VA | `51067` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `frederick_county_md__county` | Frederick County, MD | `24021` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `frederick_county_va__county` | Frederick County, VA | `51069` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `fredericksburg_va__county` | Fredericksburg, VA | `51630` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `fresno_county_ca__county` | Fresno County, CA | `06019` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `giles_county_va__county` | Giles County, VA | `51071` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `gloucester_county_nj__county` | Gloucester County, NJ | `34015` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `gloucester_county_va__county` | Gloucester County, VA | `51073` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `goochland_county_va__county` | Goochland County, VA | `51075` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `greene_county_va__county` | Greene County, VA | `51079` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `hampton_va__county` | Hampton, VA | `51650` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `hanover_county_va__county` | Hanover County, VA | `51085` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `harford_county_md__county` | Harford County, MD | `24025` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `harrisonburg_va__county` | Harrisonburg, VA | `51660` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `henrico_county_va__county` | Henrico County, VA | `51087` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `henry_county_va__county` | Henry County, VA | `51089` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `hopewell_va__county` | Hopewell, VA | `51670` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `howard_county_md__county` | Howard County, MD | `24027` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `humboldt_county_ca__county` | Humboldt County, CA | `06023` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `hunterdon_county_nj__county` | Hunterdon County, NJ | `34019` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `imperial_county_ca__county` | Imperial County, CA | `06025` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `isle_of_wight_county_va__county` | Isle of Wight County, VA | `51093` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `james_city_county_va__county` | James City County, VA | `51095` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `kern_county_ca__county` | Kern County, CA | `06029` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `king_and_queen_county_va__county` | King & Queen County, VA | `51097` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `king_william_county_va__county` | King William County, VA | `51101` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `kings_county_ca__county` | Kings County, CA | `06031` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `lake_county_ca__county` | Lake County, CA | `06033` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `lassen_county_ca__county` | Lassen County, CA | `06035` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `los_angeles_county_ca__county` | Los Angeles County, CA | `06037` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `loudoun_county_va__county` | Loudoun County, VA | `51107` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `lynchburg_va__county` | Lynchburg, VA | `51680` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `madera_county_ca__county` | Madera County, CA | `06039` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `madison_county_va__county` | Madison County, VA | `51113` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `manassas_park_va__county` | Manassas Park, VA | `51685` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `manassas_va__county` | Manassas, VA | `51683` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `marin_county_ca__county` | Marin County, CA | `06041` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `martinsville_va__county` | Martinsville, VA | `51690` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `mathews_county_va__county` | Mathews County, VA | `51115` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `mendocino_county_ca__county` | Mendocino County, CA | `06045` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `merced_county_ca__county` | Merced County, CA | `06047` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `mercer_county_nj__county` | Mercer County, NJ | `34021` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `middlesex_county_nj__county` | Middlesex County, NJ | `34023` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `monmouth_county_nj__county` | Monmouth County, NJ | `34025` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `monterey_county_ca__county` | Monterey County, CA | `06053` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `montgomery_county_md__county` | Montgomery County, MD | `24031` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `montgomery_county_va__county` | Montgomery County, VA | `51121` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `morris_county_nj__county` | Morris County, NJ | `34027` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `napa_county_ca__county` | Napa County, CA | `06055` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `nelson_county_va__county` | Nelson County, VA | `51125` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `nevada_county_ca__county` | Nevada County, CA | `06057` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `new_kent_county_va__county` | New Kent County, VA | `51127` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `newport_news_va__county` | Newport News, VA | `51700` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `norfolk_va__county` | Norfolk, VA | `51710` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `norton_va__county` | Norton, VA | `51720` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `ocean_county_nj__county` | Ocean County, NJ | `34029` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `orange_county_ca__county` | Orange County, CA | `06059` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `petersburg_va__county` | Petersburg, VA | `51730` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `pittsylvania_county_va__county` | Pittsylvania County, VA | `51143` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `placer_county_ca__county` | Placer County, CA | `06061` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `poquoson_va__county` | Poquoson, VA | `51735` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `portsmouth_va__county` | Portsmouth, VA | `51740` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `powhatan_county_va__county` | Powhatan County, VA | `51145` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `prince_george_county_va__county` | Prince George County, VA | `51149` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `prince_george_s_county_md__county` | Prince George's County, MD | `24033` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `prince_william_county_va__county` | Prince William County, VA | `51153` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `pulaski_county_va__county` | Pulaski County, VA | `51155` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `queen_anne_s_county_md__county` | Queen Anne's County, MD | `24035` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `radford_va__county` | Radford, VA | `51750` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `rappahannock_county_va__county` | Rappahannock County, VA | `51157` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `richmond_city_county_va__county` | Richmond City County, VA | `51760` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `riverside_county_ca__county` | Riverside County, CA | `06065` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `roanoke_city_county_va__county` | Roanoke City County, VA | `51770` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `roanoke_county_va__county` | Roanoke County, VA | `51161` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `rockingham_county_va__county` | Rockingham County, VA | `51165` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `sacramento_county_ca__county` | Sacramento County, CA | `06067` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `salem_va__county` | Salem, VA | `51775` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `san_benito_county_ca__county` | San Benito County, CA | `06069` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `san_bernardino_county_ca__county` | San Bernardino County, CA | `06071` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `san_diego_county_ca__county` | San Diego County, CA | `06073` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `san_francisco_county_ca__county` | San Francisco County, CA | `06075` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `san_joaquin_county_ca__county` | San Joaquin County, CA | `06077` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `san_luis_obispo_county_ca__county` | San Luis Obispo County, CA | `06079` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `san_mateo_county_ca__county` | San Mateo County, CA | `06081` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `santa_barbara_county_ca__county` | Santa Barbara County, CA | `06083` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `santa_clara_county_ca__county` | Santa Clara County, CA | `06085` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `santa_cruz_county_ca__county` | Santa Cruz County, CA | `06087` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `shasta_county_ca__county` | Shasta County, CA | `06089` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `solano_county_ca__county` | Solano County, CA | `06095` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `somerset_county_nj__county` | Somerset County, NJ | `34035` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `sonoma_county_ca__county` | Sonoma County, CA | `06097` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `southampton_county_va__county` | Southampton County, VA | `51175` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `spotsylvania_county_va__county` | Spotsylvania County, VA | `51177` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `st_mary_s_county_md__county` | St. Mary's County, MD | `24037` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `stafford_county_va__county` | Stafford County, VA | `51179` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `stanislaus_county_ca__county` | Stanislaus County, CA | `06099` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `staunton_va__county` | Staunton, VA | `51790` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `suffolk_va__county` | Suffolk, VA | `51800` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `sussex_county_nj__county` | Sussex County, NJ | `34037` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `sussex_county_va__county` | Sussex County, VA | `51183` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `sutter_county_ca__county` | Sutter County, CA | `06101` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `talbot_county_md__county` | Talbot County, MD | `24041` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `tehama_county_ca__county` | Tehama County, CA | `06103` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `tulare_county_ca__county` | Tulare County, CA | `06107` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `tuolumne_county_ca__county` | Tuolumne County, CA | `06109` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `union_county_nj__county` | Union County, NJ | `34039` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `ventura_county_ca__county` | Ventura County, CA | `06111` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `virginia_beach_va__county` | Virginia Beach, VA | `51810` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `warren_county_va__county` | Warren County, VA | `51187` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `washington_county_md__county` | Washington County, MD | `24043` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `waynesboro_va__county` | Waynesboro, VA | `51820` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `williamsburg_va__county` | Williamsburg, VA | `51830` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `winchester_va__county` | Winchester, VA | `51840` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `wise_county_va__county` | Wise County, VA | `51195` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `yolo_county_ca__county` | Yolo County, CA | `06113` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
| `york_county_va__county` | York County, VA | `51199` | false | 0 | — | — | false | 0 | `PROVIDER_UNAVAILABLE` |
| `yuba_county_ca__county` | Yuba County, CA | `06115` | true | 24 | 2001 | 2024 | true | 24 | `AVAILABLE_DIRECT` |
