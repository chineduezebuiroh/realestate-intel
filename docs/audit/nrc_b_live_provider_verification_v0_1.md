# NRC-B live-provider verification and physical-contract freeze v0.1

## 1. Status

**COMPLETE / LIVE-PROVEN.** The final local live verification completed successfully
against the real first-party Census NRC workbooks after all NRC-B corrections. Its status
was `LIVE_VERIFICATION_COMPLETE`, with zero errors, all 10 expected metric/geography
pairs observed, and zero missing pairs. PR #248 is the implementation and live-
verification closure point for NRC-B. Disposition B and the physical contract documented
below are final for NRC-B; NRC-C may begin from this frozen evidence.

The implementation remains verification-only. It publishes no production artifact or
pin and mutates no registry, pointer, cohort, Source Set, canonical market, serving state,
or downstream source identity.

## 2. Repository and legacy facts

The migration base before PR #248 is `d67922a9`. Repository geography governance in
`config/geo_manifest.csv`, the feature-loader applicability list, and the checked legacy
DB use `us_nation` and `us_region_{northeast,midwest,south,west}` as canonical identities.
The old NRC ingest instead emits `united_states__nation` and
`{northeast,midwest,south,west}_region__region`; these are legacy ingestion aliases, not
canonical authority.

The verifier independently opens `data/market_serving.duckdb` with
`read_only=True`. The previously reproduced inventory is 5,480 `census_nrc_fred` rows,
two metrics, four regions, `all` in both property fields, no null values, no duplicate
normalized keys, and no national rows. Regional starts run January 1959–January 2026
(805 each); regional completions run January 1979–January 2026 (565 each).

## 3. Retired EITS candidate

The original candidate request to `api.census.gov/data/timeseries/eits/resconst` returned
HTTP 200, `Content-Type: text/html`, and an HTML page titled **Missing Key** in local live
execution. It was not a usable JSON response. NRC-B retires that route rather than adding
an API-key dependency merely to preserve a provisional design. EITS category/region
codes formerly guessed by the verifier are no longer part of the contract.

## 4. Direct Census acquisition contract

The governed acquisition candidates are now exactly the two first-party files advertised
by the Census NRC Historical Time Series page
`https://www.census.gov/construction/nrc/data/series.html`:

| Metric input | Exact mutable provider URL | Local live SHA-256 (observed input, not a permanent expected hash) |
|---|---|---|
| starts | `https://www.census.gov/construction/nrc/xls/starts_cust.xlsx` | `02c1926ba520e5ab4f7eadf93a10293fe8c9b95f4343ee9285856321fc2b6da4` |
| completions | `https://www.census.gov/construction/nrc/xls/comps_cust.xlsx` | `62ce743e1714eacecbf8592d43a3128b101eb4aa366c419a3f59d54f9fb83986` |

Both returned HTTP 200 and genuine XLSX bytes. Each exposed exactly `Annual`,
`Not Seasonally Adjusted`, `Seasonally Adjusted`, and `Seasonal Factors`. The governed
parser opens only `Seasonally Adjusted`, requires the metric-specific title, the literal
semantic declarations “Seasonally adjusted annual rate” and “Thousands of units”, and a
validated two-row geography/measure header. It selects only `Total` under United States,
Northeast, Midwest, South, and West. It does not select unit-structure columns.

The first workbook parser iteration incorrectly expected separate `Year` and `Month`
columns. Its replacement then crossed the wrong abstraction boundary by interpreting
relationships, shared strings, merges, styles, date formats, serial dates, and workbook
epochs itself; this caused a second live-only failure on the real style index. These were
parser implementation defects, not Census semantic-contract drift. A local probe of both
live files established the stable contract: the first header row begins with `Month` and
contains the five geography labels, the immediately following row contains each
corresponding `Total`, and the provider month cells deserialize as dates.

The corrected boundary uses `openpyxl.load_workbook(BytesIO(payload), read_only=True,
data_only=True)` solely for generic XLSX deserialization. NRC-B still strictly owns all
provider semantics: exact sheets, title, unit, SAAR declaration, header identities,
selected Total columns, values, and applicability. Month cells must emerge from openpyxl
as actual `date`/`datetime` values on day one; strings, arbitrary numerics, missing
`Month`/`Total` headers, and non-first-day dates fail closed before canonical month-end
normalization.

After the validated two-row header, observations must form one contiguous monthly data
block. Blank rows may precede its first dated observation, but any other non-date Month
cell before the block fails closed. Once at least one observation has been parsed, the
first non-date Month cell terminates the table without coupling the parser to particular
footer wording. All remaining rows are still checked: any later date is fatal because it
would prove an interrupted/non-contiguous series. Missing metric cells on a valid dated
row remain provider-unavailable observations and do not terminate the block.

The URLs and filenames are mutable current-history surfaces, not immutable release IDs.
Exact response bytes are verification inputs and future pinned input members. HTTP
status, requested/final URL, content type, length, retrieval time, raw SHA-256, and a
bounded failure prefix are lineage. Raw bytes are persisted before workbook parsing so a
provider-contract failure remains reproducible.

## 5. Metric and numeric contract

| Provider concept | Canonical metric ID | Frequency | Adjustment | Physical unit |
|---|---|---|---|---|
| Total privately owned housing units started | `census_housing_starts_total_saar` | monthly | seasonally adjusted annual rate | thousands of housing units at SAAR |
| Total privately owned housing units completed | `census_housing_completions_total_saar` | monthly | seasonally adjusted annual rate | thousands of housing units at SAAR |

Physical source ID is frozen as `census_nrc`. Both property fields are `all`.
Values are parsed exactly with `Decimal`; commas are presentation separators. There is no
floating tolerance, the frozen `numeric_scale_factor` is 1, and there is **no ×1,000
numeric rescaling**. The frozen physical unit is
`thousands_of_housing_units_saar`. Explicit unavailable/suppression markers are absent
observations, never zeroes. The exact live-observed Census workbook token `(NA)` marks
pre-1979 regional completions as provider-unavailable observations: those cells are not
zero, are not filled, and are not synthesized.
Unknown nonnumeric tokens fail closed.

## 6. Geography and applicability freeze

Repository governance supports the following mapping:

| Provider code/label | Legacy ingest geo_id | Legacy DB geo_id | Feature-loader ID | Governed canonical geo_id | Compatibility disposition |
|---|---|---|---|---|---|
| `US` / United States | `united_states__nation` | absent | `us_nation` | `us_nation` | map provider label directly; do not preserve ingest alias |
| `NE` / Northeast | `northeast_region__region` | `us_region_northeast` | `us_region_northeast` | `us_region_northeast` | ingest alias maps to canonical ID |
| `MW` / Midwest | `midwest_region__region` | `us_region_midwest` | `us_region_midwest` | `us_region_midwest` | ingest alias maps to canonical ID |
| `S` / South | `south_region__region` | `us_region_south` | `us_region_south` | `us_region_south` | ingest alias maps to canonical ID |
| `W` / West | `west_region__region` | `us_region_west` | `us_region_west` | `us_region_west` | ingest alias maps to canonical ID |

Applicability is exactly two metrics by five provider-published geographies. Division,
state, metro, county, place, and other local values are absent. No lower geography is
allocated, synthesized, or derived.

## 7. Date, history, and availability contract

Provider year/month labels normalize to the final calendar day of the observation month.
Release and retrieval timestamps remain lineage, not observation dates.

The supplied local live workbook inspection established:

| Metric/geography | Observations | First | Latest | Latest value (Aug 2026) |
|---|---:|---|---|---:|
| starts / United States | 812 | 1959-01-31 | 2026-08-31 | 1275 |
| starts / Northeast | 812 | 1959-01-31 | 2026-08-31 | 96 |
| starts / Midwest | 812 | 1959-01-31 | 2026-08-31 | 198 |
| starts / South | 812 | 1959-01-31 | 2026-08-31 | 658 |
| starts / West | 812 | 1959-01-31 | 2026-08-31 | 323 |
| completions / United States | 704 | 1968-01-31 | 2026-08-31 | 1128 |
| completions / Northeast | 572 | 1979-01-31 | 2026-08-31 | 90 |
| completions / Midwest | 572 | 1979-01-31 | 2026-08-31 | 153 |
| completions / South | 572 | 1979-01-31 | 2026-08-31 | 622 |
| completions / West | 572 | 1979-01-31 | 2026-08-31 | 263 |

These counts are evidence, not hard-coded parser assumptions. Every run recomputes each
series' count, first/latest month, internal gaps, continuity, and provider-unavailable
cell count. All ten observed series were continuous within their legitimate observed
bounds. Regional completions before 1979 are provider-native unavailable and are not
synthesized.

## 8. Revision semantics

The workbooks are mutable revisionary current truth. No reliable maximum revision window
has been proven. Normal discovery must therefore support acquisition and normalized
comparison of full governed history. Provider revision behavior remains distinct from the
future immutable-candidate policy: a changed normalized-content hash at the same maximum
period must be capable of producing a new immutable candidate revision.

## 9. Census-to-FRED parity

The final revised-verifier local run compared **7,052** keys. All 7,052 were exact
matches: value differences 0, Census-only 0, FRED-only 0, and differing/revised 0.
The verifier independently downloaded all ten established FRED series, normalized both
sources to canonical IDs and calendar month-end, and performed an exact-Decimal outer
join. Thus the direct first-party Census workbook route is empirically equivalent to the
legacy FRED transport for every currently comparable governed observation. FRED remains
lineage, parity, and rollback evidence, not the future governed acquisition route.

## 10. Census-to-legacy parity

The final run opened the legacy DB read-only and confirmed 5,480 rows across four regional
geographies, zero national rows, zero duplicate keys, and zero null values. The join
reported 5,064 exact matches, 416 provider revisions, 1,572 provider-only keys, and zero
legacy-only keys. Provider-only rows were classified as:

* `NATIONAL_ABSENT_FROM_LEGACY` for the two national metric histories missing from the snapshot;
* `AFTER_LEGACY_SNAPSHOT` for regional observations newer than its maximum month; or
* `OTHER_PROVIDER_ONLY` for a structural/history difference requiring review.

The decomposition was exact: 1,516 `NATIONAL_ABSENT_FROM_LEGACY` rows (704 national
completions plus 812 national starts), 56 `AFTER_LEGACY_SNAPSHOT` rows (seven newer months
across eight regional series), and zero unexplained `OTHER_PROVIDER_ONLY` rows. The 416
historical differences are `PROVIDER_REVISION`: current Census and current FRED agree
exactly, so the local database contains older provider vintages rather than a different
current metric, unit, date, or geography contract.

## 11. Missing national legacy investigation

Current ingest declares national FRED series and current transform code does not filter
them. Yet it emits the noncanonical legacy-ingest geography spelling while the database
contains canonical region spellings and no nation. No checked raw CSV or database history
proves the exact intervening stage. The bounded diagnosis remains **historical
implementation difference or stale legacy snapshot, with demonstrated identity mismatch;
exact exclusion stage unknown**. The absent national histories are a **legacy coverage
gap**, not the governed contract: NRC-C must not reproduce that gap. NRC-B records but
does not repair the old database.

## 12. Future input and semantic identity

NRC-C should eventually pin the exact two downloaded XLSX byte streams and make
resume/replay consume those bytes without rediscovery. Retrieval lineage includes exact
URL, retrieval UTC, response metadata, byte length, and raw SHA-256 for each member.
Semantic candidate identity should contain parser/schema version, `census_nrc`, both file
identities, governed metrics/geographies, maximum periods, normalized governed-content
SHA-256, canonical key-inventory SHA-256, sentinel contract, and stable unit/frequency/
seasonality metadata. Retrieval time must not alter semantic identity; changed historical
content must.

This is a design only. NRC-B implements no durable pin or candidate lifecycle.

## 13. Physical source disposition

**Disposition B is final and frozen for NRC-B.** The single governed physical source is
`census_nrc`. Authoritative future acquisition is directly from the two first-party
Census NRC historical workbooks identified in section 4. `census_nrc_fred` / FRED is the
legacy transport and is retained for lineage, parity, and rollback evidence, but is not
the future governed production acquisition route. There is no logical NRC family
resolver.

The source is revisionary current truth, with no assumed or invented bounded revision
window. Future input pin/release machinery must make exact workbook bytes pinnable and
content-addressed using provider URL plus byte/hash identity and appropriate retrieval
lineage. Semantic candidate identity must derive from normalized governed content, key
inventory, and parser-contract identity rather than volatile transport metadata. These
are frozen contract/design conclusions, not implementation of NRC-C lifecycle machinery.

## 14. NRC-C entry gate and negative contracts

The NRC-B gate is **SATISFIED** by `LIVE_VERIFICATION_COMPLETE`; no NRC-B evidence issue
prevents NRC-C from beginning. This closeout itself does not begin NRC-C: it does not
publish production inputs/candidates, implement release pins, register a monthly source,
change an accepted pointer, integrate a cohort, publish/promote a Source Set or
canonical/serving market, consume Redfin readiness, retire FRED, or change downstream
source/metric IDs.

## 15. Local execution and closure evidence

The final local execution from the isolated PR #248 worktree used the existing repository
DB read-only:

```bash
PYTHONPATH=. python -u scripts/nrc_b_verify.py \
  --legacy-db ../realestate-intel/data/market_serving.duckdb \
  --workspace artifacts/nrc_verification/live-xlsx

PYTHONPATH=. pytest -q tests/test_nrc_b_verify.py
python -m py_compile scripts/nrc_b_verify.py
```

No generated artifacts are part of this closeout. Review of
`artifacts/nrc_verification/live-xlsx/nrc_b_verification.json` confirmed:

1. both Census inputs have HTTP 200, XLSX content, exact URLs, byte lengths, and raw hashes;
2. both workbook contracts validate the exact sheet set, metric identity, SAAR declaration,
   thousands unit, and five `Total` headers;
3. ten series have expected first/latest/count/continuity and explain unavailable cells;
4. normalized-content and key-inventory hashes are present;
5. Census-to-FRED parity has complete per-series classifications;
6. Census-to-legacy parity separates national absence, post-snapshot freshness, revisions,
   other provider-only keys, and unexpected legacy-only keys; and
7. `status` is `LIVE_VERIFICATION_COMPLETE`, with zero errors, 10 expected and 10
   observed applicability pairs, and zero missing pairs.

Together with the exact Census-to-FRED and provider-to-legacy results recorded in
sections 9 and 10, these checks close NRC-B as **COMPLETE / LIVE-PROVEN**. PR #248 is the
implementation/live-verification closure point. Candidate publication, accepted-pointer
mutation, cohort integration, and release lifecycle implementation remain NRC-C or later
work.
