# NRC-B live-provider verification and physical-contract freeze v0.1

## 1. Status

**IMPLEMENTED / AWAITING LOCAL LIVE PROOF.** The deterministic, read-only verifier is
implemented and its repository/legacy observations are proven. On 2026-09-18 this hosted
environment again rejected bounded HTTPS requests to `api.census.gov`, `www.census.gov`,
and `fred.stlouisfed.org` at its outbound proxy with HTTP tunnel 403. That is an
environment limitation, not evidence about either provider. No section below calls an
unretrieved provider response live-proven, and NRC-C must not begin yet.

NRC-A disposition B remains **provisional**. The final physical source disposition is not
frozen until one successful local run validates the direct route, all ten identities, and
both parity joins.

The first local run of PR #248 reached Census at the HTTP layer but failed with a bare
`JSONDecodeError` at byte zero for the candidate EITS query. That proves only that its
body was not valid JSON. The old verifier did not retain the status, content type, final
URL, byte count, hash, or prefix needed to distinguish an API error, HTML, an empty body,
or an intermediary response, so the exact upstream cause cannot honestly be reconstructed
from that run. The verifier now persists raw bytes before parsing and records those
bounded diagnostics; the next local run will establish the root cause without printing
an unbounded provider response.

## 2. NRC-A entry gates and locally reverified facts

The base was `d67922a9` on the platform-created `work` branch, matching the requested
authority. NRC-A, the legacy ingest/transform/refresh code, geography manifest, feature
loader, registries, serving assembly references, and the legacy database were reinspected.

The verifier independently opened `data/market_serving.duckdb` with DuckDB
`read_only=True` and found 5,480 `census_nrc_fred` rows, two metric IDs, four region IDs,
only `all` in both property fields, no null numeric values, no duplicate normalized keys,
and no nation rows. Each regional starts series has 805 observations from January 1959
through January 2026; each regional completions series has 565 observations from January
1979 through January 2026. The report normalizes those first-day legacy labels to month
end, without modifying the database.

## 3. Direct Census acquisition contract

The first-party route under test is the Census economic indicators time-series dataset:

```text
GET https://api.census.gov/data/timeseries/eits/resconst
    ?get=cell_value,data_type_code,time_slot_id,category_code,seasonally_adj,region_code
    &time=from+1959-01
```

The product/dataset identity under investigation is `timeseries/eits/resconst` (New Residential
Construction). The verifier expects a Census JSON array whose first row is a header and
requires `cell_value`, `time`, `category_code`, `seasonally_adj`, and `region_code`.
It selects only seasonally adjusted categories unambiguously identifying starts or
completions and rejects unknown region codes. Missing tokens are not observations;
unknown nonnumeric values fail closed.

The prior local failure means this request is **not a corrected or frozen acquisition
contract**. Hosted access cannot reach Census, so first-party live metadata could not be
captured here. Candidate exact selector mappings are `category_code=STARTS` and
`category_code=COMPLETIONS`, with candidate region codes `0`, `1`, `2`, `3`, and `4` for
United States, Northeast, Midwest, South, and West. The parser now compares only those
exact strings: substring matching and speculative aliases were removed. These codes
remain explicitly unproven, and any different live value fails closed.

**Not yet live-verified:** exact returned category/data-type codes, whether the query
requires narrower selectors, actual content type/schema, full-history coverage, latest
period, provider labels, response metadata, sentinel vocabulary, release markers,
whether all ten series arrive in one response, and whether the current endpoint's URL or
bytes are stable. The exact query is therefore a failed/unresolved verifier candidate, not yet a frozen
production acquisition contract. A local schema failure is useful evidence and must be
resolved by inspecting Census `variables.json`/first-party documentation, not by
loosening validation or guessing.

The historical-data page and downloadable `starts_cust`/`completions_cust` workbooks remain alternate first-party surfaces
to evaluate if the EITS response cannot provide the exact ten governed series. HTML
scraping is not an acceptable fallback. Their exact filenames, schema, and historical
stability were not live-proven and are not substituted on guesswork. No evidence yet establishes an immutable Census
vintage or old-release address.

## 4. Metric contract

| Concept | Existing metric ID | Candidate provider identity | Frequency/adjustment | Physical unit |
|---|---|---|---|---|
| Total privately owned housing units started | `census_housing_starts_total_saar` | NRC `resconst`, SA total starts category/cell (exact returned code pending) | monthly, seasonally adjusted annual rate | thousands of housing units at SAAR |
| Total privately owned housing units completed | `census_housing_completions_total_saar` | NRC `resconst`, SA total completions category/cell (exact returned code pending) | monthly, seasonally adjusted annual rate | thousands of housing units at SAAR |

The existing metric IDs remain appropriate and are not changed. Unit metadata must be
`thousands_of_housing_units_saar`. The numeric contract has scale factor **1**: a
provider value such as `1500` remains `1500`, not `1,500,000`. The verifier uses exact
decimal parsing and comparison; commas are presentation separators, documented missing
tokens are omitted, and any other nonnumeric or non-finite token is fatal. Census and
FRED first-party metadata confirmation of this unit remains a live-proof gate.

First/latest observations are response facts and are never hard-coded. The local report
will inventory them after acquisition.

## 5. Geography and applicability contract

The proposed applicability is exactly two metrics by five provider-published concepts:
United States, Northeast, Midwest, South, and West (ten series). The parser permits only
those identities and rejects any unexpected geography rather than allocating it.

The same NRC concepts are not to be synthesized for divisions, states, metros, counties,
places, or other local geographies. Whether the selected response also contains other
categories/geographies is immaterial: they are outside this contract. Live evidence must
still prove that the selected NRC product naturally publishes all ten intended cells.

## 6. Geography-ID reconciliation

The governed IDs come from `config/geo_manifest.csv` and agree with the feature-loader
contract and legacy region rows. They are not selected by majority spelling.

| Provider geography | Candidate provider code/label | Legacy ingest ID | Legacy DB ID | Feature-loader ID | Governed canonical ID | Disposition |
|---|---|---|---|---|---|---|
| United States | `0`/`00`/US (live code pending) | `united_states__nation` | absent | `us_nation` | `us_nation` | map provider identity; legacy absence is not authority |
| Northeast | `1`/NE (live code pending) | `northeast_region__region` | `us_region_northeast` | `us_region_northeast` | `us_region_northeast` | legacy ingest spelling is noncanonical |
| Midwest | `2`/MW (live code pending) | `midwest_region__region` | `us_region_midwest` | `us_region_midwest` | `us_region_midwest` | legacy ingest spelling is noncanonical |
| South | `3`/S (live code pending) | `south_region__region` | `us_region_south` | `us_region_south` | `us_region_south` | legacy ingest spelling is noncanonical |
| West | `4`/W (live code pending) | `west_region__region` | `us_region_west` | `us_region_west` | `us_region_west` | legacy ingest spelling is noncanonical |

Provider codes above are validation candidates only. Unknown returned codes fail closed;
they must be reconciled from first-party metadata before updating this table.

## 7. Date semantics

The governed deterministic rule is:

```text
provider observation/reference month -> final calendar day of that month
```

The parser accepts `YYYY-MM`, ISO date, or English month/year, then computes true calendar
month end, including leap years. FRED first-of-month labels and legacy DuckDB first-day
dates therefore compare against the same month-end key. Retrieval and publication/release
timestamps remain lineage and never replace observation date. Value differences are
reported independently; malformed dates fail rather than becoming provider-only rows.

## 8. Revision semantics

Repository evidence establishes that the legacy path obtains mutable ordinary-current
FRED histories, not immutable vintages. Hosted execution did not provide new first-party
Census evidence about preliminary/revised flags, routine prior-month revisions,
benchmark/seasonal revisions, historical in-place replacement, release markers, or old
vintage availability. No bounded maximum revision window is claimed.

Until first-party documentation proves a safe maximum, NRC-C discovery must fetch and
compare full governed history. This provider-publication behavior is distinct from the
future system policy: every changed normalized-content hash, including a same-maximum-
period revision, must be eligible for a new immutable candidate revision.

## 9. Census to FRED contemporaneous parity

**Pending local live proof.** The hosted proxy prevented both acquisitions, so there are
no fabricated counts. One verifier invocation downloads Census then all ten FRED CSVs
into the same verification workspace. It performs an exact-decimal outer join on
canonical metric, geography, month-end date, and property type, reporting `EXACT_MATCH`,
`CENSUS_ONLY`, `FRED_ONLY`, and `VALUE_DIFFERENCE`, plus totals, per-series counts,
maximum absolute difference, and first/last differing periods. Schema/date failures are
reported explicitly instead of being disguised as value parity. No tolerance is used.

## 10. Provider to legacy parity

**Pending local live proof.** Once Census succeeds, the same run compares its normalized
current truth to the read-only legacy rows and reports `EXACT_MATCH`, `PROVIDER_ONLY`,
`LEGACY_ONLY`, and `PROVIDER_REVISION`, overall and per series. Identity/schema/date
normalization failures stop parsing and appear in the report. Known inventory facts do
not substitute for live value parity.

## 11. Missing national legacy investigation

The present ingest map declares both national FRED series, and its transform does not
explicitly remove nation rows. However, that ingest emits `united_states__nation` and
`*_region__region`, while the checked database contains only `us_region_*`. Thus the
checked rows cannot be the unmodified output identity of the currently checked-in
ingest/transform pair. The canonical geography manifest and serving/feature code accept
`us_nation`, so no current explicit canonical rule explaining nation-only exclusion was
found. No raw NRC CSV or database history proved where the divergence occurred.

Most evidence-supported classification: **historical implementation difference or stale
legacy snapshot, with a geography-ID mismatch demonstrated; exact stage unknown**. It is
not supportable to choose acquisition omission, transform omission, or intentional
serving filtering. NRC-B does not repair the snapshot.

## 12. Final physical source disposition

Disposition B remains the preferred but **provisional** outcome: direct Census under
`census_nrc`, retaining `census_nrc_fred` only for legacy/parity/rollback lineage. Census
has the superior authority/provenance, and one physical source avoids unjustified
resolver complexity. Nevertheless, deterministic acquisition, exact schema and coverage,
revision visibility, contemporaneous parity, and operational stability are material
unproven gates. The contract is not frozen merely because direct Census sounds cleaner.

If live evidence shows the direct surface cannot deterministically represent the ten
series/history, record that evidence and reconsider A or C. Do not silently substitute
FRED or add a logical family.

## 13. Future input pin and release identity

No genuine immutable Census release identity is proven. Unless local evidence discovers
one, NRC-C should define a content-addressed semantic identity containing:

* adapter/report schema version and physical source ID;
* exact product/dataset/query/table/cell identities;
* governed metric set and geography applicability;
* maximum observation period;
* normalized governed-content SHA-256;
* canonical key-inventory SHA-256;
* parser and sentinel contract; and
* stable unit, frequency, seasonal-adjustment, and annualization metadata.

Keep `retrieved_at`, request URL, HTTP status/content type/ETag/Last-Modified, provider
release-page metadata, and each raw-response SHA-256 as retrieval lineage. They must not
make identical semantic content a different release. Conversely, a changed normalized
content hash at the same maximum period must create a distinct immutable revision later.

## 14. NRC-C implementation contract and entry gate

The verifier writes only raw verification inputs and
`<workspace>/nrc_b_verification.json`. It opens the legacy database read-only, computes
deterministic content/key hashes, inventories provider/legacy coverage, and emits exact
parity detail. Ordinary tests are synthetic and make no network calls.

NRC-C entry is **closed** until a reviewed local report proves the exact Census request
and codes, schema/sentinels, native units, ten-series availability, first/latest periods,
revision/discovery basis, both parity results, and final disposition. The bounded missing-
nation diagnosis above is sufficient only for that separate legacy question.

## 15. Explicit negative contracts

NRC-B publishes no production input or candidate, mutates no registry/pointer/database,
integrates no workflow/cohort, promotes no Source Set/canonical/serving market, consumes
no Redfin readiness, retires no legacy source, changes no metric ID, and creates no
derived lower geography. Generated verification artifacts must not be committed.

## 16. Local execution and remaining blockers

From repository root:

```bash
PYTHONPATH=. python -u scripts/nrc_b_verify.py \
  --legacy-db data/market_serving.duckdb \
  --workspace artifacts/nrc_verification/live

PYTHONPATH=. pytest -q tests/test_nrc_b_verify.py
python -m py_compile scripts/nrc_b_verify.py
```

Use a new or empty workspace for the corrective rerun. The live command writes only beneath the artifact directory and returns
nonzero with an `INCOMPLETE` report if any provider, schema, applicability, or legacy gate
fails. For deterministic re-parsing after a successful download, preserve the workspace
and add `--offline`. Remaining blocker: supply and review a successful local live report;
if the candidate Census query fails schema validation, establish the exact first-party
selectors from the live metadata and update the verifier/audit before freezing NRC-B.
