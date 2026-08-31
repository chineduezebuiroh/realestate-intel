# Proposed governed BLS LAUS source contract v1

**Status:** proposed contract frozen for review on 2026-08-30. This is a design
boundary, not implementation, bootstrap acceptance, publication, pointer movement,
database mutation, workflow integration, or schedule authorization.

## 1. Frozen scope and authority

The request authority for v1 is the exact, tracked membership in
`config/audit/laus_series_inventory_v0_1.csv`. It is a normalized audit projection
of `config/laus_series.generated.csv` plus the geography levels in
`config/geo_manifest.generated.csv`; LAUS-B must introduce an implementation-owned
registry rather than reading the audit file as production configuration.

| Physical metric | Class | Provider concept | adjustment | unit | transform | membership |
|---|---|---|---|---|---|---:|
| `laus_labor_force_nsa` | `GOVERNED_REQUIRED` | labor force, measure 06 | NSA | persons | none | 205 geographies |
| `laus_employment_nsa` | `GOVERNED_REQUIRED` | employment, measure 05 | NSA | persons | none | 205 geographies |
| `laus_unemployment_rate_nsa` | `GOVERNED_REQUIRED` | unemployment rate, measure 03 | NSA | percent | none | 205 geographies |
| `laus_unemployment_nsa` | `GOVERNED_DIAGNOSTIC` | unemployment level, measure 04 | NSA | persons | none | 205 geographies |
| all four `_sa` physical metrics | `LEGACY_ONLY` | same concepts | SA | persons/percent | none | 5 states |

BLS publishes all four measures as part of the LAUS statistical system. The
`GOVERNED_REQUIRED` versus `GOVERNED_DIAGNOSTIC` distinction is solely a
repository/product-governance decision based on downstream production usage; it
is not a statement that unemployment level is less statistically fundamental to
BLS. The three required metrics are the settled numeric Labor/Demand inputs, while
unemployment level has zero production weight. SA state rows remain useful legacy
evidence but are excluded because the current canonical source registry resolves
every LAUS logical metric to NSA and county/metro LAUS has no SA membership. This
avoids two physical truths for the same logical metric.

Provider values are retained without scaling. Levels are persons, not thousands;
rates are percentages (for example, provider `4.2` remains `4.2`, not `0.042`).
Publication must fail if registry unit or transform metadata is absent.

## 2. Geography and series identity

Governed membership is registry-driven, not Cartesian and not provider-discovered:
5 states, 37 CBSA metros, and 163 county-level identities (205 total). Each of the
four NSA concepts has exactly one configured series for each identity. Washington,
DC is governed as `district_of_columbia__state` and separately as
`district_of_columbia_dc__county`; they have distinct canonical identities even
where provider estimates may overlap. There are no governed cities, places,
metropolitan divisions, nation, ZIPs, or neighborhoods in v1.

Each row binds one `series_id` to one `geo_id`, physical `metric_id`, adjustment,
unit, transform, class, and target-control flag. Duplicate series IDs, duplicate
canonical bindings, unknown returned series, contradictory ID prefix/seasonality,
or crosswalk drift are fatal. Provider-area codes are configuration identity; a
new/discontinued/redesigned area requires reviewed registry change and cannot be
automatically substituted.

Unlike CES (5 states plus 44 metros and sparse optional metrics), LAUS includes
counties and has complete four-concept NSA membership. Completeness therefore
uses the exact LAUS registry, even though it happens to be rectangular today.

## 3. Deterministic acquisition

The provider edge is BLS Public Data API v2 JSON POST at
`https://api.bls.gov/publicAPI/v2/timeseries/data/`. A canonical request plan has:

* sorted governed series metadata and explicit inclusive `start_year`/`end_year`;
* ordered batches of at most 50 series and non-overlapping windows of at most 20
  inclusive years, matching the proven repository transport limits;
* `annualaverage=false`, endpoint/version, adapter-contract version, units and
  transforms, and hashes of the series registry, geo crosswalk, source metric
  registry, and revision policy;
* an optional `registrationkey` only on the wire, never in identity or evidence.

The frozen registered BLS API v2 limits are 500 queries per day, 50 series per
query, 20 years per query, and 50 requests per 10 seconds. These limits were
supplied as external reviewer evidence after the task network again could not
fetch the [official BLS API v2 documentation](https://www.bls.gov/developers/api_signature_v2.htm);
they were not locally verified. They close the v1 design gate while preserving the
official provenance URL for LAUS-B capture. `BLS_API_KEY` remains required: the
registered limits are necessary for deterministic full-history execution and the
key remains wire-only.

For exactly 840 series, `ceil(840 / 50) = 17` series batches. Therefore:

| acquisition | year windows | deterministic API requests |
|---|---:|---:|
| ordinary `end_year-2..end_year` | 1 | 17 |
| deep `1976..end_year` | `ceil((end_year-1976+1)/20)` | `17 * ceil((end_year-1976+1)/20)` |
| bootstrap `1976..end_year` | `ceil((end_year-1976+1)/20)` | `17 * ceil((end_year-1976+1)/20)` |

For an explicit `end_year=2026`, deep/bootstrap cover 51 inclusive years, use
three year windows, and make 51 requests. Counts depend only on registry size and
explicit bounds, not timing. Each mode is below 500 requests for the current
registry/range; the runner must still respect the 50-per-10-seconds rate limit.

HTTP failure, non-`REQUEST_SUCCEEDED`, absent `Results.series`,
missing requested blocks, unexpected/duplicate blocks, invalid observations, or
exhausted bounded retries aborts the complete acquisition. Partial facts are
never returned. Retry only timeout/connection/HTTP 408, 429, and 5xx, with three
total attempts and bounded backoff; deterministic 4xx/schema/content errors are
terminal. No flat-file fallback, stale cache, SA-to-NSA fallback, or guessed series
is permitted.

The normalized response uses `Results.series[].seriesID/data[]` and each datum's
`year`, `period`, and `value`. `M01` through `M12` are monthly. `M13` is annual
average and is always discarded; it must never become December. Missing/null,
non-numeric, or non-finite values are invalid, not zero, with one narrow provider
state: marker `"-"` accompanied by BLS footnote code `X` is an explicit
`provider_unavailable` observation. A bare `"-"`, `NA`, `N/A`, blank, arbitrary
text, or non-finite/malformed number remains fatal. Stable code, rather than
free-form prose or a hard-coded date, controls recognition.

LAUS-B must retain a fixture for the actual response/error schema, but API limits
are no longer an open contract decision. If official documentation differs when
it becomes locally accessible, implementation must stop for contract review rather
than silently alter partitioning.

## 4. Target month and completeness

For each of the 615 required series, compute its maximum valid M01--M12 month.

```text
required_common_max = min(max_month(series) for every configured required series)
target_month = YYYY-MM(required_common_max)
```

The cohort is the repository's production-required LAUS input set. Its composition
is downstream governance, not a claim that diagnostic unemployment level is less
fundamental to BLS. At the target month, every required series must contain a row.
A required series omitted, empty, or lagging makes acquisition incomplete and
blocks publication; diagnostic unemployment level never controls advancement but
its lag is recorded. This prevents a partial or unusually advanced series from
moving the artifact ahead of the complete production-required cohort. An
unexpected later period is retained only inside explicit bounds and cannot select
target. Target regression is fatal. A discontinued required series blocks until
reviewed registry change; there is no implicit substitution.

An explicit provider-unavailable observation is not a numeric target row and
cannot satisfy target-month completeness. It may remain as an interior historical
hole when later valid observations establish a complete common frontier; values
are never filled, interpolated, or synthesized. A period omitted entirely remains
distinct and is surfaced as an interior-omission diagnostic under the existing
fail-closed completeness policy.

Diagnostics contain requested/returned/missing series, rows per metric/geography,
per-series minima/maxima, common maximum, target month, diagnostic lag, invalid,
duplicate and M13 counts, and request/config identities.

## 5. Canonical rows

Columns and order are exactly:

```text
geo_id, metric_id, date, property_type_id, value, source_id, property_type
```

`source_id="laus"`; both property fields are `"all"`; M01--M12 normalize to last
calendar day; `value` is a finite IEEE-754 double in the declared provider unit.
Canonical key is `(geo_id, metric_id, date, property_type_id)`. Duplicate keys,
conflicting duplicates, null keys/values, observations outside request bounds, or
unknown mappings fail closed. Output is stable-sorted by key (then source/property
compatibility columns); response and batch order cannot affect bytes.
Explicit provider-unavailable observations emit no canonical row, so the market
table remains strictly numeric.

## 6. Revision and reconciliation

LAUS is `revisionary_current_truth`. The revision evidence frozen for v1 is:
annual revisions normally may revise up to five prior years; state/model-based
revision incorporates updated population controls, revised inputs, and model
re-estimation; substate estimates are then revised with updated inputs and
controlled to revised state totals. Both NSA and SA modeled observations may be
revised, although v1 governs NSA only. Exceptional historical revisions can be
materially deeper: reviewer-supplied recent examples include substate revision
covering approximately 2016--2025 and redesign/reconstruction to modeled-series
beginnings such as 1976, 1990, or 1994.

This is externally supplied reviewer evidence because local retrieval was again
blocked, not a claim of locally fetched evidence. Official provenance to capture
with the implementation record is the [LAUS program home](https://www.bls.gov/lau/),
[LAUS methods](https://www.bls.gov/lau/laumthd.htm), and
[LAUS notices](https://www.bls.gov/lau/notices/). The normal and exceptional
revision semantics are no longer OPEN contract questions. Contradictory official
evidence discovered during LAUS-B must stop implementation for review.

Returned overlap from a completely successful request wins; new keys append;
prior-only governed keys are preserved; omission is not retraction. Ordinary
reconciliation can revise only keys in its explicit request window. Annual and
exceptional full-history reconciliation refresh older history. Prior-only
preservation protects against accidental deletion, but is not proof that a
preserved value remains current provider truth indefinitely; mandatory annual
deep reconciliation prevents out-of-window history from remaining permanently
stale.

### 6.1 Routine monthly reconciliation

```text
mode = ordinary_overlap
bounds = end_year - 2 through explicit end_year
cadence = every routine governed monthly refresh
```

Three inclusive calendar years capture preliminary/current and normal near-term
changes while bounding routine acquisition. `end_year` is explicit cycle/request
context, never wall-clock-derived.

### 6.2 Scheduled annual deep reconciliation

```text
mode = deep_reconciliation
bounds = 1976 through explicit reviewed end_year
cadence = at least once per calendar year after the annual LAUS
          revision/re-estimation cycle is available
```

Full history is required because normal revisions cover multiple years,
exceptional inputs can reach deeper, redesign can reconstruct series beginnings,
and the deterministic request is feasible under registered limits. It avoids
silently retaining stale observations outside an arbitrary revision maximum. The
exact production month is **OPEN** pending a verified BLS release calendar; no
month is invented here. This cadence uses existing source execution machinery and
does not authorize a new common scheduler.

### 6.3 Exceptional methodology/geography/series reconciliation

Triggers include methodology redesign, area-definition change, replacement or
discontinuation, unusually deep historical re-estimation, or registry identity
change. Provider discovery must not silently substitute identities. Governance is:

```text
detect or externally identify exceptional change
-> block ordinary silent substitution
-> reviewed registry/methodology update
-> explicit full-history reconciliation (1976..reviewed end_year)
-> equivalence and revision diagnostics
-> reviewed acceptance
```

The full-history acquisition implementation may be shared with annual deep mode,
but its trigger and reviewed acceptance meaning remain distinct.

### 6.4 Bootstrap bounds

Bootstrap uses `1976..explicit reviewed end_year`, the same deterministic
full-history partitioning, for the first provider reconstruction. A series whose
registered availability begins later than 1976 is diagnosed rather than invented;
target-month completeness remains mandatory.

## 7. Provider and request identities

Serialize canonical JSON with sorted keys and compact separators.

```text
source_request_identity = "bls-laus-v2:" + sha256(canonical_request_plan_bytes)
provider_release_id = "laus-ordinary-current:" + sha256(
    canonical_request_plan_bytes + b"\n" + canonical_provider_observations_bytes
)
```

Canonical provider observations are stable-sorted records containing series,
year, period, status, and (for `numeric`) canonical numeric value. An unavailable
record instead contains status `provider_unavailable`, provider marker `"-"`, and
stable sorted recognized footnote codes (currently `X`), but no numeric value or
free-form prose. These records cover M01--M12, before geo mapping but after strict
validation; numeric serialization must be one specified
locale-independent representation. Include exact requested membership/bounds,
not response order, footnote order, retrieval time, Actions run ID, publisher SHA,
secret, or wall clock. Identical provider truth under the same plan reproduces the
identity. Numeric-to-unavailable, unavailable-to-numeric, and omitted-to-explicitly
unavailable changes therefore change provider identity. A future proven immutable BLS release ID may be additional metadata but
must not replace content identity.

## 8. Bootstrap equivalence and activation

LAUS-B should perform full deterministic provider reconstruction into an isolated
candidate, then a read-only comparison to both checked-in databases as legacy
evidence. Classify every mapped key as exact match, provider revision, provider
newer, provider historical-only, legacy prior-only, legacy out of governed scope,
governed identity conflict, unexplained numeric mismatch, or unit-scale mismatch.

Acceptable with counts/examples and reviewed explanation: exact, provider revision
on identical series/key/unit, provider newer, provider historical-only, and legacy
prior-only preserved by policy. A legacy-only row whose `(geo_id, metric_id)` is
absent from the exact frozen registry is retained, counted, and audited as
`LEGACY_OUT_OF_GOVERNED_SCOPE`; it is migration evidence and does not block
governed bootstrap acceptance or expand governed membership. A contradiction
inside configured identity space--including a legacy row denying a configured
pair or claiming an unknown pair as configured--is a
`GOVERNED_IDENTITY_CONFLICT` and remains fatal. Duplicate or ambiguous mapping,
unit-scale mismatch, unexplained numeric mismatch, required membership failure,
target regression, invalid values, or non-reproducible output also remain hard
failures. Legacy coverage differences never weaken gates or override the registry.

After review: immutable LAUS-only publication, catalog registration and remote
verification, explicit compare-and-swap activation of only
`accepted.source.laus`, then fresh durable resolution proving the pointer. No
legacy database is an accepted governed prior before that activation.

## 9. Common-framework seam

Reuse unchanged: source-artifact construction/validation, deterministic tar,
GitHub Release publication, catalog registration/re-resolution/remote verification,
pointer/CAS invariants, `monthly_source_execution_result_v1`, durable cycle-result
recording and semantic identity, retry typing, normal/resume/replay planning,
successful-source reuse, and the common cohort barrier.

LAUS-specific code is limited to registry loading, deterministic BLS plan and
transport invocation, response transform, membership/completeness, common target,
revision bounds/reconciliation inputs, identity payload, and bootstrap equivalence.
The smallest seam is a pure adapter returning canonical rows, provider identity,
target month and diagnostics to the same artifact/result runner pattern used by
CES. No common orchestration redesign is authorized.
