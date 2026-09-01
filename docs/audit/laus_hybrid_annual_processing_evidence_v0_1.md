# LAUS hybrid annual-processing evidence v0.1

**Decision:** no reviewed official machine-readable class-specific annual-completion
marker has been established. RSS reference-release discovery is supported, but
automatic `READY_FOR_ANNUAL_DEEP` remains fail-closed and LAUS-C2 is unsafe.

## 1. Reconnaissance scope and accessibility

Reconnaissance was performed on 2026-08-31. The separately supplied production-style
capture proves `https://www.bls.gov/feed/laus.rss` returned HTTP 200,
`application/rss+xml`, 8,056 bytes, and SHA-256
`64ff95bd1a6c60860dd4d49b06ca1bac10d19c1121d1901443a9fe2fef295631`.
The supplied retrieval receipt time is `2026-08-31T18:14:34.371823Z`; the exact
response body itself was not supplied inside this repository environment.
It identified feed `bls.gov:feed:laus`, title `State Employment and Unemployment
(Monthly)`, and the January 2026 entry `laus-2026_04_08__10_00_00`, published
2026-04-08, linking to the BLS LAUS archive.

This hosted environment then attempted ordinary HTTPS GETs to that RSS URL and
`https://download.bls.gov/pub/time.series/la/`. Both were rejected at its CONNECT
proxy with HTTP 403 before provider bytes arrived. The web facility returned 401.
No browser impersonation or bypass was attempted. Human-facing BLS HTML URLs are
likewise recorded as 403 in the supplied reconnaissance and are not a production
prerequisite.

`BLS_API_KEY` was not configured. Therefore no new API request was made and live
provider validation is incomplete. The intended representative 2025--2026 request
scope was the following governed sample (metrics 003--006: unemployment rate,
unemployment, employment, and labor force):

| class | geography | series prefix/example |
|---|---|---|
| model-based state | California, District of Columbia | `LAUST060000000000003`--`006`; `LAUST110000000000003`--`006` |
| non-modeled metro | Bakersfield, Chico | `LAUMT061254000000003`--`006`; `LAUMT061702000000003`--`006` |
| non-modeled county | Alameda, Butte | `LAUCN060010000000003`--`006`; `LAUCN060070000000003`--`006` |

The accepted acquisition path remains API v2
`https://api.bls.gov/publicAPI/v2/timeseries/data/`, registered-key POST,
50-series batches, and 20-year windows. No second broad client was introduced.

## 2. RSS parser and annual-event rule

The corrected narrow Atom parser validates the exact feed identity/title,
offset-aware feed and entry timestamps, unique entry IDs, the captured text-form
`<category>News Release</category>`, nonempty content, and exactly one HTTPS
`www.bls.gov/news.release/archives/laus_YYYYMMDD.htm` link (the captured link has
no `rel` attribute).
Malformed XML, identity drift, duplicate IDs, invalid timestamps, missing provider
facts, or off-host links are fatal; these failures are not `NOT_EXPECTED`.

The corrected reference-period rule requires the title to begin `January ` and
the content to begin `In January, `; disagreement is fatal. The headline suffix
is deliberately unconstrained. Entry ID `laus-YYYY_MM_DD__HH_MM_SS`, archive
filename `laus_MMDDYYYY.htm`, published date/time, and updated date must agree.
For a corroborated January entry, the annual reference year is the validated
provider publication year, not wall clock or title-invented year. These dates are
provenance only and never completion evidence.

RSS alone produces only a January reference-release fact. It cannot establish
annual-processing applicability. `annual_event_evidence` therefore requires a
separate governed BLS applicability record before it can normalize a C1 event;
without one it raises a distinct governance error. With such evidence, the C1
object still has `processing_classes=[]` and evaluates to `WATCHING`.

The repository does not contain the 8,056 official bytes, so Smoke 189 honestly
labels its minimized Atom as a **synthetic parser fixture mirroring the reviewed
structural facts**, including the actual headline, content, link form, and category.
It is not an official capture. `verify_official_capture_fixture` accepts future
bytes only at exactly 8,056 bytes and SHA-256
`64ff95bd1a6c60860dd4d49b06ca1bac10d19c1121d1901443a9fe2fef295631` before
parsing. Exact-byte official fixture validation remains pending.

## 3. API fields and footnote inventory

The accepted API decoder observes top-level `status`, `message`, `Results`, then
`Results.series[].seriesID` and `data[]` observation fields. Observations used by
the adapter contain `year`, `period`, `periodName`, `value`, and `footnotes[]`
code/text pairs. Requests set `annualaverage=false`; M01--M12 are canonical and
M13 is discarded. The accepted responses and prior C1 review did not establish
`latest`, catalog, calculation, revision version/vintage, annual-processing,
provisional, or finalization fields.

The complete retained provider footnote/status inventory relevant to the governed
sample is:

| code/status | normalized text | count/scope | numericity | annual meaning |
|---|---|---|---|---|
| `X` | provider data unavailable (prior accepted evidence) | LAUS-B records during 2025 appropriations lapse; exact new sample count unavailable without credentials | value `-`, unavailable | none |
| empty footnote set | no provider annotation | ordinary numeric observations across ST/MT/CN | numeric | none |
| `REQUEST_SUCCEEDED` | request succeeded | response-level transport/schema status | not an observation | none |

No other code/text can be claimed from a new live sample in this environment.
In particular no observed code means revised, preliminary, provisional, final,
benchmark/re-estimation, population-control revision, methodology revision, or
annual completion. The adapter's `-` plus `X` classification remains
`provider_unavailable`; it is not zero, NaN, interpolation, or completion.

## 4. Download surface findings

The official directory and candidate flat files (`la.series`, `la.footnote`, and
the existing acquisition's state/substate `la.data.*` families) were targeted.
The hosted proxy blocked the directory before a listing, headers, bytes, timestamps,
or redirects could be inspected. Existing repository reconnaissance establishes
that the flat files are current replacement data/metadata inputs, but it does not
establish an immutable revision version, annual vintage, or provisional-to-final
state marker. The collector allowlist therefore was **not** broadened to
`download.bls.gov`: reachability and evidentiary value were not confirmed here.

## 5. Decision against automatic READY

RSS is authoritative for reference-release discovery; separately governed annual
applicability may then start `WATCHING`. API
numeric observations and flat-file contents are provider data, but their current
availability and hashes are diagnostic/replay evidence only. The examined
review has established no annual-revision marker, provisional/final marker, or
nonnumeric cohort version. No new live API request occurred because `BLS_API_KEY`
was unavailable, so this is not a claim of exhaustive API reconnaissance. Numeric change, numeric no-change,
latest-month completeness, calendar date, RSS existence, or an operator assertion
cannot fill that gap.

Consequently no reviewed machine state can distinguish the documented April 2026
model-based publication from the May 2026 final non-modeled database loading. It
also cannot retrospectively reconstruct the staged/provisional/final 2025 states.
The pure C1 architecture can represent both sequences when authoritative class
evidence exists, but these surfaces cannot produce that evidence today. No
normalized processing-class completion object is produced and the only justified
state after separately proven applicability is `WATCHING`; RSS alone does not
manufacture that applicability, and `READY` is not evaluated from data.

Failure semantics remain distinct: RSS transport, parser drift, API transport,
API schema drift, download transport, and provider contradiction are fatal
governance failures; event absent is `NOT_EXPECTED`; valid event with completion
unproven is `WATCHING`; only independently authoritative completion of both
`model_based_state` and `substate_nonmodeled` could be READY.

## 6. Boundary and next action

No annual deep was executed, candidate built/published, accepted pointer moved,
or satisfaction created. Ordinary overlap remains three inclusive years and
annual deep remains 1976 through explicit reviewed end year.

The next action is a bounded provider-contract investigation with BLS: identify an
official machine-readable publication/status endpoint (or documented immutable
version marker) that explicitly covers final loading for all non-modeled metro **and**
county areas. Capture and commit minimized official bytes only when licensing and
repository policy permit, then test 2025/2026 staged transitions. Do not start C2
unless that contract independently establishes both classes; absent such a BLS
surface, governance requires leaving automatic READY unresolved rather than adding
a recurring human assertion.
