# LAUS annual-processing detector v0.1

**Status:** LAUS-C1 pure policy and fixture validation. No hosted LAUS workflow,
live satisfaction, publication, or accepted-pointer mutation is included.

## 1. Evidence reconnaissance and confidence labels

This audit distinguishes three categories:

* **Documented BLS behavior.** The official [LAUS methods page](https://www.bls.gov/lau/laumthd.htm)
  and [LAUS notices collection](https://www.bls.gov/lau/notices/) are the provider
  authorities retained by the existing migration record. BLS annual processing
  applies new population controls and revised inputs, re-estimates modeled areas,
  and revises substate estimates consistently with revised state totals. Publication
  can be staged and exceptional revisions can extend beyond the normal five-year
  surface.
* **Empirically observed API behavior.** API v2 observations expose a `footnotes`
  array containing code/text pairs, but no separate immutable revision-vintage or
  processing-completion field. The LAUS-B persisted acquisition observed code `X`
  with value `-` for provider unavailability during the 2025 appropriations lapse.
  Neither the API response contract nor retained current observations supplied a
  stable annual-final/provisional code across `ST`, `MT`, and `CN`. The API is a
  current-truth interface: it does not retain enough prior response state to replay
  what its metadata looked like on historical publication dates.
* **Repository policy decision.** Release/publication evidence is accepted only
  from HTTPS `bls.gov` sources and must be normalized with provider release ID,
  annual reference year, processing class, and `underway`/`complete` status.
  Numeric comparison and observation footnotes are never authorization.

Provider access from this implementation environment was blocked (the web tool
returned HTTP 401 and direct BLS HTTPS requests were rejected by the proxy with
403). Consequently C1 does not claim a new live scrape or fabricate historical
API snapshots. The official URLs and findings already captured during LAUS-A/B,
plus the persisted LAUS-B observation evidence, define the strongest defensible
contract. A future collector must preserve the official source bytes and prove
its notice-to-normalized-state parser before C2 can rely on it.

## 2. Publication mechanics found

BLS exposes a release calendar and LAUS publication/notices pages, but the release
calendar event is not a complete-universe readiness bit. Historical evidence shows
why: 2025 processing included exceptional geography/population-control changes,
some reconstructions to series beginnings, and staged publication; in 2026 the
appropriations interruption delayed processing and modeled/state revisions preceded
substate revisions. Thus a state/model release is evidence to watch, not evidence
that governed metro/county histories are ready.

No stable machine-readable API observation code was established for “annual
revision complete,” “provisional annual processing,” or “final annual processing.”
Official notice content is deterministically fetchable provider publication
information, but it is document content rather than an API-wide status field. C1
therefore defines a strict normalized evidence boundary and deliberately does not
pretend that numeric changes fill the missing status field.

## 3. Governed processing classes

Classification uses exact provider area-code prefixes in the frozen registry:

| prefix | provider geography | policy class | geographies | series |
|---|---|---|---:|---:|
| `ST` | state | `model_based_state` | 5 | 20 |
| `MT` | metropolitan area | `substate_nonmodeled` | 37 | 148 |
| `CN` | county | `substate_nonmodeled` | 163 | 652 |

The readiness gate therefore covers 20 state series and 800 substate series,
all 820 frozen governed series across all 205 geographies. The grouping does not
alter registry membership and does not infer class from geography names.

## 4. Hybrid detector and authoritative rule

Input has two layers:

1. `release_event`: official BLS publication evidence with `expected=true`. Its
   presence starts the watch for the provider-derived annual reference year.
2. `processing_classes`: class-specific official BLS publication evidence. READY
   requires exactly the configured classes and `status=complete` for each.

The authoritative rule is:

```text
official BLS annual event for reference year Y
+ official BLS complete evidence for model_based_state at Y
+ official BLS complete evidence for substate_nonmodeled at Y
=> READY_FOR_ANNUAL_DEEP
```

Missing classes, `underway`, or ambiguous completion remains WATCHING. An
untrusted URL, inconsistent annual year, duplicate class, unknown class/status,
or conflicting durable record raises an error and cannot authorize deep. Numeric
change is only corroboration; no-change is irrelevant once official completion is
established. Footnote arrays remain independently preserved and classified, so
`X` cannot become annual-ready evidence even when another code is present.

## 5. Vintage, states, and acquisition selection

`bls-laus-annual-processing-v1:<annual_reference_year>` is the semantic vintage.
The reference year comes from consistent official BLS evidence. Release IDs and
source URLs remain detector evidence but are not execution identities. Workflow
run, Git SHA, retrieval time, wall clock, GitHub Release, and candidate identity
cannot change the vintage.

| state | condition | selected mode |
|---|---|---|
| `NOT_EXPECTED` | detector disabled or no new official event | `ordinary_overlap` |
| `WATCHING` | event exists; any required class not complete | `ordinary_overlap` |
| `READY_FOR_ANNUAL_DEEP` | all required classes officially complete; no satisfaction | `annual_deep` |
| `ANNUAL_DEEP_SATISFIED` | exact vintage has durable successful satisfaction | `ordinary_overlap` |

Ordinary bounds remain inclusive `end_year-2..end_year`. Annual deep is full
`1976..explicit current/reviewed end_year`, protecting normal, unusually deep,
methodology/geography, and series-beginning revision surfaces.

## 6. Durable satisfaction

The canonical record contains schema/source/vintage/reference year, complete
detector evidence, status, satisfied artifact ID/content hash/package hash,
provider release ID, and cycle ID. Construction accepts only a `laus`
`annual_deep` result with succeeded/passed/published-verified invariants. Merely
reaching READY or attempting/failing a run cannot create it.

`add_satisfaction` is the narrow CAS semantic used by existing durable control
planes: absent creates; byte-equivalent semantic state is an idempotent no-op;
any other record for the same vintage raises an identity collision.
`GitHubAnnualSatisfactionStore` persists one immutable Contents record at
`config/laus_annual_deep_satisfactions/<year>.json`, using the existing bounded
read/create/re-read CAS convention. C1 wires no caller and records no live
satisfaction; C2 may invoke this already-defined durable boundary only after a
successful governed annual-deep result.

## 7. Historical validation and limitations

For 2025 the contract represents a state/model completion followed by incomplete
substate publication as WATCHING, and only the later complete substate evidence
as READY. It can also represent exceptional series-beginning scope without
changing the full-history operation. This is contract validation against official
historical documentation, not a replay of historical API states.

For 2026 the same state-first/substate-later sequence remains WATCHING between
the two publications despite the delayed calendar. No date is hard-coded. Again,
the API does not retain historical publication-state metadata, so retrospective
automatic detection is not claimed.

The raw half of a governed collector now archives and verifies exact official BLS
publication bytes when run in a network-capable environment. Provider access was
still unavailable during C1b, so no reviewed official bytes exist from which to
justify class-specific parsing. Until a follow-up capture review freezes and tests
that parser, automatic production READY remains unestablished. This is an
intentional fail-closed stop under the insufficient-evidence condition.

## 8. C1 boundary and next action

C1 adds only the pure detector/policy, exact-prefix classifier, annual-deep alias,
durable-record contract, and fixture smoke coverage. It does not add LAUS to a
hosted workflow, change the production schedule, consume Redfin readiness,
publish a candidate, or move `accepted.source.laus`.

Before C2, execute the C1b raw collector outside the blocked environment, review
the archived official bytes, and implement the still-missing class-specific parser
with official fixtures and drift tests. Only after that gate passes should C2 bind
the pure selector and CAS-backed satisfaction store into the monthly source runner.
