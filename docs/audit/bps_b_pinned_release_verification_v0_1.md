# BPS-B pinned-release verification execution record

**Date:** 2026-09-01

**Migration state:** `RECON_COMPLETE_IMPLEMENTATION_BLOCKED`

**Production integration:** not performed

## Outcome

BPS-B could not truthfully cross its provider-evidence decision gates in this
Codex execution. Both the web research facility and direct HTTPS access to
`www.census.gov` / `www2.census.gov` were blocked before any Census response:
web search returned HTTP 401 and the environment proxy rejected Census CONNECT
requests with HTTP 403. No third-party mirror was substituted. Therefore no
actual release month, ZIP/member hash, raw size, provider schema, official
missing-value vocabulary, geography coverage, release-to-release comparison,
or provider-versus-local equivalence count is claimed.

This is a provider-access blocker, not evidence that BPS-A assumptions are
correct. In particular, BPS-A's guessed textual unavailable tokens have been
removed from the governed adapter: only actual nulls are diagnosable missing;
every string token fails closed until proven.

## Hosted pinned-release runner

The read-only `jobs.monthly_refresh.bps_bootstrap` runner and manual
`bps-pinned-verification.yml` workflow now make the blocked verification
executable without weakening the official-source requirement. The workflow has
no schedule and requires an explicit `YYYY-MM`. It downloads only the derived
official Census URL, then emits:

* `raw_evidence_manifest.json`: URL, HTTP metadata, retrieval timestamp, ZIP
  SHA-256/size, member inventory, selected CSV SHA-256/size, raw schema/count;
* `provider_contract_diagnostics.json`: exact total-field candidate, every
  nonnumeric token/count, governed/raw counts and observation bounds;
* `nonnumeric_examples.json`: one bounded representative row per token;
* `geography_coverage.csv`: every configured identity, provider binding,
  presence, first/last observation, count, and latest-value availability;
* `canonical_provider.parquet`: diagnostic numeric rows only;
* separate compiled-only and compiled-plus-provisional read-only equivalence
  details/summaries.

The runner accepts an already persisted ZIP so hosted acquisition evidence can
be reanalyzed without another network request. It requires exactly one CSV
member and exact required schema aliases; ambiguous total fields and structural
drift fail closed. It never opens a DuckDB in write mode and contains no
publication, Release, catalog, acceptance, readiness, or cohort operation.

## Pinned release and provider semantics

| Required result | This execution |
|---|---|
| release ID/month and official URL | not available; workflow input remains required |
| raw ZIP/member hashes and sizes | not available until hosted run |
| authoritative total-units field | unresolved; runner inventories `total_units`, `units_total`, and `total_units_authorized` candidates and blocks ambiguity |
| unavailable/suppression vocabulary | unresolved; no textual token admitted |
| zero | numeric zero remains a legitimate numeric value |
| compiled snapshot/finality | unresolved |
| omission/retraction meaning | unresolved |
| annual/historical revisions | unresolved |

The bounded repository fixture is synthetic and deliberately labels token `D`
as unexplained. It proves enumeration and fail-closed behavior; it is not
provider evidence and cannot promote `D` into the contract.

## Geography and historical coverage

The provisional registry remains 168 identities (1 nation, 5 states, 162
counties) because no provider payload contradicted it. Present count, final
count, and Franklin City resolution remain unresolved pending the hosted
coverage output. The runner includes all 168 rows even when absent, preventing
absence from being hidden by an inner join.

The provider historical range is likewise unresolved. Local read-only facts
still establish only the BPS-A observations: serving compiled truth begins in
2006-04 while the older public database includes some governed identities from
1988-01. Without the pinned provider rows, this cannot be classified safely as
provider coverage, schema transition, ingestion limitation, or local migration
loss. No shorter bootstrap history is accepted and no unreconciled older local
history is imported.

## Equivalence and refresh policy

No actual bootstrap-equivalence counts exist in this execution. The hosted
runner will report exact matches, provider revisions, provider-only rows,
prior-only rows, and identity conflicts separately for compiled-only and
serving/current truth. It does not treat a newer provisional month as a defect
in the pinned compiled release.

Reconciliation remains provisional Policy B (provider overlap wins,
provider-only appends, prior-only persists) solely as the fail-safe behavior;
it is not newly validated by BPS-B. Deep-refresh outcome remains D. Target month
continues to derive from canonical provider observation maximum, but cannot be
frozen until provider schema/coverage gates pass.

## State-safety verification

Fixture execution hashes `config/artifact_catalog.json` before and after and
asserts byte identity plus absence of an accepted `bps` pointer. The workflow
does not write the catalog. It was not added to production fan-out, has no cron,
does not consume Redfin readiness, and cannot publish or accept an artifact.

## Material blockers

All five BPS-B decision questions remain blocked by the same missing official
pinned-release evidence: authoritative total field; documented/observed
unavailable vocabulary; exact valid geography coverage including Franklin
City; omission/retraction meaning; and whether each compiled release performs
full-history reconciliation.

## Single next BPS step

Manually dispatch **BPS pinned-release verification** with an explicit recent
Census release month, retain the diagnostic artifact, and review its hashes,
schema/token inventory, 168-row coverage, two equivalence summaries, and
official Census documentation before changing this migration state. Do not
publish or accept during that review.
