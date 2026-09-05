# BPS production-candidate integration v0.1

**Status:** provider-family contract **CLOSED**; durable pin architecture
**COMPLETE**; hosted BPS physical-member registration **IMPLEMENTED**; family
resolution **NOT YET IMPLEMENTED**; BPS acceptance **NOT YET PERFORMED**.

## Reconciliation with common automation

The implementation reuses canonical `create_artifact`, package-compatible
manifests, governed configuration hashes, the compiled and provisional provider
adapters, and the monthly lifecycle's immutable identity/collision vocabulary.
It does not add a BPS orchestrator, artifact backend, accepted pointer, Source
Set, Redfin dependency, DuckDB write, or schedule.

The only common extension is `monthly_source_input_pin_v1`. It is source
agnostic: a cycle/source/release owns a named inventory of exact URLs,
retrieval metadata, and SHA-256 values. Normal may discover when no pin exists;
resume and replay require a pin and cannot call discovery. Exact repeats are
idempotent and a contradictory pin for the same authority fails closed. Bytes
are checked against every member hash before either adapter executes.

## Independent BPS candidates

The compiled adapter dynamically discovers `census_bps`, pins and checks the
ZIP, requires one unambiguous authoritative `TOTAL_UNITS` mapping, full 168
current-release geography presence, safe numeric values, and non-conflicting
duplicates, then creates a complete-history candidate without manufacturing
months. Evidence includes the exact pin, ZIP/member hashes, observation bounds,
coverage, and duplicate diagnostics.

The provisional adapter independently discovers and coherently resolves all
three `census_bps_provisional` members. It pins and checks each member, requires
one release/month and 167/167 state/county coverage, sums only the four estimate
UNIT fields, rejects unsafe tokens/conflicting duplicates, excludes the nation,
and records CBSA evidence as `OUT_OF_GOVERNANCE`. It creates a separate
current-month-only candidate with its own identity and evidence.

Both candidates retain logical governed family `bps` in evidence, while their
artifact, canonical row, catalog, and cycle-result identities are respectively
`census_bps` and `census_bps_provisional`. They are independently addressable,
independently published candidates and are deliberately not combined.

## Source-agnostic hosted pin lifecycle

Provider pins use the same GitHub Contents durable state backend as automated
cycle-result records. Each immutable record has the independent
`(cycle_id, source_id)` path
`config/monthly_source_input_pins/<cycle_id>/<source_id>.json`. A create is
followed by a read-back before execution is permitted. Exact repetition is an
idempotent no-op; different content at the same authority is a terminal
identity collision. This commit point deliberately precedes canonicalization
and publication, so a terminated or failed job can leave `pin present,
candidate absent` and a later job can resume without discovery.

The common lifecycle is now: normal reads the authority first, discovers only
when absent, durably creates and re-reads the pin, and executes from the
re-read value. Resume and replay require that authority and never invoke the
discovery callback. A newer provider release therefore cannot replace a
historical selection. Missing pin, failed persistence/read-back, malformed
member inventory, contradictory identity, and byte mismatch all fail closed.

Before that provider-input lifecycle is entered, the cohort planner applies a
source-agnostic authority order: a validated durable successful cycle result
wins over a durable provider pin, which wins over new discovery. Normal-mode
re-entry and explicit resume therefore reuse completed physical members and fan
out only missing members. For a raw-pin member with no result, execution still
uses its existing historical pin without discovery; only a member with neither
authority may discover in normal mode.

## Normal re-entry corrective evidence

A live re-entry into `monthly_cycle__2026-07__7cab1c5df177a1e4` exposed that the
former planner treated every normal invocation as a new fan-out. FRED and LAUS
each acquired and published a valid new immutable candidate before attempting
to write a second, contradictory result for an already-complete physical
cycle/source key. `GitHubCycleResultStore` correctly rejected both writes with
`IdentityCollisionError`; this was not a storage failure. The planner now reads
and validates durable completion before source execution in both normal and
resume modes, preventing acquisition, canonicalization, publication, and result
recording for completed members.

The resulting FRED candidate
`src__fred_macro__2026-08__r1__b5d6104a9b8bd99e` and the corresponding LAUS
candidate remain valid immutable catalog evidence, but neither supersedes the
durable result for the completed July cohort. They are intentionally retained;
this correction adds no deletion, garbage collection, or candidate cleanup.

## Policy inventory and compatibility

Hosted barrier membership now resolves from the compact
`monthly_source_execution_registry_v1` registry's
`required && hosted_cohort_enabled` entries rather than a runtime constant.
It is separate from cohort identity policy so introducing lifecycle metadata
does not invalidate already-durable Redfin readiness/cycle identities.
The required hosted cohort is now Redfin, FRED, CES, LAUS, `census_bps`, and
`census_bps_provisional`. Both BPS entries declare `durable_raw_input_pin` and
are enabled as independent barrier requirements. A result for one cannot
satisfy the other, and their provider release identities need not agree.

FRED, CES, and LAUS retain `legacy_candidate_evidence` compatibility. Their
existing workflows acquire and publish under their already accepted semantics;
their canonical output, source identity, pointer protection, and resume paths
are unchanged. They are not assigned fabricated raw-file pins in this pass.
Their replay behavior is correspondingly unchanged. The corrected strict
no-rediscovery replay rule applies to sources declaring
`durable_raw_input_pin`; the common lifecycle smoke proves that rule with a
discovery callback that fails if invoked. Migrating an existing source to raw
input replay requires truthful provider-specific pin material and is an
explicit later change, not an implicit reinterpretation here.

## Safety and next bounded step

No accepted pointer moved; no Source Set or market artifact was created; no
Redfin readiness was read or consumed; no DuckDB was opened or changed; and no
cron was enabled. The offline smoke uses bounded generated provider-shaped
fixtures and makes no live-provider claim.

The common reusable BPS workflow selects one physical member and calls
`bps_hosted.execute_member`. For compiled, normal discovery retrieves the
discovered ZIP, persists and reads back its one-member pin, builds the governed
168-geography complete-history artifact, publishes it through the existing
GitHub Release backend, and writes the physical cycle result. For provisional,
the same path coherently discovers and persists state/county/CBSA, builds the
167-geography current-month artifact while retaining CBSA as
`OUT_OF_GOVERNANCE`, then publishes and records its separate physical result.
Resume/replay require and reuse each member's historical pin; exact pinned URLs
are retrieved and hash-verified, and discovery is prohibited.

The master has two explicit reusable-workflow job edges because GitHub workflow
topology cannot be dynamically selected from repository JSON; barrier
membership itself remains registry-driven. No live candidate publication was
performed by this implementation pass; fixtures validate publication handoff
and durable-result ordering offline. The next bounded phase is BPS family
resolution, followed separately by controlled candidate acceptance. Neither is
implemented here.

## Hosted compiled-execution corrective pass

The live cycle `monthly_cycle__2026-07__7cab1c5df177a1e4` has a reproducible
compiled-member cancellation.  In run 1 the compiled input pin was durably
persisted, the provisional member completed, and the compiled member was
cancelled before a durable result.  In run 2 the five completed sources
(`redfin`, `fred_macro`, `ces`, `laus`, and `census_bps_provisional`) were
reused, only `census_bps` was scheduled, and its exact existing compiled pin
was reused; compiled execution was cancelled again.  This is neither a
provider-contract failure nor a durable-pin failure.  GitHub marked the Python
step cancelled without a Python traceback, so resource exhaustion remains a
suspected cause rather than a proven one.

Inspection found concrete avoidable amplification: the approximately 459 MB
ZIP member was expanded into one `bytes` object and then parsed into one
provider-wide DataFrame, while pin hashing separately loaded the ZIP wholly
into memory.  Compiled inspection now hashes the CSV member incrementally,
reads only contract-required columns in bounded chunks, accumulates
provider-wide row counts incrementally, and retains only governed monthly rows
for the unchanged verifier.  Pin creation and verification use the repository's
single streaming SHA-256 helper.  Stage markers bracket pin resolution,
retrieval, hash verification, ZIP inspection, verification, artifact creation,
publication, and durable-result recording without entering artifact evidence.

The publication package contains only the governed canonical artifact (168
geographies and provider-observed history), not the provider ZIP or unfiltered
CSV.  Consequently its existing `read_bytes()` handoff is not the material
amplification identified here; the common GitHub Release publisher is unchanged
in this bounded pass.  A further hosted retry of the exact durable pin is the
next bounded step.  Family resolution and acceptance remain prohibited until
the compiled physical result succeeds and its diagnostics are reviewed.

## Provisional r2 physical-presence correction

The exact-pinned `2607` provisional evidence distinguishes the 220 configured
logical-applicable identities from the 217 identities physically present in
that release: five states, 162 counties, and 50 CBSAs. Martinsville, VA
(`32300`), Ocean City, NJ (`36140`), and San Luis Obispo, CA (`42020`) are the
three configured provider-compatible CBSAs absent from provisional `2607` and
present only in the compiled parent. They remain in the governed 53-CBSA
logical-family union; no registry contraction or synthesis is authorized.

Candidate evidence now records configured count, physical present count,
present counts by provider geography type, missing-configured count, and the
exact missing inventory. The physical gate requires complete state and county
coverage while allowing explicitly diagnosed release-variable CBSA absence.
All prior token, duplicate, identity, no-nation, placeholder, unsupported
concept, and exact-code mapping controls remain fail-closed.
