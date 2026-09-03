# BPS production-candidate integration v0.1

**Status:** provider-family contract **CLOSED**; candidate adapter and common
durable provider-input lifecycle implemented; BPS hosted registration
**NOT YET IMPLEMENTED**; family resolution/promotion **NOT IMPLEMENTED**.

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

Both candidates keep canonical row `source_id=bps`; their evidence records the
independent physical source identity. They are deliberately not combined.

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

## Policy inventory and compatibility

Hosted barrier membership now resolves from the compact
`monthly_source_execution_registry_v1` registry's
`required && hosted_cohort_enabled` entries rather than a runtime constant.
It is separate from cohort identity policy so introducing lifecycle metadata
does not invalidate already-durable Redfin readiness/cycle identities.
This keeps the currently implemented Redfin/FRED/CES/LAUS cohort unchanged and
allows two independent BPS physical execution identities later. BPS entries
declare `durable_raw_input_pin` but remain disabled from hosted membership.

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

The next bounded step is to add the two thin BPS reusable workflows. Each must
invoke its provider adapter through the common durable lifecycle, publish via
the existing GitHub Release backend, record the normal cycle result, and then
enable its already-declared policy entry. The master still has explicit job
edges because GitHub reusable workflows cannot be dynamically selected from
repository JSON; this is workflow topology, not barrier membership or provider
semantics. Hosted cohort validation follows. BPS family resolution and
controlled acceptance remain a later, separate phase.
