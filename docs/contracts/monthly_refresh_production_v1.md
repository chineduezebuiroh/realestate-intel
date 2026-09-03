# Production monthly refresh contract v1

> Phase 3A implements the independently executable Redfin candidate boundary
> in [Redfin monthly source execution v1](redfin_monthly_source_v1.md), stopping
> before this contract's barrier and cohort promotion.

**Status:** normative design; no production schedule is enabled.

## Identities, readiness, and inventory

A logical cycle ID is the hash of the governed Redfin `drop_id`, its complete registered file-set
content hash, canonical target month, and orchestration-policy hash. GitHub run ID/attempt are
evidence only. Provider release IDs and per-source observation maxima remain independent facts;
eligibility means the source-specific freshness/revision policy passed for this cycle, not that all
observation maxima equal Redfin's month.

The cycle `target_month` is specifically the Redfin catalyst/drop month and is cohort identity
evidence; it is not a universal acquisition cutoff or observation-vintage instruction. Each
automated source resolves its truthful provider state under its own governed acquisition contract.
For FRED, an omitted source target means acquire the complete governed current response and derive
the artifact target from its canonical `observation_max` (`inferred_observation_max`). Thus a July
2026 Redfin cycle may validly pin a FRED artifact with `observation_max=2026-08-31` without changing
the Redfin-derived cycle ID. Source Set v2 ultimately pins those exact immutable, potentially
different-vintage artifacts; it does not infer membership from matching calendar months.

Redfin is ready only when one atomically registered seven-family drop has produced a validated,
durably verified immutable candidate recorded in `config/monthly_refresh_readiness.json`, and that
record has not been marked consumed by the later promotion commit point.
The minimum durable ledger is keyed by drop content identity and records `registered`, `validated`,
`refresh_in_progress`, `failed_retryable`/`quarantined`, and `consumed_successfully`; “landed” remains
inbox state. A consumed drop is a successful no-op. A failed cycle retains its identity and resumes.

The versioned policy owns required sources, acquisition modes, workflow boundaries, dependencies,
timeouts, and promotion rules. Missing required results fail closed. Optional sources, when later
introduced, must have an explicit exclusion policy and cannot silently change the required set.

## Execution and result interface

The scheduled master performs the readiness check, creates/resumes the cycle ledger, and invokes
source workflows. Independent nodes run in parallel; only declared dependency edges serialize.
Each source workflow is also dispatchable alone and owns acquisition/reconciliation/validation,
immutable publication, and durable verification—but in production candidate mode it MUST NOT move
an accepted pointer.

Every invocation returns exactly `monthly_source_execution_result_v1`: `source_id`, `cycle_id`,
`status`, exact `candidate_artifact_id`, `artifact_content_hash`, `package_sha256`,
`publication_state`, `validation_status`, `provider_release_id`, `observation_max`,
`prior_artifact_id`, `source_change_detected`, `retryability`, and `evidence_uri`. Success requires a
validated, published-and-resolved immutable candidate. Provider diagnostics remain behind the
evidence URI, keeping the orchestrator provider-neutral.

## Barrier, retry, and commits

States are `WAITING_FOR_REDFIN → READY → SOURCE_REFRESH_RUNNING → SOURCE_BARRIER`, then
`FAILED_RETRYABLE|FAILED_TERMINAL` or `SOURCE_SET_VALIDATED → CANONICAL_ASSEMBLY_VALIDATED →
PROMOTION_COMMIT → COMPLETE`. Only the listed transitions are legal. An incomplete barrier cannot
create/accept Source Set v2, assemble/promote canonical market, advance serving, or trigger Macro
Regime.

On retry, the durable cycle-result registry pins each successful candidate's artifact ID and both hashes. Valid,
resolved candidates are reused; only absent/failed/expired-policy nodes rerun. Any cycle mismatch,
hash mismatch, pointer drift, or changed candidate for a pinned success fails closed. The exact same
complete entries produce one semantic Source Set v2 identity. A new Actions attempt is not a cycle.

`reused_source_ids` means a source execution was skipped because an already-successful cycle pin was
revalidated and supplied to the barrier. It does **not** mean a source workflow executed and found
unchanged provider data; that case is a newly returned successful result with
`source_change_detected=false`. Immutable artifact reuse, equal prior/candidate artifact identities,
and idempotent publication are likewise not execution reuse. The barrier derives this evidence only
from successful results supplied through its explicit revalidated resume-pin path, never from
candidate content or source-change fields. Resume reconstructs Redfin only from the exact validated
readiness/catalog pin. Automated sources use the common `monthly_source_cycle_results_v1` registry:
each record embeds its complete successful result contract plus cycle, source, and policy compatibility,
and is revalidated against the immutable catalog before fan-out. Missing catalog evidence selects only
that source for rerun; contradictory durable identity fails closed. It accepts no caller-supplied result JSON. Drift in
the Redfin artifact/content/package/publication identity fails before fan-out. If FRED has not yet
returned a successful pinned result, its retry may truthfully resolve a newer current provider state
under the same source policy; a successfully pinned FRED candidate could not be silently replaced.

FRED acquisition uses three total attempts with bounded 2- and 5-second backoffs. HTTP 5xx, 408,
429, URL/transport, timeout, and connection failures are transient. Exhaustion is surfaced as an
explicit transient acquisition failure, leaving the missing source retryable at the cohort barrier.
Validation, schema, semantic-data, identity, catalog, and publication failures are not acquisition
retries and remain terminal at their owning boundary. Acquisition failure never selects stale data
as a substitute.

### Durable automated-source cycle-result authority

The authoritative producer record is one immutable tracked JSON object at
`config/monthly_source_cycle_results/<cycle_id>/<source_id>.json`. Its semantic key is the exact
`(cycle_id, source_id)` pair. `config/monthly_source_cycle_results.json` is only the versioned registry
root/bootstrap index, not a runtime aggregate writer. The accepted July 2026 FRED and CES seeds were
deterministically moved, without changing embedded artifact, content, package, provider, or cycle
identity, into this authoritative layout.

An automated workflow creates its record only after acquisition/reconciliation, artifact validation,
immutable publication and remote resolution, and successful common-result construction. Immediately
before creation it reads the durable catalog and revalidates cycle/source identity, successful/passed
states, unchanged accepted pointer, verified publication, artifact ID, content hash, package SHA,
provider release identity, and catalog source. Missing or contradictory evidence fails closed. A
workflow exposes successful output to the cohort only after the durable write succeeds. Transient
transport or exhausted ref-contention retries fail the source job; contract and identity collisions
are terminal.

The collision-bearing identity is cycle ID, source ID, candidate artifact ID, artifact content hash,
package SHA, provider release ID, and prior artifact ID, together with compatible durable-result,
execution-result, and policy schema contracts. `prior_artifact_id` is identity-bearing because it pins
the reconciliation baseline from which the governed candidate was deterministically established; a
different baseline is incompatible lineage even when candidate bytes happen to match.

The fields of `monthly_source_execution_result_v1` are classified as follows:

| Classification | Fields |
|---|---|
| Semantic identity / collision-bearing | `cycle_id`, `source_id`, `candidate_artifact_id`, `artifact_content_hash`, `package_sha256`, `provider_release_id`, `prior_artifact_id` |
| Required governed invariant, not identity-bearing | `schema_version`, `status`, `validation_status`, `publication_state`, `accepted_pointer_changed` |
| Receipt/execution metadata | `retryability`, `evidence_uri` |
| Derived diagnostic | `observation_max`, `source_change_detected` |

Successful, passed, remotely verified publication and an unchanged accepted pointer are required
governed state invariants. They are validated before either creation or reuse rather than used to
distinguish two valid immutable candidates. Execution/result diagnostics (`source_change_detected`,
`retryability`, `observation_max`, and `evidence_uri`) do not participate in immutable candidate
identity. In particular, source-change detection describes the relationship observed by one
acquisition/reconciliation execution, so differing true/false values do not contradict an otherwise
identical governed candidate. The first authoritative record is preserved; diagnostic differences do
not merge or rewrite it. Run IDs, timestamps, attempts, publisher SHAs, Actions metadata, and recording
receipts are outside the execution-result record and also do not participate in semantic identity.
Repeating the same governed identity is an idempotent no-op. Different governed identity for the same
key is an identity collision and never overwrites the first success. Each source writes a different path, so
parallel automated-source completions share no mutable aggregate. Contents creation is atomic and
bounded read-after-conflict retries distinguish an exact repeat from a contradiction. Barrier-owned
aggregation was rejected because sibling failure before the barrier would lose an already-published
success and force unnecessary reacquisition.

Resume loads the immutable records plus bootstrap index and revalidates compatible entries against the
catalog, allowing either automated sibling to be reused after the other fails. Replay deliberately
ignores pins and executes all sources; an exact producer write is a no-op and contradictory evidence
collides. Normal fan-out remains parallel. This lifecycle creates no Source Set, advances no accepted
pointer, and does not consume Redfin readiness.

### Durable provider-input pins

The preceding replay statement describes legacy candidate-result pins. Sources
whose execution registry policy is `durable_raw_input_pin` additionally use
`monthly_source_input_pin_v1` at
`config/monthly_source_input_pins/<cycle_id>/<source_id>.json`. Normal first
reads that authority, discovers/retrieves only when absent, immutably creates
the record through the existing GitHub Contents backend, and re-reads it before
provider-dependent execution. Resume and replay require and execute from that
record without discovery. A source may therefore fail after pinning and resume
against exactly the same bytes before any candidate exists. Missing evidence,
member/hash drift, contradictory create, or failed durable read-back is
terminal. `legacy_candidate_evidence` sources preserve their established
behavior until their truthful raw replay material is separately governed.

Hosted barrier membership comes from
`config/monthly_source_execution_registry.json`; `required=true` and
`hosted_cohort_enabled=true` selects an execution member. This registry is not
cycle identity policy, so lifecycle metadata does not invalidate an existing
Redfin readiness identity. It can represent multiple physical execution
members without implying family resolution or Source Set membership.

Redfin intentionally does not write this registry. Its local/manual boundary already commits durable
readiness plus catalog evidence, and the hosted resolver reconstructs the common result from that pin.
Duplicating that authority merely for symmetry would create two control-plane truths.

Commit points are deliberately separate: (1) truthful immutable source publication/cataloging;
(2) validated candidate pinning; (3) complete Source Set v2 publication/acceptance; (4) validated
canonical artifact publication/acceptance; and (5) source-pointer advancement. Successful source
objects survive failed cohorts, while accepted production remains unchanged.

Production uses **cohort-controlled promotion**. After Source Set v2 and canonical assembly both
validate, one idempotent promotion phase uses expected-old/CAS guards to advance canonical,
source-set, and participating source pointers to exactly the pinned identities, then marks Redfin
consumed. Until a multi-object transactional registry primitive exists, the implementation must use
a prepared promotion record plus ordered CAS operations and recover idempotently; downstream readers
must follow the canonical pointer, so partial bookkeeping cannot expose an incomplete cohort.
Source-local activation remains only migration, investigation, or explicit recovery behavior.

Automatic promotion additionally requires ownership/geography/metric, revision/change, package,
remote-resolution, governed-config, complete-inventory, Source Set v2, and canonical-market checks.
An override is authenticated, reasoned, evidence-linked, scoped to an existing cycle, and never
permits a partial required cohort.

## GitHub Actions decision and operations

Use a thin master plus reusable source workflows (`workflow_call`), with one named parallel job per
policy source (generated/validated against policy) and a barrier job using `needs` and `always()`.
This gives native failure propagation, secrets scoping, logs, outputs, and no API polling. A single
matrix is compact but cannot dynamically call different reusable workflows and makes selective
retry/output handling awkward. Dispatch-and-poll weakens joins, needs broader tokens, and introduces
API/race complexity. A monolithic script would erase source boundaries.

Eventually enable one Saturday schedule (time chosen operationally) on the master only. Each check
locks/reads the durable Redfin ledger: not ready exits successfully before secrets or acquisition;
ready resumes/creates the deterministic cycle and fans out. Source workflows have manual dispatch,
not independent routine schedules.

## Downstream boundary and responsibilities

Assembly consumes only the accepted explicit Source Set v2—never local files, “latest” Releases, or
moving source pointers. The flow is provider → source artifact → Source Set v2 → canonical market →
serving/public projection → Macro Regime/visualization. Serving is packaging, not reconciliation or
orchestration state.

Normal human work ends after downloading and landing the seven Redfin files. Registration,
validation, reconciliation, artifacts, automated acquisition, publication, barrier, assembly,
promotion, projections, and downstream triggering are designed to be automatic. This contract does
not implement those production effects.

## Phase 3B executable cohort boundary

The production master is permanently manually dispatchable with three explicit modes. `normal`
performs production-equivalent governed Redfin readiness gating and returns the successful
`no_op/no_eligible_redfin_catalyst` outcome before source acquisition. `resume` requires the exact
drop and deterministic cycle identities and reuses only immutable successes whose artifact ID,
content hash, package hash, publication state, and provider release still match their pins.
`replay` requires those same identities; it is an acceptance operation over the registered lineage,
not a force flag, never changes catalyst consumption, and never advances accepted pointers.

After cycle resolution, the Redfin and FRED reusable workflows are independent sibling jobs. Their
`monthly_source_execution_result_v1` outputs join at an `always()` barrier. The provider-neutral
validator rejects missing, malformed, duplicate, wrong-source/wrong-cycle, unvalidated,
unverified, or pin-drifted results. The evidence classification is `ready`,
`incomplete_retryable`, or `failed_terminal`. Evidence pins the exact two candidates and records
invocation and GitHub metadata. `ready` deliberately means only a complete Phase 3B source cohort:
no Source Set v2, canonical assembly, promotion, serving/public build, Macro Regime run, accepted
pointer activation, or Redfin consumption commit occurs.

The master passes FRED the logical `cycle_id` but intentionally omits a source target month, so the
standalone governed FRED runner retains its `inferred_observation_max` selection rule. Its result
attaches the independently resolved provider release, observation maximum, artifact/content/package
hashes, and prior identity to that unchanged cycle. Barrier readiness remains mandatory before any
downstream progression.

The Saturday readiness schedule remains intentionally disabled until the complete downstream
commit path is governed and accepted. Adding a source consists of a reusable source workflow,
policy membership, the common result contract, and one sibling fan-out job; barrier logic remains
provider-neutral.

### Hosted Redfin durable candidate boundary

Raw Redfin source files are processed locally and are not transported to GitHub-hosted runners.
The local governed Redfin runner publishes a compact immutable source candidate and commits a small
durable readiness record. Hosted orchestration consumes only that durable candidate/control-plane
state. The readiness identity is `redfin_readiness__<cycle_id>` and pins the drop ID/hash, target
month, cycle, artifact/content/package hashes, publication state, and numeric Release/asset IDs.
The artifact catalog remains authoritative; repeated identical records are no-ops and conflicting
cycle reuse fails closed. A publication that succeeds before catalog/readiness commit remains
truthful orphan evidence but is not hosted-ready.

`normal` requires exactly one unconsumed record; none is a successful no-op and multiple records are
ambiguous. `replay` and `resume` use user input only to identify an exact readiness record and then
revalidate it against the catalog. They cannot synthesize trust. Hosted Redfin downloads and checks
only that compact package while FRED runs as a sibling. Barrier `ready` does not promote the cohort
or consume its catalyst: consumption is written only by the later promotion transaction.

The master temporarily has a path-filtered push trigger limited to
`monthly-refresh-orchestration`, solely so the not-yet-default-branch workflow can receive hosted
Phase 3B acceptance. Remove it once the workflow is registered on the default branch. No Saturday
schedule is enabled. Future automated sources remain parallel reusable-workflow siblings.
