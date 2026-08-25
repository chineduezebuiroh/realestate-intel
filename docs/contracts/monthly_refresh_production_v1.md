# Production monthly refresh contract v1

**Status:** normative design; no production schedule is enabled.

## Identities, readiness, and inventory

A logical cycle ID is the hash of the governed Redfin `drop_id`, its complete registered file-set
content hash, canonical target month, and orchestration-policy hash. GitHub run ID/attempt are
evidence only. Provider release IDs and per-source observation maxima remain independent facts;
eligibility means the source-specific freshness/revision policy passed for this cycle, not that all
observation maxima equal Redfin's month.

Redfin is ready only when one atomically registered seven-family drop has immutable file hashes,
passes registration and validation, is not quarantined, and has no `consumed_successfully` record.
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

On retry, the durable ledger pins each successful candidate's artifact ID and both hashes. Valid,
resolved candidates are reused; only absent/failed/expired-policy nodes rerun. Any cycle mismatch,
hash mismatch, pointer drift, or changed candidate for a pinned success fails closed. The exact same
complete entries produce one semantic Source Set v2 identity. A new Actions attempt is not a cycle.

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
