# ACS historical persistence reliability hardening

## Incident-to-code mapping

B3 run `39027613733` committed the 2015 result before its immediate mutable
readback failed; unchanged-code run `39088035004` subsequently completed. B5 run
`39306453566` committed the correct, durable 2010 pin before reporting a
read-after-write mismatch; unchanged-code run `39310520767` resumed successfully.
These establish a persistence/readback weakness. They do not establish GitHub
caching, a particular HTTP status, or the precise historical stale-read cause.

| Weakness | Correction |
| --- | --- |
| `write_current()` discards successful commit identity | Return the proposed commit SHA after validating the ref-update response SHA. |
| Pin/result verification reads mutable `main` | Verify exact JSON equality and canonical Git blob identity at the returned immutable commit. Historical catalog writes receive the same verification. |
| The lifecycle immediately calls `store.get()` again | Historical stores resolve a freshly governance-checked main commit and read that immutable snapshot for every `get()` and pre-write read. No cached pin or write receipt bypasses current authority. |
| Temporarily unavailable immutable reads cause false negatives | Four total read attempts, with 0.1/0.2/0.4 second backoff, for missing content or `TransientPublicationError`. Contradictory JSON/blob content and other errors fail immediately. |
| A ref-update transport failure may follow success | Read the durable Git ref with bounded transient retries. Only the exact proposed head plus exact commit-bound content establishes success. Never repeat PATCH, rebase, force, or restart provider acquisition within the operation. |
| Failed batches lose the returned execution list | Track fixed failure boundaries, completed vintages and authoritative write events. On batch failure write sanitized `result.json`; the existing workflow's `always()` artifact upload already collects it. |

## Write and recovery contract

Destination blob CAS, independent plan/authorization checks, physical-key checks,
single-parent commits and non-force ref updates remain required. A concurrent
main advance still rejects the sibling commit. Permission and current content
are rechecked after immutable pin/result verification; historical correctness
does not confer permission to continue under changed governance.

A transient tree/commit creation failure stops immediately, before any ref update;
unreferenced Git objects may remain. A subsequent invocation reconstructs current
authority and destination state before creating anything else. If PATCH's response
is lost, a ref at another SHA (including a later descendant) remains uncertain:
stop and reconcile inventory/exact publication identity before recovery. This
conservative limit deliberately avoids a generalized history-search/retry system.
An unexpected successful PATCH response identity also fails closed.

An unavailable immutable verification never reports success. A later invocation
may recognize the exact already-durable pin/result and return an immutable no-op.
Existing package verification and reconciliation recover published artifacts
without selecting new provider inputs or duplicating immutable publication.

Missing objects at a freshly resolved snapshot receive the same bounded read
window, then may be treated as absent for initial lookup. Expected post-write
objects must exist. Each missing lookup adds at most 0.7 seconds of backoff;
network request duration remains controlled by the existing API transport.

## Failure artifact interpretation

`acs_historical_execution_failure_v1` contains fixed boundary labels, the current
vintage, completed vintages, and write events containing repository-relative
paths and Git identities. It contains no exception strings, provider payloads,
pins, credentials, signed URLs or raw request/response headers.

* `ref_update_uncertain`: candidate commit known; durable ref outcome not established.
* `ref_update_confirmed`: exact ref identity established; full content verification
  has not completed. This is not a verified durable object receipt.
* `verified_durable`: exact commit-bound object and canonical blob verified. A
  later governance or current-state check can still fail execution.

Publication failures conservatively require reconciliation of inventory and exact
Release/package identity. A failure artifact is diagnostic evidence, never an
execution authorization, catalog receipt, or serving promotion. Failures before
entering the batch have no confirmed batch progress to report. Process termination
that prevents exception handling can still prevent artifact creation.

## Shared-helper compatibility

The shared ACS write helper is used by historical physical stores and catalog,
`CurrentCatalog`, ACS family-resolution persistence, cohort-plan persistence,
and ACS prepared-promotion persistence. Its arguments and single-parent/CAS
semantics are unchanged; ignored return values remain valid. All callers now
require the expected ref-update SHA. Existing offline fixtures were adjusted to
return the real response identity; the historical fixture now computes actual
Git blob object IDs rather than hashing content without the Git blob header.

Only historical pin/result/catalog adapters add the new normal-path immutable
content verification. The shared helper verifies content when reconciling an
ambiguous ref update, including for other ACS callers. Those callers may now fail
closed on uncertainty instead of propagating an undifferentiated transient error.
Generic GitHub CAS, monthly provider stores, provider acquisition, serving
promotion mutations, and publication lifecycle are unchanged. ACS prepared-record
persistence adds only the final governance check described below.

The implementation changes the governed code identity. Previously adopted plans
and authorizations continue to reject code/configuration/runtime drift. There is
no compatibility exception and no modification or regeneration of B1–B7 plans.
Any future physical execution using changed code requires the normal reviewed
plan and separate authorization process.

## Protected identities

Base: `2f2c30d1ab2f8878cfec9a64b5748345f0c00d34`. The hosted checkout was clean.
Shell `git fetch origin main` failed because its configured proxy was unreachable;
the GitHub connector independently fetched remote `main` and confirmed this exact
expected SHA before branching. The local remote-tracking ref was stale; it was
not used as the implementation base.

The implementation branch is `fix/acs-historical-write-readback` in its own
worktree. These complete protected Git trees must remain identical to the base:

| Path | Base tree SHA |
| --- | --- |
| `config` | `c3a8cdc8965cc29c9cd184aa8a4adf204413e08e` |
| `artifacts` | `56d69bfbfadd95cb6620969352755f158ba8ff80` |
| `data` | `20e0327b6e3de239b9d2659e04cf62f5d9ae770f` |
| `docs/audit` | `417d43cd9ab94a9dc621a5077a338688ab86f187` |

Equality of these trees covers all tracked historical pins, results, adopted
reviews, catalog and serving pointers, including accepted B1–B7 and August 2026
identities. No live physical artifact, authorization, plan or pointer is written
as part of this change. No workflow dispatch, acquisition, promotion or merge is
authorized by this PR.

## Validation and merge guidance

Offline fixtures replay the B3/B5 boundary shapes using ACS1 2015 and 2010
provider fixtures, rather than claiming to reproduce the historical transport
cause. Coverage includes commit-bound content/blob verification, missing mutable
reads, bounded transient reads, contradictory content, unexpected ref identities,
ambiguous mutations, governance drift after verification, lifecycle read
consistency, every Git write boundary, publication-boundary recovery, multi-vintage
partial progress, and continued rejection of adopted-authorization drift.

Merge after code review and passing relevant checks. No live acquisition is
needed to validate this correction. After merge, inspect the merged diff and
protected tree identities and retain existing accepted pointers. If a separately
authorized future execution fails, inspect its sanitized artifact, reconcile any
uncertain write and exact immutable package identity, and restore reviewed
authority or adopt a new plan through the existing process before retrying.

Validation recorded during implementation:

* Full repository suite: `697 passed, 138 warnings` (69.74 seconds). Warnings
  are existing pandas deprecations in regime feature construction/normalization.
* Combined backfill/boundary/readback suite: `339 passed` (44.40 seconds).
* Final readback suite, including two subsequently added CLI failure-artifact
  cases: `75 passed` (1.09 seconds).
* `git diff --check` and protected-path diff checks passed.

## Corrections following adversarial review of b76d7837

The review reproduced three defects despite the initial passing suite. The final
behavior is:

* Historical `_current_read()` checks current governance and resolves a commit,
  reads at that immutable commit, then checks current governance and the commit
  again. Any head change during the read fails closed, including an unrelated
  advance; governance drift also raises. This brackets post-write current-object
  verification and every lifecycle `get()`. A pin replacement during the final
  GET cannot start retrieval, build, execution or publication.
* `write_current()` revalidates current governance before returning success on
  both ordinary and reconciled writes. Reconciliation additionally requires that
  the freshly checked head still equals the proposed commit. It does not accept
  a later descendant, repeat PATCH, or treat a receipt as current permission.
* Family-resolution persistence resolves a checked immutable snapshot on every
  attempt, validates the Git blob, and rechecks governance and snapshot identity
  before any no-op return. Transient reconciliation failure therefore cannot
  bypass superseded governance through an existing-object retry. Recovery under
  unchanged governance can recognize the exact durable object without a second
  PATCH.
* Cohort-plan and ACS prepared-record persistence recheck governance after their
  final durable rereads, covering existing-record/no-op paths as well as writes.
* Diagnostic state changes occur immediately upon confirming the exact ref and
  upon verifying exact content plus the canonical blob. A later governance check
  or duplicate read failure cannot downgrade previously established evidence.
  `verified_durable` means historical durability at the named commit, never
  authorization to proceed under current governance.

All shared callers were inspected:

| Caller | Final authority check |
| --- | --- |
| Historical pin/result | Shared write check, exact commit verification, bracketed current read, bracketed lifecycle GET |
| Historical physical catalog | Shared write check and existing post-verification authority check |
| `CurrentCatalog` | Existing check after initial read/no-op; shared check after a write |
| Family resolution | Checked immutable read on each attempt/no-op and shared check after a write |
| Cohort plan | Shared write check and final governance check after durable reread |
| ACS prepared promotion record | Shared write check and final governance check after durable reread |

The helper's SHA return contract, object equality rules, destination blob CAS,
single-parent commits and non-force ref updates are unchanged. No non-ACS
persistence path gains a governance requirement. Previously adopted plans and
authorizations still reject code/configuration/runtime drift without exceptions.

New regression cases replay all three adversarial reproductions, including real
reconstructed shared ACS capabilities. They test changes during the second/current
read, governance drift and replacement during the final lifecycle GET with
retrieval/build/publication callbacks forbidden, shared cohort/family ambiguous
PATCH reconciliation, family transient/no-op recovery, cohort/prepared existing
record rereads, head advancement during reconciliation verification, and both
diagnostic under-reporting sequences. Existing B3/B5-shaped, publication recovery,
and multi-vintage idempotency cases remain required.

Concurrency limits remain: checks describe authority at their completion; no
sequence of remote reads can prevent a change immediately after the last check.
Subsequent authoritative mutations still rely on a freshly validated parent and
non-force ref fencing. Changed snapshots fail closed rather than silently
rebasing or broadening authorization. A durable object can remain after execution
fails; reconcile it before recovery. Process termination can still prevent a
failure artifact from being written. No live acquisition is needed to validate
these corrections.

Final correction validation: the full repository suite passed **730 tests** with
138 existing pandas deprecation warnings (85.42 seconds). The final focused
readback/shared-reconciliation suite passed **106 tests** (16.10 seconds). The
original three adversarial probes are retained as passing regression cases;
superseded snapshots cannot start provider retrieval, shared stale-governance
successes now raise, and both diagnostic boundaries retain the correct evidence.
Original B3/B5-shaped readback, partial publication, and idempotent recovery tests
also pass. Assertions enforce one PATCH per ambiguous write and unchanged
release/publication counts during recovery. Protected `config`, `artifacts`,
`data`, and `docs/audit` trees remain identical to the base table above.
