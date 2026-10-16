# ACS historical physical acquisition — Phase A

This tool acquires bounded, independent, ordinary ACS1 or ACS5 single-vintage
parents. It does not approve historical governance, bootstrap a state, resolve a
family, plan a cohort, activate a catalog pointer, promote canonical/serving
objects, or run Macro. The default is read-only inventory/preflight. The manual
workflow has no schedule or workflow-call trigger. Do not dispatch it as part of
implementation review.

## Call graph

```text
manual workflow: preflight (contents:read)
  acs_backfill.main -> repository_identity + main code/config authority
    -> GitHubCatalogCAS.read -> GitHubReleaseArtifactResolver.resolve
      -> reconcile (validate parent manifest, rows and exact package)
    -> historical.validate_evidence (including exact omission receipts) -> make_plan -> plan.json
manual workflow: authorize (contents:read)
  main -> recheck main authority and inventory -> authorize(exact reviewed plan_id)
    -> separate authorization.json + unchanged plan.json in run artifact
manual workflow: execute (contents:write, actions:read)
  download successful main authorize-run artifact -> verify run + immutable ZIP digest
    + exact archived plan/authorization equality + hosted execution context
  execute_batch (sequential, at most five vintages, stop at first failure)
    -> recheck main code/config/gap and scoped inventory
    -> verified package reuse -> publication_result -> governed_record -> cycle-result store
    OR execute_product
      -> discover_persist_execute -> exact_pin -> acquire_snapshot (one exact year)
        -> historical.validate_snapshot -> snapshot_to_pin -> FencedPinStore
      -> checked retrieval on resume -> verify_member_bytes
      -> historical validation -> acs_monthly.candidate (stable pin timestamp/code)
      -> exact_publish -> existing publish_candidate
        -> deterministic package -> GitHub release prepare/upload/verify/finalize
        -> FencedPhysicalCatalog (existing ACS main-ref fence) -> resolver verification
      -> publication_result -> governed_record -> FencedResultStore
```

`historical.preflight` is an independent, injected-transport provider reader:
exact discovery dataset -> exact variables endpoint -> exact requested geography
responses -> complete variable/member accounting. It has no implicit HTTP
transport. Unit fixtures never contact Census. The hosted default consumes
previously reviewed provider evidence from a repository JSON file; it never
fetches Census data merely to produce an inventory plan.

## Preflight and review

For separately authorized bounded hosted provider observation capture, see
[ACS provider evidence](acs_provider_evidence.md). Its output remains a review
candidate; main evidence adoption, planning and acquisition authorization are
distinct boundaries.

1. Use the Cloud checkout of `chineduezebuiroh/realestate-intel`. Verify baseline
   `282ba2d4dcb0fa18d1e945fc5f3cb16957d9da32` is an ancestor. Hosted execution code
   and authority must originate from main.
2. Select one exact `census_acs1` or `census_acs5` and one to five unique years
   (2005–2100 input bounds are syntax bounds, **not** provider availability).
   There is no latest-year discovery fallback in historical execution.
3. Prepare official provider evidence separately, only when provider reading has
   been authorized. Use `sources.census_acs.historical.preflight` with an injected
   transport, `geography_plan()` and the exact year. Credentials belong solely to
   transport parameters; never place them in a URL or evidence. The result is
   keyed by its year in an evidence JSON map, for example `{"2019": <result>}`.
   Store reviewed input at `config/acs_backfill_reviews/<safe-name>.json` through
   a separately reviewed change. Hashing alone does not authenticate provider
   evidence: reviewers must verify official provenance, exact URLs and facts.
4. A manual `preflight` run uses read-only GitHub authority, with an optional
   `evidence_file`. Without official evidence, missing acquisitions retain an
   explicit verification requirement and cannot be authorized. Download the
   `acs-backfill-preflight` artifact and review `plan.json`.
5. Review **every** parent identity, package hash, vintage, geography, variable
   accounting, exclusion, official dataset identity, code/configuration identity
   and verification requirement. Never choose a convenient parent when multiple
   acquisition IDs exist for the same year. An inaccessible parent is unknown,
   not missing. A known gap can only be read from an existing main-approved
   `ACSGovernance.scope`; this tool has no approval writer.
6. Put the exact reviewed plan in a safe review JSON path on main via the normal
   external review process. Do not edit it after hashing. Review files are not
   executable configuration; changes to them do not change code identity.

CLI planning (only when separately authorized to read GitHub):

```bash
python -m jobs.monthly_refresh.acs_backfill \
  --source-id census_acs1 --vintages 2019 --batch-size 1 \
  --evidence config/acs_backfill_reviews/provider-2019.json \
  --workspace /tmp/acs-preflight --output /tmp/acs-plan.json
```

The command defaults to preflight; it has no publication or provider-acquisition
path. Hosted planning requires checked-out main HEAD. New untracked Python files
are included in local code identity for review; production uses reviewed tracked
code. Dynamic pin, catalog, and result writes do not invalidate code identity.

## Separate authorization and execution

Run manual `authorize` with the safe `plan_file` and its complete `plan_id`.
Authorization rechecks scoped inventory, code and main configuration. It writes
no production state. It produces `acs-backfill-authorize`, containing unchanged
plan bytes and a distinct authorization binding plan, repository identity, actor
and the `acquire_physical_only` action.

Run manual `execute` only after explicit acquisition authorization, providing
that successful authorize run's ID and the exact reviewed plan ID. The workflow
uses the named artifact from that run. The CLI verifies successful main-branch
workflow-dispatch provenance, actor, authorization-run code and repository/workflow identity.
It independently reads that run's unique, unexpired authorization artifact,
verifies its immutable ZIP SHA-256, safely parses exactly two JSON members, and
requires exact semantic equality with the supplied plan and authorization.
Rehashing a modified plan or naming a successful unrelated run grants no rights.
The write CLI always requires the dedicated main workflow context; removing or
setting `GITHUB_ACTIONS` cannot disable authorization verification. Product,
vintage, evidence and batch overrides are rejected on authorize/execute. Input
values are passed through environment variables, quoted arguments or a Python argument
list; review paths forbid traversal and symlinks. Only execute receives the
Census secret or contents write permission. There is no force/overwrite option.

The plan also binds the Python executable/version and pandas, NumPy and PyArrow
versions plus installed wheel RECORD checksums, with actual declared installed
file bytes checked against those checksums before identity acceptance. If the
serialization runtime changes, restore the reviewed implementation environment before retry; a new compatible
version does not authorize regeneration of an old immutable identity.

Execution independently verifies content hashes and authority; main ancestry is
insufficient if code or governed configuration changed. It rejects incomplete
GitHub comparison evidence. Main code/configuration/gap authority is checked
before inventory, pin persistence, publication and result persistence. Catalog
CAS and immutable pin/result stores retain their existing collision semantics
and paths/schemas. Physical pin, catalog and result writes now reuse PR #281's
single-parent Git tree/commit fence and update main with `force=false`. A main
advance between authority validation and ref update rejects the write without
rebasing. Scoped reconciliation is rechecked before acquisition/build/publication
and at every main-tip fence. Catalog registration independently rejects competing
same-product/vintage acquisition IDs, including concurrent monthly acquisitions.
This is not a new multi-object transaction: a concurrent remote policy change
may leave a staged immutable release or orphan Git objects, but cannot advance
the corresponding authoritative main record. Resume only under the same valid
reviewed identity. No historical approval capability or registry is created. The fenced source
stores reject writes outside the plan's product/vintage/cycle scope; catalog
registration explicitly forbids source-set, canonical and serving object types.

Each cycle is `acs_backfill__<source>__<year>__<hash-of-plan-id>`. A different plan
gets a different isolated cycle. A batch runs sequentially, stops on its first
failure, and returns no successful result for an unfinished vintage. Completed
vintages remain recoverable by the same plan. Do not create a new plan just to
bypass a failed immutable identity.

## Historical validation and unresolved facts

Ordinary datasets must have exact components `["acs", "acs1"]` or
`["acs", "acs5"]`, the requested vintage and an exact Census dataset identifier.
Provider identifiers are opaque: they may differ from the API endpoint, but then
must carry an exact official advertised `accessURL` for that product/vintage.
Any advertised variable/geography links must also agree exactly; no ID naming
convention or substring confers compatibility. Ambiguous ordinary discovery
entries fail even if only one has plausible endpoint metadata. Experimental ACS1 substitutes, duplicate dataset identities and substring
matches fail. Each selected historical variable must have label, concept and
numeric predicate metadata. Response header names must be exactly NAME, both
physical variables, and requested geography columns; order may vary. Missing,
extra or duplicated columns fail. Returned state/county/us/metro codes must
match the request. All planned members and variables must be accounted for once.

Provider nulls (`null`/JSON null), the existing three physical-contract sentinels,
and observed empty-body HTTP 204 member responses require explicit
`acs_historical_omission_evidence_v1` receipts. Every receipt binds exact
source/product/vintage, canonical geography, variables, safe request URL, HTTP
status, classification and decoded response SHA-256; its full semantic identity
is hashed. The reviewed preflight and receipt IDs are also bound into the existing
snapshot pin evidence. Canonicalization omits only documented unavailable cells
or members, preserving unequal geography coverage without synthesis.

A receipt is a recorded acquisition observation, **not** an eligibility claim,
whole-vintage gap or governance approval. The inherited snapshot status name
`provider_ineligible_no_content` is retained for compatibility; the historical
receipt/accounting classification is explicitly `provider_no_content`. The
historical transport checks actual status and requires an empty body for 204.
Request/transport failures, non-200/204 statuses, parsing/schema failures,
missing responses/headers, invalid/unknown sentinels and undocumented omissions
cannot be converted into receipts. Missing variables in response headers remain
schema failures even when an omission receipt is supplied. Metadata endpoints
require 200; a 204 there cannot substitute for provider identity/variable evidence.
Current monthly behavior is unchanged.

No historical threshold, geography crosswalk, missing variable replacement or
experimental-year compatibility is invented. PR #281 explicitly keeps
geography-specific absence as acquisition evidence rather than a whole-vintage
gap, so offline fixture implementation is compatible. Whole-vintage/all-metric
absence still fails closed and needs separately approved gap governance. This
implementation creates only synthetic test fixtures; it approves no actual
production omission evidence or whole-vintage gap (including ACS1 2020).

A snapshot must match the persisted physical source/product/vintage. The pin
binds byte hash, request identity, membership and retrieval timestamp. Execution
also compares complete response identity with the reviewed preflight; later
provider changes require a new review before pin persistence. Resume requires
the exact persisted bytes, not new compatible-looking provider values.

## Publication failure and recovery matrix

| Boundary or failure | Behavior and operator recovery |
| --- | --- |
| Before durable pin | No candidate publication occurs. Retry original plan; reacquire exact year and compare reviewed evidence. No artifact identity was published. |
| Pin written, response lost | Resume reads the immutable pin, never rediscovering a newer vintage. Restore/retrieve exact original snapshot bytes. |
| After pin, before release | Build with pin retrieval timestamp as `artifact_created_at`, reviewed code commit and bound configuration. Exact package bytes must match across retries. |
| Release created, preparation response lost | Retry exact tag through the common publisher; never create an alternate identity to escape conflict. |
| Partial upload or upload response lost | Download existing exact asset and compare SHA-256. Exact bytes reuse; incomplete/different bytes block recovery. Preserve remote release and compare original package/member hashes. |
| Finalized release, no catalog | Retry identical package through common publisher; remote verification precedes catalog CAS. Finalized releases are not overwritten or deleted. |
| Catalog/main-ref CAS conflict | Single-parent `force=false` update rejects concurrent main advance. Re-read authority and catalog on retry; do not rebase. Exact registered identity is reusable; a different record/package fails closed. No pointer is advanced. |
| Catalog registered, no cycle result | Reconcile and resolve the verified package. Compare remote lineage pin, cycle, code, config, creation timestamp and package identity; persist the common governed result without fetching provider data. |
| Existing verified package/result | Resolve exact package and revalidate immutable facts. Result store treats exact semantic repeats as no-ops; contradictory result identity collides. |
| Existing artifact ID, different package bytes | Stop with artifact ID/local package hash diagnostics. Never accept equality of data/content hash as equality of package bytes; preserve the remote object. |
| Provider drift on resume | Raw pin hash mismatch stops before publication; restore the originally pinned snapshot. A new year or approximate replacement is prohibited. |
| Multiple existing parents | Explicit conflict; independent review must reconcile identities. No automated winner. |
| Historical geography variability/204 | Only an exact reviewed receipt for an observed empty 204 can omit that requested member. Undocumented absence is a verification requirement. No synthesis, eligibility inference or fallback year. |
| Missing metric/metadata | Missing headers/metadata fail; null/sentinel cells require exact explicit receipts and complete accounting. |
| Invalid provider response | Malformed shape, wrong geography, duplicate/unexpected headers, invalid/nonfinite/unknown sentinel values fail before pin publication. |
| Governance/code/configuration drift | New review required; stop remaining batch members and retain immutable published facts for diagnosis. |
| Inaccessible catalog/package/provider | Stop or classify unknown, never convert access failure to absence. |
| Secret-bearing transport errors | Only typed, allowlisted historical request diagnostics survive the CLI wrapper; no URL, parameter map, header, body, credential or original exception is recorded. |

The generic artifact ID excludes `artifact_created_at`, while manifest/package
bytes include it. Phase A fixes this specifically for historical acquisition by
using the immutable pin timestamp and rejects existing mismatched package bytes.
Generic publication schemas and current monthly timestamps remain unchanged.
Library/dependency encoding changes that prevent identical package rebuilding
must fail closed; verified remote package reuse is preferred once cataloged.

## Offline review and Phase B handoff

Run fixture tests with outbound network connections disabled:

```bash
PYTHONPATH=. pytest -q tests/test_acs_backfill.py tests/test_acs_c_physical_integration.py \
  tests/test_acs_historical_materialization.py tests/test_acs_boundary_hardening.py
```

Additional existing smoke coverage includes 170 (Release transport/CAS), 195
(durable inputs), and 196 (BPS hosted registration). Run the full `tests` suite
and `git diff --check`. Do not use live verification scripts or production
acceptance fixtures. `tests/fixtures/census_acs_historical/provider.json` contains
synthetic response metadata/geographies, not claims of official historical facts.

The preceding feasibility report is not in this checkout; the matrix above
covers the failure modes explicitly supplied in the implementation request.
Compare against the original report before approving completeness of its matrix.

Phase A omission support is implemented and pressure-tested offline. Lack of
live official observations is an operational prerequisite, not an implementation
or PR #281 contractual blocker.

Phase B must separately establish approved historical product scope and official
gap evidence, retain reviewed acquisition omission receipts, reconcile all acquisition parents, and feed verified
immutable inventory into PR #281's existing governed materialization contracts.
It must separately authorize state acceptance, family resolution and downstream
promotion. Phase A success does not authorize any of those steps. Retain exact
plans, authorizations, pins, packages, catalog records and cycle results as the
handoff evidence. Nothing in this implementation performs production bootstrap.


## Historical transport recovery and acquisition deadline

Historical acquisition uses at most **three total attempts per logical request**.
Only connection failures, timeouts and HTTP **429, 500, 502, 503, 504** are retried.
Backoff is 1 then 2 seconds; numeric or HTTP-date `Retry-After` may increase that
wait to at most 10 seconds. Invalid, negative or nonfinite header values cannot
extend the bound. Redirects are not followed: 3xx responses fail permanently,
matching the evidence-capture endpoint boundary and avoiding hidden requests.
HTTP 400/401/403/404 and all other unlisted statuses are not retried. TLS/certificate
errors, malformed JSON/payloads, metadata mismatches, invalid 204 bodies and
reviewed snapshot drift also fail without retry. Valid empty 204 behavior and
existing omission, metadata, snapshot, pin and publication validation are unchanged.
No reviewed-snapshot fallback is introduced.

Each vintage has one **900-second wall-clock acquisition deadline**, covering all
geography requests, decoding, output construction, retry waits and `Retry-After`.
There is no reset per request or retry. Connect and read inactivity timeouts are
each capped at 30 seconds and half the remaining budget. A wait that cannot fit
is rejected; late responses cannot proceed to snapshot validation or pinning.
Socket timeouts alone do not stop an indefinitely trickling body, so a POSIX
`ITIMER_REAL` alarm interrupts in-flight acquisition as well. It is installed only
on the main thread with no existing real-time timer, and the original handler is
restored on every exit. Unsupported contexts fail closed as
`deadline_unavailable`; they do not silently acquire without a deadline. These
requirements match the existing Ubuntu GitHub Actions Python entry point. Five
vintages therefore have at most 75 minutes of acquisition time; control-plane and
publication work are separate. The alarm does not wrap publication or weaken CAS.

`HistoricalAcquisitionFailure` logs only these trusted fields: source/product,
vintage, logical request ordinal (retries do not increment it), request kind,
configured geography ID or null, fixed category, integer HTTP status or null, and
attempts consumed. Dataset/variables requests, if used by the transport, are
identified separately and carry no geography ID; ordinary historical acquisition
continues to make its existing geography requests without adding discovery calls.
The historical wrapper and backfill CLI preserve this typed message in the
Actions traceback, suppressing the original exception chain. No raw exception
text, URL, request parameters, headers, credentials or provider body are logged.

Categories distinguish `connection`, `timeout`, `http_status`, `tls_failure`,
`invalid_json`, `invalid_payload`, `invalid_no_content`, `invalid_response`,
`metadata_mismatch`, `credential_unavailable`, `request_identity`,
`deadline_exhausted`, `deadline_unavailable`, and fixed generic
`transport_failure`/`acquisition_failure` fallbacks. Geography IDs come only from
the governed request plan. A failure remains a failure, never evidence of absence.
An unrecognized error outside the historical adapter retains the fixed redacted
CLI fallback. A sanitized timeout or 503 diagnoses only that request failure;
it does not prove the cause of the earlier opaque ACS5 B2 failures.

### Post-merge ACS5 B2 recovery (separate future actions)

Changing `historical.py` / `acs_backfill.py` changes the bound production code
identity. The old B2 plan `...__ba06f0384549ae1b501c62e957bf496854c46e4be6938b695d48651232349563`
and authorization run `38956749622` **cannot authorize this implementation**.
Dependencies, Python selection, governed runtime configuration, evidence and
geography scope are unchanged. No serialization runtime change is required by
this patch, but each hosted plan still binds and verifies the actual Python
executable and installed library RECORD/file hashes; later drift must fail closed.

1. Externally review and merge only this transport change. Preserve the existing
   adopted ACS5 B2 evidence map unchanged; do not recapture provider evidence.
2. Separately dispatch the existing read-only preflight against merged main:

   ```sh
   gh workflow run acs-historical-backfill.yml \
     --repo chineduezebuiroh/realestate-intel --ref main \
     -f action=preflight -f source_id=census_acs5 \
     -f vintages=2018,2019,2020,2021,2022 -f batch_size=5 \
     -f evidence_file=config/acs_backfill_reviews/provider-acs5-2018-2019-2020-2021-2022.json
   ```

3. Download `acs-backfill-preflight`, verify/review its **new** plan ID, runtime,
   evidence and current scoped inventory, then adopt its exact unchanged bytes
   on main at `config/acs_backfill_reviews/plan-acs5-b2-transport-recovery.json`
   through a separate plan-adoption review. No new ID is generated in this patch.
4. After separate authorization approval, dispatch existing `action=authorize`
   with that `plan_file` and its full new `reviewed_plan_id`. Preserve and verify
   the resulting `acs-backfill-authorize` artifact and authorization run ID.
5. Only after separately authorized execution approval, dispatch existing
   `action=execute` with the same new `reviewed_plan_id` and new
   `authorization_run_id`. Old IDs are not substitutes. Retain all runtime,
   inventory, exact snapshot, pin, package, publication and accepted-pointer gates.
6. Independently verify successful physical release bytes/catalog records and
   unchanged accepted August pointers. If acquisition fails, inspect only the
   sanitized category/context; do not repeat opaque retries or loosen checks.

These are future operational boundaries. This implementation performs no
preflight dispatch, plan generation, authorization, acquisition or production write.
