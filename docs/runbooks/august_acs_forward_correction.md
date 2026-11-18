# August historical ACS forward correction

This adapter is limited to the accepted August ten-source baseline and the exact
published historical ACS candidate/resolution bound in
`jobs/monthly_refresh/august_acs_correction.py`. It reuses existing Source Set v2,
canonical v2 supersession, assembly, immutable release publication, production ACS
verification and current-main CAS helpers. It does not acquire or rebuild ACS.
Independent validation reconstructs the published historical lineage as required
by the existing contract; no replacement ACS package is emitted.

The inspected predecessor is:

- Source Set `source_set__2026-08__v2__b429053765fb9fed`.
- Canonical `market__2026-08__r2__7793c3e80612bf80`.
- ACS `src__acs__2024-12__r1__366cf814efe424ef`.

The replacement is `src__acs__2024-12__r1__7bad02240d41e76c`, package SHA-256
`a20d1f34d1b8e8d867dfcdc9beabca3ca0977f9f1b3ed97459cccd06411da869`, resolution
`acs_family_resolution__9bb174c384bc46be9de6b3e0de367493806905b8ca4f5db487dabed60af3703f`.
The other nine entries (all fields, not only IDs/hashes) and BPS lineage are copied
exactly. Current configuration hashes are explicit successor provenance. Canonical
revision 3 names revision 2 as its predecessor; non-ACS facts are compared exactly
against the downloaded accepted database. Serving stays on its existing immutable
predecessor until a separately authorized serving operation. Macro is untouched.

## Atomicity and authorization

Ordinary cohort promotion and the existing FRED-unemployment correction advance
pointers in recoverable separate catalog commits. They support forward recovery,
not automatic rollback. Neither is reused as an ACS acceptance mechanism.

All accepted pointers live in ONE catalog JSON. This adapter computes the full
successor value and calls existing `acs_authoritative_write.write_current()` ONCE
with the current blob SHA and governed main parent. One non-force ref advancement
exposes Source Set, canonical and ACS together. The other nine source pointers,
serving pointer, immutable records and every other catalog field are preserved
(except the existing CAS bookkeeping field). Mixed old/target accepted maps fail
closed. No partially accepted state is a supported recovery state.

Validation is not permission to publish. The two authorization strings below are
existing-style exact-content acknowledgements, not owner-identity proof or
self-issued governance approval. The operator must obtain genuine independent
review and explicit human authorization outside the adapter, first for the exact
review bundle's preparation, then for the exact prepared correction's acceptance.
A token appearing in a report is not a recorded human decision. Installing this
code or generating a review does not authorize either operation.

## Execution prerequisites

First review and merge the implementation separately. No production operation is
authorized by the implementation task. Run from a clean checkout of production
main containing this adapter, with repository dependencies installed and your
normal authenticated `GITHUB_TOKEN` set without printing it. Read-only validation
needs repository contents/release read access. Preparation/acceptance require
contents write and permitted non-force advancement of main; publication also
requires release/asset creation/upload. Do not weaken branch protection.

All output files and the retained bundle must be outside the repository. Validation
constructs candidates once in temporary storage and retains the exact two TARs
plus sealed `review.json` in `--bundle-dir`. Its GitHub transport rejects every
non-GET call. Preparation publishes verified copies of those retained bytes;
acceptance downloads the exact published packages. Neither write mode assembles
DuckDB: both query candidate facts read-only and independently check ACS lineage,
the nine unchanged sources, input metadata, schema and counts. It never writes a
resolution record, readiness, governance or production database.

```sh
: "${GITHUB_TOKEN:?Set your authenticated GitHub credential without printing it}"
export ACS_CORRECTION_WORK="$(mktemp -d "${TMPDIR:-/tmp}/august-acs.XXXXXX")"
python -m jobs.monthly_refresh.august_acs_correction_hosted validate \
  --bundle-dir "$ACS_CORRECTION_WORK/reviewed-bundle" \
  --output "$ACS_CORRECTION_WORK/review.json"
```

Retain both `review.json` and the complete `reviewed-bundle/` unchanged. Missing,
changed or mismatched TARs are rejected before any preparation write. The review
binds both TAR SHA-256 hashes; a regenerated DuckDB is never their replacement. Review its exact predecessor commit, complete
accepted map (including serving), all existing catalog records, protected August
records/readiness hashes, production governance hash, configuration/dependency
hashes and both proposed successor package commitments. A fresh validation is not
a substitute for a retained partially executed review.

### Preparation / publication — separate human authorization required

After independent review and explicit authorization, supply the EXACT
`preparation_authorization` string from `review.json`:

```sh
read -r ACS_PREPARATION_AUTHORIZATION
python -m jobs.monthly_refresh.august_acs_correction_hosted prepare \
  --review "$ACS_CORRECTION_WORK/review.json" \
  --bundle-dir "$ACS_CORRECTION_WORK/reviewed-bundle" \
  --authorization "$ACS_PREPARATION_AUTHORIZATION" \
  --output "$ACS_CORRECTION_WORK/prepared.json"
```

Side effects: append review-bound operation intents/observations by Git CAS at
`config/acs_august_corrections/monthly_cycle__2026-08__a9e022a980d29cd7.publication.json`,
publish/reuse the retained inactive Source Set and canonical TARs, append only
those two catalog records, then write a create-once immutable record at
`config/acs_august_corrections/monthly_cycle__2026-08__a9e022a980d29cd7.json`.
No accepted pointer, readiness, historical plan/promotion/correction/resolution,
serving or Macro value changes. Both packages are remotely downloaded and
verified. A changed accepted map, protected evidence, governance, unrelated main
advance or unauthorized catalog addition stops the attempt. Only this review's
two catalog appends, append-only publication journal and exact correction record
may advance main. Journal entries are operational evidence, not human approval.

### Acceptance — separate human authorization required

Review `prepared.json`, both immutable remote receipts/packages and the complete
three-pointer delta. Only after explicit authorization supply its exact
`acceptance_authorization` string:

```sh
read -r ACS_ACCEPTANCE_AUTHORIZATION
python -m jobs.monthly_refresh.august_acs_correction_hosted accept \
  --record "$ACS_CORRECTION_WORK/prepared.json" \
  --authorization "$ACS_ACCEPTANCE_AUTHORIZATION" \
  --output "$ACS_CORRECTION_WORK/accepted.json"
```

No local candidate bundle is required for acceptance. The exact durable prepared
record, its complete publication journal and the published immutable package
identities are required. This independently validates the published ACS and successor inputs,
verifies both remote packages and the exact immutable prepared record, rechecks
production governance/evidence/accepted predecessor, then performs the single
catalog commit. It does not modify the nine other source pointers, serving,
Redfin consumption, physical-state registry, Source Set package contents or Macro.

## Failure and recovery

- Validation failure: no production writes. Resolve the mismatch, never edit a
  report or infer permission. Before any writes, the checkout must match the exact
  reviewed production commit; dirty tracked code/configuration is rejected.
- Preparation interrupted: accepted state remains wholly old. Keep the original
  review and repeat the same authorized prepare command. Already cataloged exact
  packages are revalidated and reused; candidates are never reassembled. The create-once record rejects different
  reviews or publications. Do not replace historical August files.
- Lost release create/upload/finalize response: a review-bound intent is committed
  and verified BEFORE each mutation. Its observed numeric release/asset identity is
  persisted before progressing. A restarted or concurrent caller cannot repeat a
  pending mutation. Known release IDs are queried numerically; listing/tag absence
  never licenses their replacement. Unknown creation outcomes require exhaustive
  authenticated discovery and a unique authoritative numeric object. If discovery
  cannot establish the outcome, STOP, even if listings are empty. Do not delete or
  reset the journal to retry. Reconciliation of assets verifies retained hashes.
  Conflicts stop recovery; no asset is overwritten or deleted. The completed
  journal is bound into the immutable prepared record. Concurrent intent claims
  use the existing main/blob CAS, so only one caller can issue each mutation.
- CAS conflict before acceptance: no accepted switch from this attempt. Immutable
  orphan Git objects may exist. Revalidate; never force/rebase or regenerate a
  review to hide drift.
- Lost ref-update response: existing `write_current()` checks the authoritative
  ref and exact commit-bound catalog bytes. The accepted value is wholly old or
  wholly new. If outcome cannot be established it stops. Repeating the same
  acceptance, after reconciling remote state, validates the retained record and
  produces a no-op if the entire target accepted map is already present.
- Postcommit verification failure: do not roll back or dispatch downstream work.
  Retain evidence and reconcile. A mixed map or any unrelated advance is rejected;
  this adapter does not repair arbitrary existing partial production states.

Local fixtures validate the lifecycle and crash boundaries. They are not a claim
that real production packages have been revalidated or any human authorization
has been issued. Real validation and both human gates remain required after code
review/installation. No workflow is added or dispatched by this change.

## Implementation validation

The construction timestamps use the existing cohort adapter's fixed logical
August clock, not a claim of August-time execution or provider retrieval. Actual
release/publication and Git commit timestamps retain their genuine provenance.

Verification: **192 focused tests passed** (the original 180 plus 12 new cases),
including **45 correction tests**. Syntax and `git diff --check` passed.

The two reviewed defects are covered by a real-assembler lifecycle in separate
validation/preparation/acceptance directories with actual package validators,
shared publisher and fenced CAS. Assembly is forbidden in the write-mode test;
acceptance is also tested after moving the local bundle away. Missing/altered TARs
and rehashed databases with changed ACS or non-ACS facts fail before writes.
Lost create/upload/finalize acknowledgements, hidden listings, exact retries and
concurrent intent claims are covered with offline remotes. No actual production
command has been executed. Run the focused suite before review:

```sh
python -m pytest -q tests/test_august_acs_correction.py \
  tests/test_acs_g_cohort_integration.py tests/test_acs_write_readback.py \
  tests/test_august_fred_unemp_forward_correction.py \
  tests/test_acs_historical_materialization.py
```

The previously confirmed 85 baseline failures/errors at main
`67d92fd4882e73c608eb1f00857f54c678f06ff4` are unchanged and outside this fix.
They were not revisited or absorbed into the implementation. Real production
validation, independent review and the two separate human authorizations remain
required after implementation review/merge.
