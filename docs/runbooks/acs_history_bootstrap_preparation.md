# Initial ACS physical-history preparation (Phase 1)

This adapter prepares and independently verifies local candidates. It does not
publish, approve acceptance, install governance, update accepted pointers, invoke
readiness, build a Source Set/canonical/serving object, or run Macro. No network
mutation or provider acquisition entrypoint exists. The only remote requests are
read-only current governance/catalog verification through existing trust paths.

## Supply the real inputs

Use current reviewed main plus this implementation. The exact baseline is 19
ACS1 acquisitions (2005–2024 except approved standard-product 2020 nonpublication)
and 16 ACS5 acquisitions (2009–2024). The 37 catalog records include two alternative
2024 parents that must not be substituted. The 34 recovered original TARs remain
Mac-local; add the newly acquired ACS5 2023 original package to the same directory.

The new package is
`src__census_acs5__2023-12__r1__4de4cec871ace0a1.tar`, release `435428291`, asset
`713988353`, SHA-256
`4cc76ae8ca66e1e1d9cac272434147e1ee21a8110bee216d826058e67dcf83d8`.
Retrieve that existing immutable asset only if absent; do not run acquisition or
rebuild TARs from extracted caches. For an authorized Mac environment:

```sh
ACS_TARS="$HOME/acs-readiness-review/discovery-1/retrieved-packages"
ACS_BOOTSTRAP="$HOME/acs-readiness-review/physical-bootstrap"
mkdir -p "$ACS_BOOTSTRAP"

python -m jobs.monthly_refresh.acs_history_bootstrap \
  --repository-root "$PWD" --template --package-root "$ACS_TARS" \
  --output "$ACS_BOOTSTRAP/input-template"
```

Inspect `input-template/inputs.json`. The template lists exact artifact/content/
package/data identities; it is an explicit review input, not a selection authority.
Non-2024 catalog ambiguity prevents template creation. It fixes the two original
August 2024 parents and ignores other files in the package directory. Verify all
35 path mappings yourself. If retained filenames differ, edit only their paths;
no code chooses a newer acquisition or substitutes a parent. No synthetic ACS1
2020 package, rows or geography eligibility classification is permitted.

Set `artifact_created_at` to the actual candidate preparation time once. This
new-candidate timestamp is not original acquisition provenance. Preserve the
resulting input bytes for deterministic replay, rather than refreshing time on a
retry. For example, after reviewing the template:

```sh
python - "$ACS_BOOTSTRAP/input-template/inputs.json" <<'PY'
import json, sys
from datetime import datetime, timezone
from pathlib import Path
p = Path(sys.argv[1])
spec = json.loads(p.read_text())
if spec['artifact_created_at'] is not None:
    raise SystemExit('Creation time already fixed; retain it for replay')
spec['artifact_created_at'] = datetime.now(timezone.utc).isoformat()
p.write_text(json.dumps(spec, sort_keys=True, indent=2) + '\n')
PY
```

Provide GITHUB_TOKEN using the existing authorized environment, without printing
it. Production preparation has no local/cached-governance fallback:

```sh
python -m jobs.monthly_refresh.acs_history_bootstrap \
  --repository-root "$PWD" --prepare \
  --inputs "$ACS_BOOTSTRAP/input-template/inputs.json" \
  --output "$ACS_BOOTSTRAP/preparation-1"

python -m jobs.monthly_refresh.acs_history_bootstrap \
  --repository-root "$PWD" \
  --verify "$ACS_BOOTSTRAP/preparation-1/bundle.json" \
  --inputs "$ACS_BOOTSTRAP/input-template/inputs.json" \
  --output "$ACS_BOOTSTRAP/verification-1"
```

Exit 0 means preparation/verification passed, not installation readiness. Exit 2
means missing inputs or inaccessible authority; exit 1 means invalid/contradictory
evidence. `verification.json` always distinguishes implementation availability,
real 35-package verification, local file presence, production-loader status and
remaining gates. Invalid input JSON and partial candidates do not become a pass.
Outputs must be new directories outside the repository. Failures retain partial
local output for inspection; rerun into a new directory. Verification never
replaces a retained candidate or original acquisition.

## Verification contract

`reconcile_inventory()` uses existing `ACSGovernance.scope`, original TAR
extraction, acquisition validation, complete provider accounting and
`acs_history.reconstruct`. Exactly one explicit parent is required per expected
source/vintage, with exact catalog/content/data/package identities. Ordinary
vintages retain independent-provider validation; every historical pin must bind
the exact reviewed snapshot. ACS5 2023 also matches its durable repository pin
and all four exact HTTP 204 receipts (438 values, eight no-content cells).

The installed ACS1 2020 gap must have current approved membership and standard
whole-vintage nonpublication classification. It contributes no acquisition parent
or observations. It does not authorize experimental ACS1/ACS5 substitution or
geography ineligibility claims.

For the two original 2024 acquisitions the adapter calls the existing production
compatibility consumer, with unchanged original reports and original TARs. It
independently verifies authority and complete qualification evidence. Original
`manifest.git_sha="unknown"`, separately recovered execution evidence, process-
attested absence, 67 ACS5-only geographies and four jointly missing geographies
retain their existing assurance limitations. Missing observations are not filled.

`prepare_bootstrap()` reuses `acs_historical_inventory_v2`,
`materialize_current(mode="BOOTSTRAP")`, and `verify_state`; no duplicate inventory
or physical-state schema is introduced. The state is reconstructed from all
original immutable parents, never a prior state's rows. Fixed creation time and
unchanged runtime/inputs produce deterministic local candidate bytes.

`verify_bootstrap()` starts again from original TARs/evidence. It independently
verifies candidate TARs, semantic rows and complete lineage, and checks that loose
candidate directories exactly match their retained package bytes. A retained pass,
changed hash, missing second product, substituted package or edited acceptance
delta cannot bypass reconstruction. Test-only fixtures remain test_only and
cannot satisfy the production CLI. Hashing a bundle does not establish authority.

Production verification loads its own current-main governance capability, compares
local registry evidence to that live snapshot, independently reads the catalog at
current main, and rechecks the fence after parent/candidate verification. Changed
registry/catalog or concurrent main movement stops preparation. The final report
is preparation-time evidence only; installation needs a new live check later.

## Inactive acceptance contract

`bundle.json` is `acs_history_bootstrap_review_v1`, an inactive review wrapper,
not a new state schema or approval. Its full semantic bundle identity binds exact
parents, evidence hashes, existing inventory/state identities, candidate artifact/
content/package identities, fixed build provenance and governance/catalog baseline.
Candidate paths are fixed relative paths inside the bundle directory.

`propose_acceptance()` emits only the following proposed delta:

- Add both existing-schema state records under `states[state_id]`.
- Set both `current_state[source_id]` pointers in the same atomic registry change.
- Require the exact baseline registry hash and initially empty state/pointer maps.
- Preserve scopes, gap evidence, current_scope, compatibility records/pointer and
  every other registry field. No accepted catalog pointers change.

No approval provenance is inserted into semantic state records: doing so would
change the existing reconstructable state identity. Genuine human provenance must
be retained with the future exact bundle/installation review using reviewed-main
controls. Local proposal hashes, CLI flags and copied reports cannot self-approve.

Installation readiness remains blocked even after all 35 parents and current
production governance pass. Phase 2 must separately obtain human authorization,
publish/verify the exact immutable candidates and catalog receipts, recheck live
scope/configuration/governance and acquisition identities, then review installation
of both states/pointers together. No candidate release/receipt exists in Phase 1.
A subsequent installation must compare expected registry/catalog identities,
verify immutable publication, and use one reviewed atomic update, rejecting
concurrent changes rather than automatically rebasing. This is a specification,
not an implemented writer.

The existing `write_current()` helper must not be used unchanged to install the
registry: its final old-snapshot check would classify its own registry update as
supersession. Prefer the separately reviewed installation PR, or separately review
a future narrow installer with immutable-commit verification and fresh production
reload after its own successful atomic update. Do not weaken the generic helper.

If only local candidates exist, no production acceptance occurred. If Phase 2
later encounters partial publication, it must reconcile/reuse exact verified
immutable bytes; the Phase 1 bundle cannot claim publication or authorize retry.
An already installed or one-product-only state rejects this initial bootstrap;
updates/recovery require their separately reviewed lifecycle. Phase 1 supplies no
publication recovery execution. Readiness corrections remain a separate PR;
`acs_readiness.py` and every downstream activation boundary are unchanged here.

## Original qualification catalog versus live baseline catalog

The two original report identities bind the complete original catalog bytes,
SHA-256 `2b49256a75802f8290094c5841e6015ced4fc09cfc6dd7680b4def267978bfa9`.
The ACS5 2023 addition legitimately changed today's catalog. The adapter retains
the exact original catalog from the existing qualification Git anchor in its
temporary verification workspace and passes those bytes to the existing bounded
compatibility consumer. It separately requires both original 2024 records to be
exactly equal in the live catalog and validates all 35 current parent identities
against the freshly read live catalog. Both catalog digests are bound into the
review bundle. There is no local-catalog approval shortcut: live governance and
current catalog checks remain mandatory, and unchanged report-file digests and
complete qualification evidence must pass the existing consumer. No original
report is reserialized or relabeled with today's catalog digest.

Phase 2 publication legitimately advances the catalog. That future phase must
retain the original review bundle, verify unchanged acquisition tuples, bind the
new exact publication records and fresh catalog snapshot as additional installation
evidence, and obtain the corresponding reviewed installation decision. Phase 1
intentionally rejects a changed catalog baseline; do not edit an old bundle to
silently absorb the advance or treat it as publication-recovery authorization.

## Implementation validation versus real-input verification

The new adapter's tests use a complete 35-parent **test_only** fixture inventory,
including independently reconstructed rows/lineage and simulated read-only HTTP
boundaries. Those tests are not production package verification. Independently,
all 33 ordinary durable repository pins match their reviewed provider evidence;
both available original 2024 TARs match the existing materialization envelope,
and both reproduce the unchanged qualification reports with their anchored
catalog evidence. These partial checks do not establish the complete baseline.

`docs/audit/acs_history_bootstrap_phase1_verification.json` retains the actual
cloud CLI result: two local original TARs present, zero fully verified baseline
parents, 35-package verification not established, and production-loader transport
unavailable. No candidate bundle was produced. Supply the missing 33 originals
and obtain live governance access in the owner environment before claiming actual
35-package preparation. Human approval, immutable candidate publication and atomic
installation remain separate gates even after that preparation succeeds.

A pre-existing deterministic scope-proposal regression also occurs on clean main
`7327cbce27141cc88ad2523aeb82c7cc199f9807`: the new ACS5 2023 catalog record makes
archived `census_acs5.scope.proposal.json` acquisition coverage and the inactive
initial-registry proposal's catalog/scope digests stale. This Phase 1 change does
not rewrite those historical proposals or suppress the test. Report it separately
from adapter failures; a green full suite requires the bounded metadata correction
through its own review. No approved scope or governance record needs to change.

Local validation results: **322 focused tests passed** (including 28 new adapter
cases). Full suite: **1,185 passed, four existing-main failures**, 138 existing
warnings. All four failures were reproduced independently in an untouched worktree
at the baseline commit:

- `test_repository_discovery_never_retrieves`: old 34-parent discovery expectation.
- `test_deterministic_checked_in_artifacts`: archived metadata/catalog digest drift.
- `test_scope_and_coverage_are_separate`: still expects ACS5 2023 acquisition missing.
- `test_acs5_proposal_remains_identical_to_original_303`: compares present coverage
  against the original pre-acquisition proposal.

These failures remain visible and unmodified; they require a separate bounded
baseline-test/archived-metadata review. Module syntax and whitespace checks pass.
