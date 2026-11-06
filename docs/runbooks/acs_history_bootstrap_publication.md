# Bounded ACS bootstrap publication (Phase 2)

This adapter publishes only the two retained packages in bundle
`acs_history_bootstrap_review_v1__99e0abccc424165f2c5ec6e4f603904087ceff4562f3c0725e0b0aedcd2fd7cf`.
It never installs registry states/pointers, changes catalog accepted pointers,
invokes readiness, acquires data or promotes Source Set/canonical/serving/Macro.
Publication authorization does **not** authorize later physical-state acceptance.

| Product | Artifact suffix | Package SHA-256 | State suffix |
| --- | --- | --- | --- |
| ACS1 | `57a4580db2986248` | `1abc68022f7ad50ced1e734a51e40c6f4cf995979ae3cee65688c316dfd563ad` | `6595317ece328607538fafbd3ac113939876797550f77d72290f9892338365cb` |
| ACS5 | `177a1404c91f6594` | `26e838730b84fb08401a8f9ac189a3982f98f975d403510b907b1b9bb689756d` | `3d2b106bcb9ef914b260857ed6c58593bc19e6bc6ad4328dd5fbdea7907b316a` |

Both artifacts have prefix `src__<source_id>__2024-12__r1__`. States use
`acs_physical_historical_state_v2__`. Production baseline is
`4ef518d37d1ee0da55a19a83dabb205363a97d44`, with registry semantic SHA-256
`87fd4c909fb3b1e314e2646e00fa9a2f60bec70023d3a541b25979f412db7141`.
These constants scope the adapter; they confer no authorization.

## Retained inputs and inactive review request

Run on the owner's Mac using the exact retained Phase 1 input JSON, preparation
directory, original 35 TARs and referenced evidence. Keep the original prepare and
independent verification reports too. Do not edit the input template, bundle,
candidate directories, package bytes or timestamps. Keep the Phase 1 directory
read-only. Use a separate journal directory on durable local storage.

Use the existing project environment with repository requirements installed and
full Git history. Start with the reviewed Phase 2 implementation; no original
acquisition or candidate rebuild is needed. Example paths below are placeholders
for the owner's **existing** retained files, not instructions to regenerate them:

```sh
ACS_INPUTS="$HOME/acs-readiness-review/physical-bootstrap/input-template/inputs.json"
ACS_PREPARED="$HOME/acs-readiness-review/physical-bootstrap/preparation-1"
ACS_PHASE2="$HOME/acs-readiness-review/physical-bootstrap/publication"
mkdir -p "$ACS_PHASE2"

python -m jobs.monthly_refresh.acs_history_bootstrap_publication \
  --repository-root "$PWD" --inputs "$ACS_INPUTS" \
  --bundle-directory "$ACS_PREPARED" --review-request \
  --request "$ACS_PHASE2/request.json"
```

This offline command creates an inactive request, not an approval. It binds the
exact bundle file hash, original input semantic hash, all parent/provider/report/
pin file hashes, both full candidates, original governance/catalog baseline,
inactive two-state delta, and protected code/workflow/requirements/configuration
Git blob identities. It rejects a different bundle or either candidate package.
No production package verification is claimed by request generation alone.

Have the generated JSON reviewed in a small PR at
`docs/proposals/acs_history_bootstrap_publication.json`. Copy its bytes exactly;
do not regenerate it for the current catalog. Implementation code must be on
main before publication. The request PR can change documentation only: code and
protected inputs must remain equal to the request. Retain its exact reviewed head.
An implementation PR merge, request hash, this runbook or an uploaded audit is
never publication consent.

## Genuine owner decision

The owner must personally review the request and post a JSON decision on that
request PR. No command generates or posts approval. The required body fields are:

- `schema_version`: `acs_history_bootstrap_publication_decision_v1`.
- `repository`: `chineduezebuiroh/realestate-intel`.
- `request_id`: exact generated request identity.
- `request_sha256`: canonical JSON SHA-256 of the entire generated request.
- `reviewed_pr`: request PR number; `reviewed_commit`: its exact 40-character head.
- `decision`: owner's explicit `authorize_exact_publication_only` decision.
- `personally_reviewed_and_authorized`: owner's explicit boolean `true`.
- `authorship`: `human_personal`.
- `acknowledgements`: exact list in the reviewed request, including 2020, 2023 and
  bounded 2024 assurance limitations and the prohibition on installation/activation.
- `decision_rationale`: owner's nonempty explanation.

For independently computing the request hash (does not create a decision):

```sh
python - "$ACS_PHASE2/request.json" <<'PY'
import sys
from jobs.monthly_refresh.acs_history_bootstrap_publication import read
from core.source_artifacts.hashing import sha256_json
print(sha256_json(read(sys.argv[1])))
PY
```

Retain `decision-locator.json` containing **only** the actual `reviewed_pr`,
`reviewed_commit` and `comment_id` after posting. No reviewer, timestamps, snapshot
or provenance is guessed from a URL. The adapter obtains these from live GitHub.
It checks repository owner account identity, personal attestation, no GitHub App,
exact unedited comment and request, direct/list consistency, exhaustive comments/
reviews/inline pagination, conservative supersession and stable double scans.
Any later authorized human discussion blocks until reviewed and resolved through
a fresh decision. GitHub proves account identity, not who operated credentials.

## Credential and read-only verification

Use the Phase 1 Mac credential preflight in
[acs_history_bootstrap_preparation.md](acs_history_bootstrap_preparation.md).
The adapter reads `GITHUB_TOKEN`, not the implicit GitHub CLI credential cache.
Never print tokens or run with shell tracing enabled:

```sh
set +x
if [ -z "${GITHUB_TOKEN:-}" ]; then
  GITHUB_TOKEN="$(gh auth token --hostname github.com 2>/dev/null)"
  export GITHUB_TOKEN
fi
```

Read-only verification needs repository Contents/pull-request/issues read access.
Authorized publication additionally needs Contents write access for Releases and
catalog commits, subject to repository protections. No bypass of branch protection
is supplied. If direct catalog advancement is disallowed, stop: do not weaken
protection or silently switch to a different catalog installation mechanism.

Check out the exact owner-reviewed request PR head, using a clean checkout. The
adapter requires that HEAD, verifies protected files against the reviewed head and
current main, and permits main advances only with unchanged governance and exactly
the allowed catalog prefix. Original scope/configuration and all evidence bytes
remain mandatory. It does not require a new Phase 1 bundle after publication.

The following is an execution instruction for **after** genuine consent and review,
not authorization to run it during implementation:

```sh
python -m jobs.monthly_refresh.acs_history_bootstrap_publication \
  --repository-root "$PWD" --inputs "$ACS_INPUTS" \
  --bundle-directory "$ACS_PREPARED" --request "$ACS_PHASE2/request.json" \
  --decision "$ACS_PHASE2/decision-locator.json" \
  --journal "$ACS_PHASE2/journal" --publish
```

Before the first production write, all 35 original parents, independent candidate
states, lineage, provider omissions, approved gap, compatibility evidence, live
governance and owner decision are checked. Recovery repeats reconstruction. Every
publication stage/catalog write rechecks consent, code/configuration, evidence
hashes and current main. There is no offline authority fallback.

## Sequence, receipts and failure recovery

The sequence is ACS1 then ACS5. Each uses existing source-artifact Release naming,
prepares, uploads exact retained bytes, downloads/verifies, finalizes, independently
checks numeric Release/asset identity and tag, then catalogs the verified receipt.
Uploads use retained bytes without compression or rebuilding. Verification may
create a temporary deterministic envelope solely to compare hashes; it never
replaces a retained TAR. Original candidate timestamps and acquisition identities
remain unchanged.

The durable journal contains append-only intents, known numeric remote identities,
per-fence live authorization audits, original receipts and final installation
evidence. A local advisory lock prevents concurrent use of one journal. Use one
owner execution/journal; do not start parallel publishers with separate journals.
Git main/catalog CAS still rejects concurrent advancement.

| Failure | Operator response |
| --- | --- |
| Lost create/upload/finalize response | Retain journal; rerun the identical command. Inspect exact existing objects and bytes; never replace assets. |
| Known numeric Release/asset disappears or tag changes | Stop; investigate. A replacement is not automatic recovery. |
| Existing asset bytes conflict | Stop; preserve the remote object and local TAR. |
| One candidate published/cataloged | Neither state is installed. Retry the same reviewed operation with the same journal. |
| Catalog response lost | Exact durable main/content checks reconcile; otherwise stop. Retry only after inspecting retained evidence. |
| Catalog/governance/pointers drift | Stop for review. Do not edit the bundle/request or whitelist the drift. |
| Owner decision withdrawn/edited or reviewed head changes | Stop; fresh explicit decision/review required. |
| Journal receipt missing/truncated/corrupt after cataloging | Stop; restore genuine retained evidence and verify it. Do not synthesize a receipt to match the catalog. |
| Local audit/disk write fails | Stop. Published objects remain inactive; preserve partial evidence. |

A fresh decision cannot silently rebind an existing journal. If authorization
changes during recovery, review a handoff to a new journal retaining the original
receipts and remote-identity evidence; never edit the old binding or manufacture
replacement evidence.

No mutation is blindly repeated within a failed invocation. Unknown outcomes are
resolved by fresh remote reads on the next invocation. Known numeric identities
are pinned by the journal. A process kill before an ID was received can only be
recovered through exact tag/asset discovery and byte verification. Request retries
may produce new verification timestamps; an existing retained receipt is preserved.

## Separate installation evidence

After both catalog entries exist, or on a fresh read-only invocation:

```sh
python -m jobs.monthly_refresh.acs_history_bootstrap_publication \
  --repository-root "$PWD" --inputs "$ACS_INPUTS" \
  --bundle-directory "$ACS_PREPARED" --request "$ACS_PHASE2/request.json" \
  --decision "$ACS_PHASE2/decision-locator.json" \
  --journal "$ACS_PHASE2/journal" --verify-publication
```

This performs no remote writes. It rechecks all original parents/candidates and
fresh authorization/governance, downloads both remote packages, verifies receipts,
and permits exactly the original catalog plus the ACS1-first prefix of the two
exact additions, including expected CAS bookkeeping. Installation evidence
requires both additions. All baseline records and accepted pointers must remain
unchanged; additional records or unrelated catalog changes require explicit review.

Phase 1's verifier is unchanged and will correctly reject an advanced catalog.
Phase 2 uses a separate temporary current-catalog witness for its existing parent
reconciliation primitives; the original input and bundle are never overwritten,
re-identified or presented as a newly passing Phase 1 run. The original catalog is
independently read at the original production baseline, with current-main ancestry
and registry checks. Candidate reconstruction, original qualification catalog and
assurance limitations remain unchanged.

The final append-only installation evidence binds current commit/catalog blob and
semantic digest, exact receipts/records, fresh decision audit and original
`proposed_acceptance`. It records `passed_publication_only`, registry installation
not performed, and separate installation authorization required. The next step
is human review/authorization of an atomic registry PR adding both original state
records and both `current_state` pointers together. Preserve every other registry
field and all catalog accepted pointers. This adapter cannot perform that step.

Live external consent and Release mutations cannot be one GitHub transaction;
checks occur immediately before each bounded operation. A later withdrawal blocks
subsequent operations and installation, without deleting already published
immutable objects. The evidence is time-specific, never a permanent approval token.

Implementation tests use only offline fixtures. They do not establish publication
of the owner's retained packages or replace a Mac-side authorized execution audit.
