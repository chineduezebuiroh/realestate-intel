# Inactive ACS historical governance installation proposal

PR #303 merged at `6e900d517b7bcb77fe1725242bf3157e442cd3ae`. GitHub's
main branch was independently resolved to that commit before creating
`feat/acs-historical-governance-installation`. PR #302 was independently checked
as open/draft and is not modified. This change is a proposal for review, not the
subsequent authorized installation merge.

## Exact review inputs

[The installation proposal](../proposals/acs_historical_governance_installation.json)
contains three complete existing-contract `records`, three `approval_templates`,
and an `initial_registry` with production namespace and **all five maps empty**.
No `config/acs_historical_governance.json` is installed by this change.

| Record key | Existing semantic identity |
| --- | --- |
| `acs1_2020_gap` | `acs_governed_gap_evidence_v1__29ce8edab380e39f094ecd344165ead307e1756e66fc2e6f2f1e832ad72fe676` |
| `acs1_scope` | `acs_historical_scope_authority_v1__58ba7be33c560fbf3eda7b89b15029474c7063b57f9a60846c23223fa84023b8` |
| `acs5_scope` | `acs_historical_scope_authority_v1__0bcd7e9639b2c0630fb914a1e85868bca6b632b6fdbb7df43074248ae1e91658` |

Configuration byte digests, preserved verbatim in all approval templates:

- `config/geo_manifest.generated.csv`: `99d1bf72bcf4c2108824ff57751252047ee33dc211abc68c8669eeb07c8e9615`
- `config/monthly_refresh_policy.json`: `8075b7419ebb815b2632a720476b1e717212db6eacbcff40edcce76d6c6afd16`
- `config/source_metric_registry.csv`: `14acfa965907b8884dbb6225e9f97b4143e6b248cefe53e13c3b3307cf435e4f`
- `config/source_refresh_revision_policy_v0_2.json`: `8ae73b4f0bcdd80b3f03f20fb6b1db4ca1ae214af6b40ac3f97ad92872449e8a`

The ACS1 scope is 2005–2024: 19 expected available vintages and the separately
referenced standard ACS1 2020 whole-vintage nonpublication gap. ACS5 is 2009–2024:
all 16 vintages expected available, no gaps; its missing 2023 acquisition remains
an acquisition-recovery issue. No historical completeness or acceptance follows.

Both population `B01003_001E` and median household income `B19013_001E` retain their
current physical source metric IDs. Each approval template binds all 223 requested
geographies, their membership digest, the 163 county/54 CBSA/five state/one nation
partition, and all ten explicit Metropolitan Division exclusions. The scopes bind
the existing four configuration digests. The gap additionally retains exact
variables, request scope, citations, per-excerpt hashes and documentary capture
SHA-256 `4ad2769dc2da426cd47db8dafe7bb77b95d72dc574c4d3caa72be8596aad8b8b`.
The capture contains reference metadata and short excerpts, not complete HTML or
an endpoint response. Experimental ACS1 and ACS5 are not substituted.

The proposed records are byte-independent semantic copies of #303's conditional
ACS1 scope, gap proposal, and ACS5 scope record. Original acquisitions, reports,
configuration and accepted August identities are not regenerated or edited.

## Initial bootstrap only, not recurring production approval

This procedure establishes the **initial historical governance registry**. It is
not a prerequisite for routine monthly refresh, materialization or promotion
under already-approved authority. No workflow, generic `ACSGovernance` check, or
recurring production consumer is changed. `verify_remote` in this module is an
initial-installation audit wrapper: its empty-state requirement intentionally
makes it unsuitable for normal operations after physical acceptance. Ordinary
production consumers continue using their existing remote-main authority checks.

## Two genuine-human provenance mechanisms

Existing scope/gap contracts require attributable human-controlled authority and
separate gap/scope decisions. They do not specify two distinct humans. GitHub
prevents a PR author from submitting an APPROVED review on their own PR, even if
they own the repository. This correction supports owner authorization without a
second account while retaining actual approved reviews as an alternative.

`acs_historical_governance_approval_provenance_v2` is the bounded initial-install
provenance format. Each subject retains the exact immutable commitments:
`schema_version`, `repository`, `subject` (proposal identity and canonical record
SHA-256), `documentary_evidence` (capture path and byte hash),
`approved_request_scope` (metrics, full membership digest, exclusions and level
counts), `config_hashes`, and `limitations`. The gap subject commits the retained
citations and individual excerpt hashes; scope subjects commit range and complete
vintage partition. The seven genuine fields remain:

- `decision`: explicit `approved`, separately for each subject.
- `reviewer_login`: actual authorizing human's GitHub login.
- `reviewed_at`: actual GitHub submission/creation timestamp with timezone.
- `reviewed_pr`: exact positive PR number.
- `reviewed_commit`: full lowercase 40-character reviewed SHA.
- `review_url`: exact canonical GitHub URL of the decision object.
- `decision_rationale`: the human's subject-specific rationale.

Two new fields distinguish mechanisms:

| `provenance_kind` | `review_url` | `owner_comment` |
| --- | --- | --- |
| `github_approved_review` | `https://github.com/chineduezebuiroh/realestate-intel/pull/<pr>#pullrequestreview-<id>` | null |
| `github_owner_decision_comment` | `https://github.com/chineduezebuiroh/realestate-intel/pull/<pr>#issuecomment-<id>` | Exact metadata object below |

Owner-comment metadata contains exactly `comment_id` (positive integer),
`comment_url`, `author_login`, `author_user_id` (positive integer), `created_at`,
`updated_at`, `body_sha256`, and `snapshot_sha256`. The body digest is SHA-256 of
the **exact UTF-8 GitHub API `body` string**, without newline normalization or
canonical JSON reserialization. `snapshot_sha256` uses existing `sha256_json` on
`comment_snapshot(actual_comment)`: exactly `id`, `html_url`, `issue_url`, `user`
(only `id`, `login`, `type`), `created_at`, `updated_at`, `body`, and
`performed_via_github_app`. Author login, URL, ID and creation time must agree
with top-level provenance. Edits are blocked: created and updated instants must
be equal. Post a new personally authored decision instead of editing one.

All genuine fields, including discriminator and owner metadata, remain null in
the checked-in templates. No decision or actual provenance has been created.

## How the owner personally records the decisions

After reviewing the **final corrected proposal head**, the owner personally signs
in to GitHub and posts a PR **Conversation comment**. Do not ask an LLM, bot,
application or agent using the owner's credentials to post it. The owner must
personally exercise the policy decision and review the exact evidence. Generated
wording, tests and a merge are not proof of that decision.

The comment body must be a single valid JSON object, without Markdown fences or
surrounding prose. Its exact shape is specified by `decision_body()` in
`jobs/monthly_refresh/acs_governance_provenance.py`:

- `schema_version`: `acs_initial_governance_decision_v1`.
- `repository`: `chineduezebuiroh/realestate-intel`.
- `reviewed_pr`: the actual proposal PR number, and `reviewed_commit`: its actual
  full reviewed head SHA. Use the corrected head, not the superseded #304 head.
- `personally_reviewed_and_authorized`: JSON boolean true.
- `authorship`: `human_personal`, as a truthful personal attestation.
- `decisions`: entries keyed by `acs1_2020_gap`, `acs1_scope`, `acs5_scope`.
  Each entry contains `decision="approved"`, the human's `decision_rationale`,
  and exact copies of `subject`, `documentary_evidence`,
  `approved_request_scope`, `config_hashes` and `limitations` from that subject's
  reviewed approval template. No implied or omitted scope/configuration binding.

**One comment can cover all three subjects.** Each entry has its own subject
identity, digest, commitments and rationale. Separate comments/reviews may cover
disjoint subjects instead; the verifier requires all three provenance objects and
requires each object's body to cover exactly its selected subject group. A
partial approval cannot produce a complete initial registry. Mixing mechanisms
is allowed, but no subject may be silently replaced or left unbound.

For the review alternative, an authorized human other than the PR author submits
an actual GitHub APPROVED review with the same structured decision body. The
verifier checks review state, submitted time, commit, URL, PR, author permission
(write/maintain/admin or owner), non-self-review and absence of an application
attribution. The personal repository owner is resolved live by both login and
numeric user ID; owner-comment decisions must come from that exact account.

Do not edit or delete authorization comments. A withdrawal or replacement must
be a **new** comment explicitly stating withdrawal/supersession and naming the
affected subject/earlier URL. Such a later comment blocks installation. Resolve
it by new personally authored decisions at the current proposal head, and keep
the old evidence for audit; do not reuse its provenance. Moving the proposal PR
head invalidates old reviewed-SHA provenance even if the change looks harmless.

GitHub authenticates the account, not the person operating its credentials. The
verifier rejects bot users, GitHub-app-attributed decisions, explicit LLM/fixture
claims, missing personal attestations and nonexact payloads. It **cannot detect
an LLM impersonating a human through the same credentials with an identical
payload**. A truthful owner attestation plus human review of personal authorship
is an irreducible requirement. No claim of cryptographic proof of personhood is
made, and neither audit JSON nor an offline fixture grants authority.

## Read-only live verification and retained audit

Prepare three provenance objects from the real GitHub evidence without changing
production configuration. The operator obtains the comment ID, canonical URL,
author ID/login and GitHub timestamps from `GET /issues/comments/<id>`, then
computes exact body and selected snapshot hashes using `body_hash()` and
`comment_snapshot()`; those functions do not issue decisions. The subject
provenance must retain the reviewed template commitments unchanged. For a
review, use its actual review ID/URL and submitted timestamp. No sample approval
objects in tests may be used.

Run from the reviewed checkout, using the configured GitHub credential normally
(or read-only public access where supported), without printing credentials:

```sh
python -m jobs.monthly_refresh.acs_governance_provenance \
  --provenance /tmp/acs-genuine-provenances.json \
  --audit-output /tmp/acs-bootstrap-authorization-audit.json
```

This command constructs its own read-only GitHub transport; it accepts no API
injection or local evidence fallback. It performs only GET requests:

1. Resolve the fixed repository and its current personal owner. Require the exact
   PR/repository/main-base association and reviewed head SHA for every decision.
2. Read the proposal at each commit-pinned reviewed SHA and validate its exact
   content against current inputs. Drift, ambiguous JSON or stale SHA blocks.
3. Retrieve each actual comment/review directly. Require presence in the complete
   paginated stream, genuine owner/authorized user account, no app attribution,
   exact URL/PR/time/body/identity/commitments, explicit human attestation and the
   exact selected subject decisions. Edited/deleted/missing evidence blocks.
4. Fetch **every page** of conversation comments, inline review comments and submitted reviews, including
   pages after a short page or absent Link header, until an empty page. Validate
   next-page links, reject duplicates/skipped links and require conversation count
   to equal the PR's GitHub counts (conversation and inline comments). Missing continuation evidence, incomplete
   retrieval, permission errors and the bounded page limit all fail closed.
5. Conservatively block on **any later authorized-human comment or review** not
   among the selected decision objects, including an earlier comment updated
   later. This includes plain-language withdrawals, supersession and conflicting
   decisions without trusting an NLP interpretation. Later benign authorized
   discussion also blocks: resolve with a fresh final personal decision. Earlier
   discussion may remain; selected disjoint subject decisions do not block each
   other. Dismissed selected reviews always fail.
6. Repeat all complete scans and the PR read; require stability. Reread selected
   objects at completion. Concurrent edits/deletion, PR movement, comment-count
   changes or altered paginated evidence block. This is a point-in-time check;
   no remote API provides an atomic lock against subsequent human changes.

The new audit file is created exclusively, outside `config`, without overwriting
an existing snapshot. It contains schema `acs_initial_governance_authorization_audit_v1`,
`namespace="audit_only"`, repository/owner identity, check timestamp, each complete
selected evidence record and provenance, reviewed proposal hashes, all list
endpoint page inventories and scanned comment/review content, verification
results and a canonical `snapshot_sha256` excluding that field. It contains no
transport credential. It reports `production_authority=false`, `activation=false`.
The audit explicitly records the personal-authorship limitation.

Retain the exact snapshot in the later installation review as a small audit
artifact; it is **evidence, not authority**. Reverify against it with a new output
path immediately before installation:

```sh
python -m jobs.monthly_refresh.acs_governance_provenance \
  --provenance /tmp/acs-genuine-provenances.json \
  --prior-snapshot /tmp/acs-bootstrap-authorization-audit.json \
  --audit-output /tmp/acs-bootstrap-authorization-audit-rechecked.json
```

A retained file alone is never sufficient. The verifier checks its digest/schema,
namespace, owner and exact evidence against fresh actual GitHub objects. Edited,
missing, withdrawn, superseded or stale evidence blocks even when an old snapshot
looks valid. Preserve prior snapshots, rather than overwriting history. Human
review of the final installation PR remains required. If discussions continue,
repeat verification and obtain fresh decisions as necessary.

## Exact later authorized installation sequence

This PR stops for review. Successful tests or audit checks do not authorize any
of these operations:

1. Owner personally reviews and records the three decisions at the corrected
   proposal head, or authorized nonauthor humans use actual approved reviews.
   Verify the real evidence and retain its audit as above.
2. After explicit installation authorization, start a **separate branch from
   freshly resolved main**. Require `config/acs_historical_governance.json` to
   remain absent. If it appeared, stop bootstrap and reconcile in a new reviewed
   change. Verify unchanged reviewed evidence, configuration and request scope.
3. Fill each provenance object with the genuine live-verified metadata. Construct
   the exact candidate offline using the existing contracts:

```python
from copy import deepcopy
import json
from pathlib import Path
from jobs.monthly_refresh.acs_governance_installation import generate, validate_initial_registry
from jobs.monthly_refresh.acs_governance_provenance import verify_authorizations
from jobs.monthly_refresh.acs_scope import identify, GAP_VERSION, SCOPE_VERSION
import os

root = Path('.').resolve()
proposal = generate(root)
approvals = json.loads(Path('/tmp/acs-genuine-provenances.json').read_text())
# Always live; never replace this with a successful old audit or fixture:
audit = verify_authorizations(approvals, os.environ.get('GITHUB_TOKEN', ''), root)
registry = deepcopy(proposal['initial_registry'])
gap = deepcopy(proposal['records']['acs1_2020_gap'])
gap.update(status='approved', approval_provenance=approvals['acs1_2020_gap'])
gap = identify(GAP_VERSION, gap, 'evidence_id')
registry['gap_evidence'][gap['evidence_id']] = gap
for name in ('acs1_scope', 'acs5_scope'):
    scope = deepcopy(proposal['records'][name])
    scope['approval_provenance'] = approvals[name]
    if name == 'acs1_scope':
        scope['governed_gaps'] = [{'vintage': 2020, 'evidence_id': gap['evidence_id']}]
    scope = identify(SCOPE_VERSION, scope, 'authority_id')
    registry['scopes'][scope['authority_id']] = scope
    registry['current_scope'][scope['source_id']] = scope['authority_id']
validate_initial_registry(registry, root)  # Structural check, never authorization
with Path('/tmp/acs-authorized-initial-registry.json').open('x') as output:
    output.write(json.dumps(registry, sort_keys=True, indent=2) + '\n')
```

4. Review the three final identities, exact registry byte digest, approval/audit
   references and current main. Rerun focused/full checks. Keep `states` and
   `current_state` empty. Only in the separately authorized installation branch
   add the exact candidate at `config/acs_historical_governance.json`; submit a
   separate installation PR and explicitly review that diff. A merge by itself
   is not the owner's policy authorization. Reverify live evidence immediately
   before its separately authorized merge. No direct runtime writer is provided.
5. After that authorized merge, use this **initial-installation-only** wrapper:

```python
import os
from pathlib import Path
from jobs.monthly_refresh.acs_governance_installation import verify_remote, REPOSITORY
result = verify_remote(REPOSITORY, os.environ.get('GITHUB_TOKEN', ''), Path('.').resolve())
```

It uses existing `ACSGovernance.load_production`, requires exact initial
membership/current pointers and empty physical state, rechecks all three live
authorizations through the new verifier, then rereads current main to detect
changed registry content. Preserve `result['authorization_audit']` in the review
record. Neither this wrapper nor its audit replaces the generic remote-main
trust model. No data activation follows.

## Identity changes, limits and remaining gates

The three **proposed record identities remain unchanged**. Adding genuine v2
provenance and changing gap status changes the gap ID; rebinding ACS1 to that
final ID and adding its provenance changes ACS1's ID; adding ACS5 provenance
changes its ID. Use those final keys/pointers. No final approved IDs exist yet.
The unapproved subject commitments remain stable inside provenance, avoiding a
self-referential future-commit identity. The proposal PR/commit is the reviewed
subject; the later installation PR separately reviews the finalized registry.

PR #302 and PR #299 remain untouched. Compatibility authorization, ACS5 2023
acquisition recovery, original TAR/provider verification, physical completeness
and acceptance, publication, serving and Macro require their separate gates.
Initial owner-comment bootstrap authorization grants none of those operational
permissions and adds no recurring comment requirement to their workflows.

## Checks

```sh
python -m jobs.monthly_refresh.acs_governance_installation --check-proposal docs/proposals/acs_historical_governance_installation.json
python -m jobs.monthly_refresh.acs_scope_bootstrap --check docs/proposals/acs_historical_scope_bootstrap
python -m pytest -q tests/test_acs_governance_provenance.py tests/test_acs_governance_installation.py tests/test_acs_scope_bootstrap.py tests/test_acs_historical_materialization.py tests/test_acs_boundary_hardening.py tests/test_acs_readiness.py
python -m pytest -q
```

Tests intercept HTTP below the read-only transport and use labeled synthetic
in-memory decisions only. They prove positive transport shapes and rejection of
wrong identity/repository/PR/SHA, bots/apps/LLM/fixtures, incomplete or changed
subjects/evidence/configuration/limitations, edited/deleted/missing comments,
stale snapshots, review dismissal/self-review, page-two withdrawals/conflicts,
pagination omissions and concurrent mutation. They do not establish actual human
authorization. No real approval comment or installed authority is included.
