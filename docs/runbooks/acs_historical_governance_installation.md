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

## Approval provenance and review boundary

Existing `ACSGovernance.scope()` requires nonempty approval provenance; the
backfill reviews and bootstrap runbook use review/decision references but do not
provide a typed human approval format for these initial production scopes. This
proposal defines a **bounded installation verifier**, without changing the scope,
gap, identity, transport or capability contracts. Legacy fixtures remain valid
under the generic contract; the installation verifier imposes the stricter exact
three-record proposal and provenance requirements for this initial installation.

Each `approval_templates` value has null genuine-human fields:

- `decision`: the human's explicit `approved` decision, independently for the gap,
  ACS1 scope and ACS5 scope. Endorsed policy direction is not this approval.
- `reviewer_login`: actual authorized human GitHub login.
- `reviewed_at`: actual decision timestamp with timezone.
- `reviewed_pr`: exact PR number reviewed.
- `reviewed_commit`: full 40-character commit SHA that the review actually covers.
- `review_url`: exact `https://github.com/chineduezebuiroh/realestate-intel/pull/<number>#pullrequestreview-<id>`.
- `decision_rationale`: the human's explanation of the approved decision.

No field above is populated here. Other fields bind the **unapproved subject** by
exact semantic identity and canonical JSON digest, documentary evidence path/byte
hash, complete request scope, configuration digests and explicit limitations.
These commitments are inside final approval provenance and therefore inside the
final record identity. The gap subject commits the retained individual reference
and excerpt digests as well as the capture. Scope subjects commit the inclusive
range, expected list and gap partition. A scope's reference to the proposed gap
is replaced only by the final, separately approved gap identity.

This avoids a self-referential identity: the decision reviews this proposal at an
existing immutable commit, not a future commit containing its own SHA. The later
installation PR separately reviews the finalized records and diff. The exact
reviewed proposal commit need not equal the installation commit. A later change
to a subject, evidence, configuration or limitations requires fresh human review;
do not update a reviewed commit to the latest head without a corresponding review.

The verifier checks schema, decision, timestamp timezone, exact subject/evidence/
configuration/geography/limitations bindings, PR/commit syntax and review URL.
**It does not authenticate GitHub reviews, human identity, repository permission,
or signatures.** A perfectly formatted invented review can pass an offline
structural check. Hashes and structural success cannot distinguish that forgery.
The authorized human reviewing the installation must independently open each
review URL, verify its actual author is an authorized human, its submitted time,
APPROVED state, exact commit SHA, and explicit decision/rationale for that subject.
Confirm the proposal file at that SHA equals the reviewed subject commitments;
verify the official citations and retained hashes. A generic code approval or a
model-generated statement is insufficient. Reject a dismissed/stale review or
any mismatch. Preserve the real review references as provenance in the final PR.

Main membership is a trust boundary, not a reviewer authentication service:
`ACSGovernance.load_production()` supplies its existing constrained read-only
transport/capability and resolves the commit-pinned main registry. A raw local
registry, proposal, test fixture or self-consistent hash cannot replace it. An
unauthorized but well-shaped registry merged to main would violate the human
review process; neither the existing loader nor this verifier claims otherwise.

## Exact later authorized installation sequence

Do not perform these steps merely because tests pass. This PR stops for review.

1. Authorized humans independently review and decide the gap, ACS1 scope and ACS5
   scope at an exact proposal PR commit. Record the seven genuine fields for each
   subject; verify the real reviews as described above. Review all limitations,
   including no compatibility exception or physical acceptance. This proposal PR
   may merge only as inactive preparation; no active file is included.
2. After explicit installation authorization, start a **separate installation
   branch from freshly resolved current main**. Require the production registry
   to remain absent; if one appeared, stop initial bootstrap and reconcile it in
   a newly reviewed change. Compare current configuration, request scope,
   documentary capture and proposal commitments with those actually reviewed.
   Any drift requires renewed review, not copying old approvals.
3. Make three JSON provenance objects by filling only the seven null fields of
   the corresponding templates with the real decisions. Keep them outside active
   configuration until finalized. Never use the synthetic test fixture data.
4. Perform the following exact construction offline in the separate branch,
   after genuine approval. This snippet is documented preparation, not executed
   by proposal validation or by this PR. The `approvals` map must contain the
   verified complete provenance objects keyed by the three record keys.

```python
from copy import deepcopy
import json
from pathlib import Path
from jobs.monthly_refresh.acs_governance_installation import (
    generate, validate_initial_registry,
)
from jobs.monthly_refresh.acs_scope import identify, GAP_VERSION, SCOPE_VERSION

root = Path('.').resolve()
proposal = generate(root)
# Human-supplied file, independently verified against the real GitHub reviews:
approvals = json.loads(Path('/tmp/acs-genuine-approvals.json').read_text())
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
validate_initial_registry(registry, root)  # structural only; never authorization
Path('/tmp/acs-authorized-initial-registry.json').write_text(
    json.dumps(registry, sort_keys=True, indent=2) + '\n')
```

5. Inspect the candidate, independently recompute all three IDs, verify that each
   gap reference and current pointer names the exact installed record, and run
   the read-only verifier and focused/full regression suites. Both physical-state
   maps must remain empty. Only after the separate branch is authorized, add the
   exact candidate bytes at `config/acs_historical_governance.json`. Open a separate
   reviewed installation PR targeting main. Its description must inventory final
   IDs, registry file digest, approval references, configuration hashes and the
   explicit limits. Review the exact diff; recheck fresh main before merge. No
   direct main writer, automated approval, auto-merge or runtime installation is
   provided by this proposal. Merge only under the separate human authorization.
6. After that authorized merge, verify remote authority using the existing loader
   and the read-only wrapper:

```python
import os
from pathlib import Path
from jobs.monthly_refresh.acs_governance_installation import verify_remote, REPOSITORY
# Use the repository's configured GitHub credential normally; never print it.
result = verify_remote(REPOSITORY, os.environ['GITHUB_TOKEN'], Path('.').resolve())
print(result)
```

`verify_remote` accepts no injectable API, uses the fixed repository, checks the
exact initial membership and approval commitments, invokes the existing
`scope()` for both products, requires empty physical state, and rereads current
main at completion. Missing registry, proposed/malformed approvals, mismatched
references/keys/pointers, wrong namespace, configuration drift, duplicate metadata
and intervening governance-content drift fail closed. Remote verification is
initial-installation-specific; it deliberately rejects later nonempty physical
state. It never activates data. Review authenticity remains an external human
requirement even if remote structural validation passes.

## Identity changes and remaining gates

All fields except the record's own identity participate in its semantic hash.
Changing gap status and adding actual provenance changes the gap `evidence_id`.
Rebinding ACS1's gap reference and adding its scope approval changes ACS1's
`authority_id`; adding ACS5's approval changes ACS5's `authority_id`. Map keys and
current pointers must use those final IDs. No final identities can honestly be
reported before the genuine provenance exists. A later provenance edit also
changes IDs and must propagate through dependent references and fresh review.
The unapproved subject commitments remain stable inside provenance.

PR #302 remains draft and requires its separate authorization work; scopes do not
approve the 2024 compatibility exception. ACS5 2023 acquisition recovery, original
TAR/provider evidence verification, physical completeness and state acceptance,
Source Set publication, serving promotion and Macro remain separate gates. This
proposal neither modifies PR #299 nor authorizes those operations.

## Reproducible checks

```sh
python -m jobs.monthly_refresh.acs_governance_installation --check-proposal docs/proposals/acs_historical_governance_installation.json
python -m jobs.monthly_refresh.acs_scope_bootstrap --check docs/proposals/acs_historical_scope_bootstrap
# Later, read-only candidate verification in the authorized installation branch:
python -m jobs.monthly_refresh.acs_governance_installation --check-initial-registry /tmp/acs-authorized-initial-registry.json
python -m pytest -q tests/test_acs_governance_installation.py tests/test_acs_scope_bootstrap.py tests/test_acs_historical_materialization.py tests/test_acs_boundary_hardening.py tests/test_acs_readiness.py
python -m pytest -q
```

The new tests use explicitly labeled synthetic approval data only in memory;
none are included in production proposal provenance. HTTP tests intercept below
the existing constrained loader, require GET-only commit-pinned reads and test
remote absence, proposal rejection, duplicate metadata, wrong namespace and main
content drift. A synthetic positive remote test proves the transport/contract
path, not genuine human approval or actual installed remote authority.
