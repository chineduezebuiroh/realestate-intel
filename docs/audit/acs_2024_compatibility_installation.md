# Bounded 2024 compatibility installation review

Base main and PR #302 merge: `c009bee32d61ac76863662336a1ef0f6e8ff79f7`.
Reviewed #302 head: `66042d165595df78bb95da52f0fd470a49592a5a`.
Owner decision: https://github.com/chineduezebuiroh/realestate-intel/pull/302#issuecomment-6590583804

Final proposed installation identity:
`acs_2024_compatibility_authorization_v1__9bf426f18fffcb183ac33ae4cf543873071b5dde1093a77094ead13f166868ba`.
Original proposal:
`acs_2024_compatibility_authorization_v1__1edf025a6b1daa7b005527e3f1645df81746af142697ca7b15da9a672b02f66c`.
Proposal semantic SHA-256:
`f6bc4888322a82743ddd8afaeaa03f07e7c9aea2ae560d0adc414221ebe313fb`.

## Retained genuine evidence

`acs302-owner-audits.zip` is the unchanged owner-uploaded archive (35,409 bytes),
SHA-256 `586dfe810a649e3377d2f9ae19be49f448adf2d1bdb5a71714bc338feabe0de7`.
It retains exact audit bytes once, compressed; no report or package is duplicated.

| Audit | Raw file SHA-256 | Internal snapshot SHA-256 |
| --- | --- | --- |
| compatibility-302-owner-audit.json | 20a9068011972b41696379ab98c1395781ce81bf51629d4e02d4fb7d36733283 | 156889be5dd0b605fbda0d97d6d740e4cc7148ec1865bf6e347ab6ea4f238c31 |
| compatibility-302-owner-audit-preinstall.json | 29fde9b5e20d3cc18eef5ad29a794d3893b31765b7e27015f62cccb23da8a698 | 17473b733a6278261e534bdc29b7bd549a6162e5b8f049849520a39eb7b9f3a5 |

The original audit checked main `97ef9baafe23fff06288e9e8fef455a42a1d25ae`;
the fresh preinstall audit checked merged main `c009bee32d61ac76863662336a1ef0f6e8ff79f7`.
Both independently obtained the identical genuine approval provenance, copied
unchanged into the installation candidate. Both verified current scopes through
the production loader, with registry semantic SHA-256
`201875379f4eabc2af6cecd5343140a1bed019421c3fd2af71ad85aa0eb81392`.

Validation recomputed both internal digests, exact owner-body and comment snapshot
hashes, reviewed-head and subject commitments, canonical decision content including
the owner's actual rationale, both complete stable discussion scans, and the
current-scope bindings. The original report commitments and limitations remain
unchanged. Both original TARs were technically requalified independently; each
complete semantic report exactly matches its unchanged retained original.

## Independent current checks and environment limit

GitHub connector reads confirmed merged #302 at the exact reviewed head, latest
main, and the unchanged production registry blob
`189334e7a9bfe9ebfecf942b1d35309cac1ee88a`. Two live reads confirmed the exact
personal owner comment unchanged and unedited, with one issue comment and no
reviews or inline comments; there is no later discussion to supersede the decision.
The owner account numeric ID agrees with the repository owner. These are live
corroborating checks, not a manufactured production ACSGovernance capability.

An independent cloud attempt from the exact reviewed checkout failed on proxy
CONNECT 403 for api.github.com before production scope reads. This is an
environment limitation, not an audit or evidence mismatch. The supplied genuine
Mac audits fulfill the established live owner-decision/current-scope contract for
preparing this draft; that contract does not require the cloud to repeat a
successful Mac audit. No runtime transport, generic verifier, or governance
contract has been changed. Copied audit bytes alone confer no runtime authority.

## Exact installation delta and remaining gates

The production registry candidate adds only:

- `compatibility_authorizations[final_authorization_id]`: the exact proposal with
  status changed to approved, genuine audit approval_provenance attached, and
  authorization_id recomputed using the existing semantic identity function.
- `current_compatibility_authorization`: that final identity.

Removing those two fields reproduces the complete audited original registry
semantic hash. Existing scopes, gap evidence, current_scope, states and
current_state are unchanged, with both physical-state maps empty. No production
code, workflow, report, acquisition, or original proposal changes are included.
The older negative test now checks that the *proposal ID* remains unusable,
rather than requiring the registry to remain forever without an approved record.

The 67 ACS5-only geographies retain ACS5 identity and process-attested ACS1
absence. The four jointly unobserved remain missing. Original manifest.git_sha
remains unknown; recovered execution-code evidence stays separate. No provider
response verification or broader-vintage exception is asserted.

This is a draft installation change, not an installed authority. Explicit human
installation review and merge authorization remain required. Recheck current
main/scopes and owner-decision supersession before merge; material changes require
renewed review. After authorized merge, run the existing production consumer
verification against remote main and original TARs from an API-accessible owner
environment. That post-merge check cannot pass for this unmerged candidate and
must not be simulated with fixture authority. Historical physical-state acceptance,
Source Set publication, serving promotion and Macro execution remain separate gates.

## Repository validation

- Focused installation/authorization/provenance/initial-installation tests: **172 passed**.
- Full regression suite: **1,128 passed**, 138 existing pandas deprecation warnings.
- CI environment import check (`duckdb`, `pandas`): passed.
- `git diff --check`: passed.
- Both original TAR technical qualifications: passed, complete original report match.
- No approved-production readiness or historical acceptance is claimed before merge.
