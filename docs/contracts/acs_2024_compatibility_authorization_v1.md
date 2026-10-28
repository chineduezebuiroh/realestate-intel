# Bounded ACS 2024 compatibility authorization

This implementation and the complete authorization proposal remain inactive.
PR #305 installed the historical scopes on main at
`97ef9baafe23fff06288e9e8fef455a42a1d25ae`; the proposal now binds those exact
current scope IDs. Scope approval expressly excludes compatibility approval.
No production registry, original package, manifest, pin, catalog, August identity
or PR #299 is changed by this PR.

## Authority trust path

`ACSGovernance.load_production(repository, token)` constructs its own read-only
GitHub API transport, resolves remote `main`, and reads
`config/acs_historical_governance.json` at that commit. It retains the repository,
commit and complete registry semantic digest in its existing private capability
registry. Local JSON, a proposal digest, test fixtures, injected transports and
recovery-only snapshots cannot confer production authority.

The existing registry may, after separate human review, contain
`compatibility_authorizations` keyed by full `authorization_id`, and one
`current_compatibility_authorization` pointer. Membership and current pointer
must agree. The selected record must have schema
`acs_2024_compatibility_authorization_v1`, namespace `production`, status
`approved` and approval provenance. This follows the existing reviewed-main
trust model; approval strings alone are insufficient. There is no approval or
registry-write API in this change. Merging this implementation is insufficient
for authorization.

The consumer requires current production governance in the exact repository,
checks remote-main content before and after qualification and at readiness
completion, and rejects superseded registry content. Both current governing
scopes must validate through the existing `scope` contract, include 2024 and
match the current geography, metric and policy configuration digests.

## Complete record and evidence binding

`python -m jobs.monthly_refresh.acs_2024_authorization` takes `--acs1-report`,
`--acs5-report`, `--acs1-scope`, `--acs5-scope`, `--repository-root` and a new
`--output` path. It only generates status `proposed` with null approval provenance.
It accepts only the two exact successful qualification identities recorded in
the proposal inputs. It never publishes, approves or advances a registry.

The generated record binds both complete semantic report digests, exact original
file-byte SHA-256 hashes and sizes, qualification IDs, parent identities and data
hashes, complete evidence inventories, compact SHA-256 references to the full
July publication and August acceptance lineage in the retained reports, original and
reference configuration hashes, reviewed policy differences, code provenance,
qualification validator digest and historical-scope context. It also binds the
consumer file digest, current governed configuration hashes, governing scope
IDs, authorized classifications and every explicit limitation. Its full semantic
SHA-256 identity covers these fields, including status and provenance; approval
therefore creates a new identity. Retain both original report JSON files unchanged
with the proposal review evidence. Neither their successful status nor their
self-consistent digest substitutes for independent validation.

Readiness uses `compatibility_authorization` in its input specification with
`authorization_id` and `reports` keyed by physical source ID. Both original TARs
must be supplied in the existing packages map, and both exact 2024 keys must be
in `monthly_compatibility`. The CLI requires explicit
`--verify-production-compatibility` and `GITHUB_TOKEN` to load current remote
main. Missing credentials or authority fail closed. Offline mode remains advisory.
The consumer re-runs PR #301's independent qualifier against each original TAR,
anchored catalog, pins, cycle results, workflow evidence, Git objects, code and
configuration. The original file bytes must match the approved byte hash and size, even when
a reformatted file has the same semantic identity. The entire fresh report must
equal the unchanged retained report.
Missing, altered or stale reports/evidence/configuration/validator bytes fail.
The approved binding must equal the retained binding in every field.

## Evidence semantics and limits

`manifest.git_sha="unknown"` remains literal and
`manifest_code_commit_recorded=false` remains explicit. Independently recovered
execution checkout evidence is retained separately in `code_provenance`; it
never overwrites manifest provenance or establishes signed execution attestation.

Only `retained_value` and `monthly_no_content_attested` are authorized. The 67
ACS5-only geographies can contribute ACS5 observations to advisory 2024 logical
potential when ACS1 has recorded process-attested no-content. Their physical
source remains `census_acs5`. The four jointly unobserved geographies remain
missing and continue to block complete readiness. No provider-ineligibility
inference, imputation or synthetic observations are permitted. Original provider
response verification remains `not_established` on every compatibility row.

Readiness separately labels available compatibility observations and authorized
process-attested absence. It does not relabel either as independent-provider
verification. Other vintages retain the existing independent-provider path.
Historical acceptance and downstream activation remain blocked, including after
a future approved record passes this bounded readiness consumer. Physical-state
acceptance, Source Set publication, serving promotion and Macro execution each
remain separate human-governed operational gates, outside this implementation.

## Production evidence retention and observed blockers

The two user-uploaded reports were independently checked against their exact
qualification identities, 24 successful predicates each, current qualifier
bytes, original catalog identities, and anchored July/August pins and cycle
results. Their bytes were copied without regeneration or reserialization into
`docs/audit/acs_2024_qualification_reports/<file-sha256>.json`. The proposal
contains exact byte hashes and sizes, from which canonical content-addressed
relative paths are derived; it binds the complete semantic report plus compact
lineage section digests. Neither user-reported
success nor an embedded status is treated as governance approval.

The existing source artifact contract permits small specifications/manifests in
Git and prohibits material Parquet/raw data/DuckDB. These two reports total
173,869 bytes and contain qualification metadata, hashes, request/absence
classifications and lineage, not material source observations or raw HTTP
responses. Retaining one unchanged copy of each as audit metadata follows the
existing `docs/audit` evidence pattern. Numeric geography classifications are
not substituted for missing observations. No TAR, Parquet, provider snapshot,
large log payload or duplicate full lineage is added. An opaque upload pointer
or a bare digest without reachable bytes would be insufficient durable retention.

To access exact bytes, check out the immutable PR/reviewed commit containing the
reports, resolve each `qualifications[source].report_file_sha256` to
`docs/audit/acs_2024_qualification_reports/<sha256>.json` under that checkout, and
supply those paths in readiness's `compatibility_authorization.reports` map.
The consumer checks the byte hash/size and exact qualification identity before
independent requalification. A later reviewed-main authorization must bind these
same byte commitments; being present in Git alone does not approve the reports.

## Installed authorities and current validation

Installed main commit: `97ef9baafe23fff06288e9e8fef455a42a1d25ae` (merged PR #305).
Registry byte SHA-256:
`3f72a4fe4d595c7692261aa0b05e746a5824cc2faed71a313552e2214f58148f`.
The installed registry and its original provenance are preserved byte-for-byte.

- ACS1: `acs_historical_scope_authority_v1__48b6da5ee873a6e6be1855478b840ac64521acfa0bfde75de9abcc7511f7795e`
- ACS5: `acs_historical_scope_authority_v1__1d0cfe3c900e0cb1da7ea0b8ab5ae44ed73737b836c04e4649345d511ed092ee`

The merged installation validator confirms the local committed registry's exact
structure, approved scope/gap identities, provenance shape, configuration and
empty physical-state maps. This is structural evidence, not a production
capability or a new authentication of the old human decision. Direct verification
through `ACSGovernance.load_production` was attempted and blocked by proxy CONNECT
403 for api.github.com. Do not substitute local files, connector captures,
mocked transports or passing tests for that live result.

Read-only command for an authorized environment with GitHub API access:

```sh
python -m jobs.monthly_refresh.acs_2024_authorization \
  --repository-root . \
  --check-current-proposal docs/proposals/acs_2024_compatibility_authorization.json
```

The command uses `GITHUB_TOKEN` if supplied and the existing constrained,
read-only production loader. It validates exact report bytes/proposal integrity,
current-main membership of both scopes, current configuration and inclusion of
2024, then rechecks main. It grants no compatibility, acceptance or activation
authority. Network/provenance/current-scope failures propagate; there is no
fallback. Retain a successful result against the reviewed PR head before
claiming live production scope verification.

Both uploaded original TARs are now accessible locally. Their catalog hashes
match and independent technical qualification reproduces each complete retained
report exactly, including original limitations and its original
`historical_scope.status="missing_authority"`. That historical report field is
not rewritten to imply it originally had installed scope authority. No governance
argument is supplied to this technical rerun; current authority is checked
separately, preserving exact qualification identities.

| Source | Report file SHA-256 | Original TAR SHA-256 |
| --- | --- | --- |
| ACS1 | `c11e8cc403683582270be830092408220a21299f9e15dbb0d178016dccdaf8e1` | `7ea888ffb034decf6b9d118c029399922dccf1de2c74a0ca3d39d6720f76d600` |
| ACS5 | `23f02c9cf19cf93ea7b1fa277865076b9ce97170e55c7c9945c7dab7b9a571de` | `225bbe8ffd3a08b6ae049600f0e5fa05aa9132b83301fab54dbbcd591a3922fb` |

Each TAR is 40,960 bytes. TARs and newly computed technical results are not
committed or substituted for the retained reports. Qualification is advisory:
original-response verification remains not established, process-attested absence
is not a provider omission receipt, and manifest code provenance remains unknown.

## Exact remaining human authorization

`docs/proposals/acs_2024_compatibility_review_request.json` is a deterministic
review request for the complete proposed authorization ID and report/scope
commitments. All decision, reviewer, reviewed-commit, rationale, review-reference
and approval-provenance values remain null. This is not a signed approval,
authorization comment or installation adapter.

After the read-only current-scope check succeeds, an authorized human must
explicitly approve or reject the exact compatibility proposal at the exact
reviewed PR #302 head, identifying the proposal ID/hash, both qualification and
report-byte hashes and both installed scope IDs. They must explicitly review
original/reference policy differences, the missing manifest code recording and
independently recovered execution evidence, and accept the original assurance
limitations and process-attested monthly no-content classification. The decision
must preserve the 2024-only two-parent scope, the 67 ACS5-only fallback identities
and four jointly missing geographies, with no broader-vintage or operational
permission. A general policy endorsement or the earlier scope-only decision is
insufficient.

Use the repository's existing human-controlled reviewed-main process: retain the
genuine human GitHub decision/review reference, author, timestamp, exact reviewed
commit, exact proposal subject/hash and rationale as approval provenance. Do not
populate these fields with inferred, copied scope-only or model-generated review.
The original compatibility contract does not define a new signature/decision
service: current reviewed-main registry membership is the production boundary.
The initial-scope-only verifier from PR #305 is not a verifier or authorization
for this new compatibility subject and must not be repurposed to claim one.

Only following explicit authorization may a separate reviewed-main change set
the compatibility record's status to approved, add genuine provenance, recompute
its final semantic `authorization_id`, insert it under
`compatibility_authorizations[final_id]`, and set
`current_compatibility_authorization=final_id`. Approval changes the identity;
the proposed ID is not the installed approved ID. Preserve all existing
`scopes`, `gap_evidence`, `current_scope`, `states` and `current_state` values
exactly. Do not use an initial-registry overwrite. Initial-installation validation
is a one-off bootstrap check, not a recurring test that forbids a subsequently
reviewed compatibility registry extension.

The current PR does not perform those approval/installation steps. Merging its
inactive infrastructure/proposal alone cannot authorize compatibility. The
consumer subsequently requires the approved current-main record, both current
scopes, unchanged original reports and fresh qualification of both original TARs.
Physical-state acceptance, publication, serving and Macro remain separately
blocked even after bounded compatibility authorization.
