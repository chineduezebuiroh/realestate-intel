# Bounded ACS 2024 compatibility authorization

This implementation is inactive. The proposal inputs under `docs/proposals/`
are incomplete and deliberately cannot pass the consumer. They are not an
approval record. Complete reports and reviewed scope identities must be supplied
before a full record can be generated for human review. No production registry,
original package, manifest, pin, catalog, August identity or PR #299 is changed.

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

The generated record binds both complete semantic report digests and copies
qualification IDs, parent identities and data hashes, complete evidence
inventories, July publication and August acceptance lineage, original and
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
configuration. The entire fresh report must equal the unchanged retained report.
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

## Supplying the retained production reports

The human reports that the originals remain on their Mac under
`$HOME/acs-readiness-review/compatibility-301-code/acs1/` and the sibling `acs5/`
directory, named by the two exact qualification IDs in the proposal inputs.
Their reported qualification code commit is
`3d5b94dc6f51c4f3ec4b0a95bcea50bd3b8be086`. These locations and reported outcomes
do not substitute for inspecting the report bytes.

Attach those two unchanged JSON files to the review chat, or place copies in the
implementation workspace and supply their paths. No production TAR is needed
merely to construct the proposed record from retained reports. Once the report
bytes and governing scope IDs are available, generate and review the full
inactive record; do not change the status to approved during this task. Approval
and registry installation remain separate reviewed-main human actions. To test
the real-data consumer later, supply both original TARs from
`$HOME/acs-readiness-review/discovery-1/retrieved-packages/` and the required Git
objects/evidence to a suitable local workspace. Fixture suite success is not
production requalification or authorization.
