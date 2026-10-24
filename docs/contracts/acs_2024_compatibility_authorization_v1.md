# Bounded ACS 2024 compatibility authorization

This implementation is inactive. The proposal inputs under `docs/proposals/`
remain incomplete because authoritative historical scope identities are absent.
They deliberately cannot pass the consumer and are not an approval record. The
two uploaded production reports are retained unchanged and their evidence
bindings are complete; no scope identity is invented. No production registry,
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
contains exact relative paths, byte hashes and sizes; it binds the complete
semantic report plus compact lineage section digests. Neither user-reported
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
reports, resolve the proposal's `retained_report.path` under that checkout, and
supply those paths in readiness's `compatibility_authorization.reports` map.
The consumer checks the byte hash/size and exact qualification identity before
independent requalification. A later reviewed-main authorization must bind these
same byte commitments; being present in Git alone does not approve the reports.

At inspected remote main `34ed70b2125b45c449086bccc4179b7ac4c32960`,
`config/acs_historical_governance.json` is absent in the Git tree and its GitHub
Contents lookup returns 404. Both unchanged reports explicitly record
`historical_scope.status="missing_authority"`. The original August pins, cycle
results and accepted family resolution provide acquisition/acceptance lineage,
not historical scope authority IDs. Exact governing scope IDs for both products
are therefore **unavailable**, not inferred from backfill plans, monthly policy,
family identity or policy endorsement. The proposal retains null scope IDs and
no authorization ID. Separate human-reviewed governance must establish the
registry and actual scope IDs before a complete inactive authorization can be
constructed. This task does not create or approve scopes.

No original production TAR exists in this workspace. Read-only retrieval of the
cataloged numeric Release assets was attempted and returned `Forbidden` for both:

| Source | Release ID | Asset ID | Required package SHA-256 |
| --- | --- | --- | --- |
| ACS1 | 392729325 | 578392031 | `7ea888ffb034decf6b9d118c029399922dccf1de2c74a0ca3d39d6720f76d600` |
| ACS5 | 392729171 | 578391228 | `225bbe8ffd3a08b6ae049600f0e5fa05aa9132b83301fab54dbbcd591a3922fb` |

Each original TAR is 40,960 bytes according to its retained qualification.
The proposal includes the exact cataloged filenames, tags and source identities.
Provide unchanged files from the Mac's
`$HOME/acs-readiness-review/discovery-1/retrieved-packages/` or authorized access
to these existing assets for real-data requalification. Do not reacquire source
data or reconstruct packages/reports. The original execution Git objects,
workflow evidence and original code/configuration bytes remain required by the
qualifier; they must be available for that later run as well.

Report-integrity and repository-lineage verification passed. TAR-member
reverification, live authority validation and real-evidence readiness did not run
to completion because the original packages and historical scope authority are
unavailable. No production report was regenerated, no proposed record was
approved, and no historical acceptance or activation occurred.
