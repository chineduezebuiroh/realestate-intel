# ACS 2024 monthly-to-historical compatibility qualification v1

This is a read-only technical qualification contract for exactly the original
August-cycle physical parents:

- ACS1: `src__census_acs1__2024-12__r1__a1f5cd392af90fcb`.
- ACS5: `src__census_acs5__2024-12__r1__2bebdc938d8c2fb6`.

Technical reconciliation does not grant historical eligibility or acceptance.
`compatibility_governance` remains `not_approved`, including when an offline
historical registry is supplied. `original_provider_response_independently_verified`
always remains `not_established`. No approval flag, caller report, alternative
parent, fixture namespace, snapshot reconstruction or omission-receipt generation
is supported. This change supplies no production approval record or writer.

## Evidence and acceptance predicates

`jobs.monthly_refresh.acs_2024_compatibility` reads an original uncompressed TAR,
the catalog, preserved August results/pins and accepted family record. It extracts
only into a temporary directory with the existing strict package extractor.
The TAR bound is 512 MiB; JSON inputs are bounded at 32 MiB. Original inputs are
never modified or repackaged. No network or credentials are used.

Immutable repository anchor `40b950b4edda9dff30a93754735654aab0da28e1`
binds the original catalog records, complete July publication and August acceptance
pin objects, both cycle results and the family record. Both the checkout and any
explicitly supplied catalog must preserve those facts. Caller-editable catalog hashes cannot authorize different bytes. The TAR
SHA-256 must match the original record; manifest/content/data/validation hashes,
physical product and accepted family parent identities must agree. Original
snapshot digest, size, timestamp, endpoint, request identity, membership and
cycle-specific pin identity in the manifest must exactly match the anchored July
publication pin. The manifest cycle and retrieval timestamp remain July provenance.
There is no September-parent substitution or pin rebinding.

The confirmed monthly reuse contract preserves the original package: July and
August cycle results name the same artifact/content/package identities. August
acceptance is checked separately against its exact anchored result and pin. Both
pins must have identical product/release identity and complete snapshot member
objects except for `retrieved_at`: snapshot SHA-256, size, endpoint and the entire
request/geographic evidence object must match exactly. Both full cycle-specific
pins remain independently anchored and validated; neither is rewritten. This is
restricted to the confirmed July-to-August reuse of the two named parents, not a
general rule allowing arbitrary prior-cycle provenance.

Actual TAR rows are inspected independently. The 223 requested geographies must
partition uniquely and without overlap into the recorded available and no-content
lists. Ten Metropolitan Division exclusions remain separate. The exact cross
product of available members and `B01003_001E`/`B19013_001E` must equal the actual
canonical geography/metric keys. ACS1 requires 152 available, 71 recorded
no-content and 304 rows; ACS5 requires 219 available, four recorded no-content and
438 rows. Counts are assertions in addition to exact key comparisons. Duplicate,
missing, extra, invalid or unexplained cells fail. Dates must be precisely
2024-12-31, source IDs must agree and both property fields must be `all`. Values
must be numeric, finite and outside the three physical sentinel values. Manifest
coverage diagnostics must agree with this partition. The request-plan identity
is recomputed from configured geography codes, safe URLs and recorded statuses.

When the manifest records a full original code commit, that commit must be
available locally and an ancestor of the repository evidence anchor. For only the
two named original packages, the known manifest value `git_sha="unknown"` may
instead be resolved through the fixed original workflow evidence described below.
Missing fields, other placeholders, unmatched packages or altered evidence still
fail. Original config hashes must match the independently identified original
source files in either path. The historical configuration reference is merged main commit
`4beea6b8b860cc1f825bd0f6b33728acbc90c9dc`; all four current governed config
files must match it. Original geography and metric configuration must match that
reference exactly. Policy-file hash differences are enumerated for explicit human
review; technical reconciliation cannot approve their semantics. Unknown current
configuration drift fails closed and requires a separately reviewed contract
revision, rather than silently advancing the reference.

AST comparisons bind original acquisition and transformation behavior to known
monthly implementation `47ff8c6bc2ff5a73ca36a6d0e40951faaa59ffa4`, and compare
acquisition, geographic planning, request construction, transformation and relevant
constants with the historical reference and checkout. Original candidate processing
must match the known monthly implementation. This is a bounded compatibility
check under trusted repository/runtime assumptions, not a Python security sandbox.
Original and reference code byte hashes and semantic hashes are retained.

Optional offline historical governance is compared for production registry schema,
current source scope identity, product, namespace, approval provenance, configuration
hashes and inclusion of 2024. It cannot establish current-main provenance or approve
compatibility. Full scope/gap governance remains the existing production boundary;
this tool does not reinterpret other vintages or gaps.

## Deterministic report and operation

From a complete Git checkout, with the project Python environment active:

```sh
python -m jobs.monthly_refresh.acs_2024_compatibility \
  --repository-root "$PWD" --source-id census_acs1 \
  --package /absolute/path/original-acs1.tar \
  --output /absolute/path/new-acs1-qualification
```

Run separately for ACS5 using `--source-id census_acs5` and its original TAR.
Optional `--catalog` selects a retained catalog; `--governance` selects an existing
offline registry. Use a fresh output directory outside the repository. No downloads,
provider calls, publication or approval are performed. Missing Git evidence objects
fail closed; obtain a complete authorized repository checkout separately.

The report schema is `acs_2024_monthly_compatibility_v1`; its `qualification_id`
is the schema plus the full SHA-256 of canonical report JSON excluding that ID.
The sole output filename is `<qualification_id>.json`. Identical evidence produces
identical output without clocks, absolute input paths or temporary-path errors.
The inventory hashes the catalog, original TAR, package members, July publication
pin/result, August acceptance pin/result and family record. `provenance.original_publication`
and `provenance.august_acceptance` expose both layers separately. `original_pin`
contains the July publication pin; the August pin is preserved under acceptance.
The original pin commitments, exact 446-cell reconciliation,
excluded members, canonical row hash, predicates, original/reference configuration
and code identities, limitations and approval requirements are included. Invalid
inputs produce deterministic failure reports. Exit 1 means technical failure;
exit 2 means technical reconciliation passed but governance remains pending.
There is deliberately no successful approval/activation exit code.

## Human governance process and residual limitations

1. An independent reviewer verifies the original TAR evidence and reruns both
   qualifications. Review the exact identities, evidence hashes and complete
   cell classifications, not just row counts or a self-consistent report hash.
2. Review each policy difference, original code provenance and the intended
   historical scope. Any semantic incompatibility blocks eligibility. Resolve
   differences by an explicit decision, never by rewriting the August parents.
3. In a separate human-reviewed main-branch governance change, explicitly decide
   whether these exact parents and qualification IDs may be used under the monthly
   evidence class. Record reviewer/approval provenance, policy decisions and the
   limits below. Existing scope approval alone is not compatibility approval.
4. A future reviewed production boundary must independently load that authority
   from main and verify the exact evidence before historical acceptance. This
   work package implements neither that authority adapter nor acceptance. The
   offline validator cannot consume a local claim of approval as authority.

The original normalized snapshots are missing. Their hashes and sizes are
commitments, not byte verification. Monthly acquisition code recorded HTTP 204 as
`provider_ineligible_no_content` without the historical empty-body validation.
Those classifications remain process attestations and cannot become synthetic
historical omission receipts, geographic eligibility claims or whole-vintage gaps.
Monthly response-shape and canonical validation did not independently establish
exact historical headers, returned geography codes, discovery identity or variable
metadata. Complete retained-output accounting is therefore a distinct assurance
class from independent original-provider-response verification. Agreement with a
future fresh capture would corroborate data at a new verification date, without
proving the original September 2026 HTTP bytes.

## Historical readiness integration

The existing readiness input accepts an optional list:

```json
{"monthly_compatibility": ["census_acs1:2024", "census_acs5:2024"]}
```

Use the existing exact `packages` mapping. Readiness recomputes qualification from
those TARs; it does not trust a supplied qualification report or status. Other keys,
duplicates, dictionaries containing approval claims and other vintages are rejected.
The explicit lane does not ignore contradictory independent provider evidence.

Readiness includes `monthly_compatibility_qualifications` and reports the exact
cells as `available_compatibility_reconciled` or `monthly_no_content_attested`.
Their technical status is separate from `compatibility_governance=not_approved`
and independent verification. These cells do not enter the independently verified
parent inventory, resolve ACS1 preference, authorize ACS5 fallback or pass source
availability. Historical acceptance and downstream activation remain blocked.
Absent explicit opt-in, the existing missing-provider-evidence behavior is retained.
All independent provider validation and historical gates for other vintages remain
unchanged. No production configuration, artifacts, acquisition, Source Set,
serving, Macro or HTML behavior is changed.

## Confirmed publication-reuse regression

Mac-local inspection of both production TARs confirmed the original catalog/TAR/
manifest/data identities, exact July embedded pin/cycle/retrieval metadata, and
August acceptance of the same package and snapshot evidence. The initial validator
incorrectly required the original manifest to contain August publication metadata.
`exact_original_pin_binding` now requires exact July publication provenance; the
separate `exact_august_snapshot_and_geographic_evidence_binding` predicate preserves
all August source-input commitments. Both result bindings remain exact. The original
TARs, pins, catalog records and retained-row/configuration/governance checks are
unchanged. Rerun both qualifications into fresh directories: validator and report
identities change, and earlier reports are not silently upgraded.

## Original acquisition code recording omission

`original_code_commit_recorded` originally required `manifest.git_sha` to be a
40-character commit SHA. The original hosted runner called the candidate builder
without its `git_sha` argument, leaving the builder's literal `"unknown"` default.
The publisher received `GITHUB_SHA` separately; it did not rewrite the acquisition
manifest. The manifest therefore genuinely omitted an acquisition commit. It must
not be relabeled as having recorded one.

Independent original execution evidence is retained in
[acs_2024_original_workflow_code_evidence.json](../audit/acs_2024_original_workflow_code_evidence.json).
This is captured GitHub execution metadata and log excerpts, not a synthetic
provider snapshot, omission receipt or approval. Its exact SHA-256 is pinned in
the validator. It identifies successful original run `35567244220`, attempt 1,
ACS1 job `106231601318` and ACS5 job `106231601412`. Each job log records checkout
`9131dc8ad2626f393e7975411693908d43445173` and the complete publication result,
including the exact original artifact/content/package hashes and July cycle.
The result must equal the independently anchored July result in full. Full
retrieved log-content SHA-256 values and exact checkout/result excerpts are
retained for review; complete runner logs are not copied into the repository.
The evidence is based on GitHub's observed run/log service, not a signed execution
attestation. Hashes bind the retained capture; they do not confer approval.

That checkout was on `monthly-refresh-orchestration`, not main. GitHub's comparison
with the immutable repository baseline reports divergent histories, with merge base
`42baead0f29f844132979017998e71d89a16dddf`. The original workflow deliberately
allowed the checked-out ref to supply execution code while durable writes targeted
main. The validator does not claim this execution commit is a main ancestor or
replace it with the later main commit. Instead, this fixed, package-specific
workflow evidence binds its exact Git blob identities for both acquisition files,
the physical source implementation and all four governed configuration files.
Those original bytes are read from the local Git object database and their Git
blob identities are recomputed. Missing objects fail closed. No network fallback
is used during qualification.

The manifest configuration hashes must still equal the original file SHA-256
values. Current configuration-reference checks, geography/metric equality,
policy-difference review and original-versus-historical code semantics checks are
unchanged. No code compatibility is inferred just from a successful workflow.
The manifest-recorded commit path retains its existing main-ancestry requirement;
this alternative cannot authorize any other missing-code acquisition.

The qualification report retains `manifest_git_sha="unknown"`,
`manifest_code_commit_recorded=false`, the independently recorded original commit,
the workflow evidence identity and the divergent baseline relationship. This is
external recovery of code provenance, not a backfill into the package. Separate
human compatibility governance may review the manifest omission as a documented
limitation; governance and original provider-response verification statuses remain
unchanged. Retrieve the original repository commit into the local Git cache before
rerunning if needed:

```sh
git fetch origin 9131dc8ad2626f393e7975411693908d43445173
```

This fetch reads repository code/history only. It does not acquire Census data or
change the selected parent, original package, pins, catalog or production state.
Use new qualification output directories because report identities change.
