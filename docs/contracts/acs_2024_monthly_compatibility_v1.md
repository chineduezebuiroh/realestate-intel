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
binds the original catalog records, complete pin objects, cycle results and family
record. Both the checkout and any explicitly supplied catalog must preserve those
facts. Caller-editable catalog hashes cannot authorize different bytes. The TAR
SHA-256 must match the original record; manifest/content/data/validation hashes,
physical product and accepted family parent identities must agree. Original
snapshot digest, size, timestamp, endpoint, request identity, membership and
cycle-specific pin identity must be identical to the anchored pin and the
package's embedded pin. There is no September-parent substitution or pin rebinding.

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

The manifest's full original code commit must be available locally and an
ancestor of the repository evidence anchor. Original config hashes must match
that commit's files. The historical configuration reference is merged main commit
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
The inventory hashes the catalog, original TAR, package members, August pin/result
and family record. The original pin commitments, exact 446-cell reconciliation,
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
