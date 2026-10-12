# Governed ACS historical materialization v2

This ACS-only layer sits between immutable single-vintage acquisition and ACS1
preferred / ACS5 fallback family resolution. It does not change physical
acquisition, canonical assembly, serving, Macro policy, or monthly scheduling.
There is no live bootstrap, publication, or pointer change in this implementation.

## Independent authority

`acs_historical_scope_authority_v1` binds physical source/product, inclusive
historical range, exact expected available vintages, governed gaps, the existing
geography/metric/policy configuration hashes, namespace and approval provenance.
Its `authority_id` is a full semantic SHA-256. `acs_historical_inventory_v2`
references that authority and exactly one immutable acquisition identity per
expected vintage (artifact ID, full content/package/data hashes). An inventory
cannot declare its own range or gaps; a 2024-only inventory cannot satisfy an
approved 2005–2024 range.

`acs_governed_gap_evidence_v1` binds product, vintage, provider evidence and
independent approval provenance. Evidence must be a member of the approved
control-plane evidence registry, not merely have a self-consistent caller hash.
A scope's available vintages and evidence-backed gaps must exactly partition its
approved range. Missing evidence, superseded scope and configuration drift fail
closed. Geography-specific provider absence remains acquisition evidence and is
not converted into a whole-vintage gap.

Production `ACSGovernance.load_production` resolves `main` to a commit, then reads
`config/acs_historical_governance.json` at that commit through a newly constructed read-only
GitHub control-plane transport. Local/worktree files and raw production dictionaries
are not accepted as authority. Production approval is membership in that
human-controlled main registry, following the repository's main-branch authority
model; an arbitrary hash or approval string in a caller proposal is insufficient.
`from_fixture` accepts only the explicit `test_only` namespace, which cannot
pass publication or hosted cohort preparation. These are application boundaries,
not a sandbox against arbitrary Python code replacing governance implementations.

The registry schema is `acs_historical_governance_registry_v1` with `namespace`,
`scopes` keyed by authority ID, `gap_evidence` keyed by evidence ID,
`current_scope` keyed by physical source, `states` keyed by full state ID, and
`current_state` keyed by physical source. Missing production registry fails
closed. This task deliberately supplies no approved production scope/evidence.

## Lifecycle and complete physical state

`acs_physical_historical_state_v2` binds the inventory plus semantic canonical
rows and row lineage. BOOTSTRAP requires approved current scope and no current
state. UPDATE requires exact equality to the current governed prior record;
missing, stale and wrong priors fail. Every retained vintage must keep its exact
acquisition identity. Scope narrowing and surrogate replacement fail even if
new scope approval exists. REPLAY requires the identical inventory and state;
it cannot narrow or extend scope. Passing `prior=None` cannot rebootstrap an
already governed product.

Every state is reconstructed from all immutable acquisition packages. The prior
record is a reconciliation baseline, never a recursive data parent. Each parent
must satisfy `published_immutable_verified`, `artifact_status=complete` and
`validation_status=passed`, including validation-file status, manifest/catalog
agreement and verified package bytes. Rows must match the product's metrics,
governed geography, `{vintage}-12-31` observation and `all` property semantics.
Duplicate/conflicting physical keys fail. Legitimate distinct vintages survive.

No correction/replacement policy for an already retained ACS vintage is invented.
Any changed acquisition identity for that vintage fails closed; historical
Census correction requires separate reviewed governance.

## Independent logical validation

`acs_logical_historical_source_v2` uses the existing observation-key resolver:
ACS1 wins each available key, otherwise ACS5. Independent reconstruction verifies
both physical states against their complete acquisition inventories and current
governed identities, reruns selection, then compares exact canonical rows,
values, lineage and required metadata with the candidate package. Candidate
manifest diagnostics are not evidence of completeness.

Lineage is reconstructable:

logical row → winning physical historical state → inventory → immutable
single-vintage acquisition parent → vintage and approved scope authority.

The row includes physical source/metric, historical artifact/content identity,
semantic state ID, inventory/authority IDs, acquisition artifact/content/package
hashes and vintage, alongside ACS1/ACS5 availability, values and reason codes.
`acs_family_resolution_record_v3` contains the independently verified context
and exact published logical identity. Record construction and durable store
writes validate the package again. `publish_family` checks production governance
and independently validates before calling the shared immutable publisher.
Hosted cohort preparation performs the same reconstruction before Source Set
construction or publication. v3 logical-plan construction also requires package
validation and matches cycle acquisition candidates to historical inventories,
rather than confusing an acquisition ID with a historical-state ID.

Legacy v1 frame/build and pure cohort-planning APIs remain for archived offline
proofs; they do not confer historical completeness. Manifest-only authoritative
record construction is retired. Hosted publication/preparation refuse legacy
single-vintage ACS. Existing frozen records and artifacts are not rewritten.

## Determinism and follow-up boundary

Full authority, inventory and state identities hash canonical semantic JSON;
float values retain round-trip precision. Physical state and logical source
semantic hashes do not depend on Parquet metadata or encoding. The shared source
artifact/content and package hashes still include serialized Parquet bytes.
PyArrow is not pinned by this repository, so byte equality across arbitrary
runtime versions is not guaranteed. Artifact creation time is operational;
tests can supply a fixed clock, while normal builds record actual creation time.
REPLAY preserves semantic state/identity; immutable published packages must be
resolved/reused under the existing catalog reuse contract, not overwritten.

Separate production follow-ups must approve scope and independently reviewed
gap evidence on main, acquire/accept every required vintage, bootstrap physical
states, publish exact packages, and atomically advance the historical governance
registry with catalog/package identities under compare-and-swap protection.
No registry writer, workflow activation or production bootstrap is enabled here.

Steady-state automation must load current governance from main and use
`materialize_current(mode="UPDATE", ...)`, which resolves the current baseline
automatically. It must select complete immutable acquisition parents, publish
verified state packages, reread main and reject concurrent scope/state changes
before CAS advancement. It must then resolve historical artifact IDs from durable
catalog identities, build and validate ACS family output, and pass the resolver
and governance to v3 logical planning/cohort preparation. Human-entered prior
state IDs are not the steady-state interface. Workflow wiring, atomic registry
advancement, governance approvals and production execution require a separate
reviewed activation phase; this task changes no workflow YAML.

## Authoritative boundary hardening

Production governance is loaded by `ACSGovernance.load_production(repository,
 token)` using a newly constructed read-only GitHub transport, resolving `main`
once and fetching the exact registry at that commit. Caller-supplied transports
are rejected by the retired `from_main` interface. Fixtures have a separate
constructor and cannot satisfy `require_production`, even after relabeling their
namespace/provenance. A private process-local capability records the remote
snapshot identity and checks that its content has not changed. This boundary
assumes trusted application code, Python runtime, HTTPS transport and the approved
repository/main governance process; it is not a security sandbox against code
that monkeypatches private internals. Semantic SHA-256 hashes prove content
identity, not review or approval. No local-file or offline fallback exists.

`acs_reconstruction_attestation_v1` carries the exact remote repository, main
commit, registry hash, complete ACS v3 resolution and full SHA-256 of the target
plan/prepared promotion. Its semantic identity is also a full SHA-256. Evidence
is not a signature and is never sufficient by itself: the dedicated ACS boundary
independently reconstructs the logical package before issuing a nonserializable,
process-local validated capability. The authoritative durable-plan store requires
that capability bound to the exact plan before even reading the store. Pure plan
validation remains structural and performs no remote access; its result is not
an authorization to store. The plan-store CLI explicitly loads the current remote main
snapshot and reconstructs before storage. The plan ID includes the evidence;
the evidence target hash excludes the plan ID and evidence to avoid recursion.

Prepared promotions extend only the hosted envelope with this evidence; generic
promotion schemas and transition semantics are unchanged. Promotion evidence also
requires verification of the immutable source-set and canonical packages. The
source-set ACS output, resolution ID and physical-parent identities must match
the independently reconstructed record; every target source must match the
source-set. The canonical manifest must bind that source-set semantic identity
and package hash and match the prepared canonical hash. Evidence binds the
complete prepared object, including expected-old and target pointers.

Live and recovery execution load the existing envelope, reject legacy/unattested
ACS, load its exact governance snapshot, independently reconstruct ACS and verify
the immutable object graph before reaching the executor. A pinned snapshot must
still be an ancestor of current remote main and have the exact recorded registry
hash. Recovery does not reprepare or rewrite the prepared object, and can resume
or classify an already completed promotion as a no-op. Each hosted execution and
prepared-record storage entry point requires the exact process-local capability
before an external write. The authorization token includes the evidence.
Non-ACS records retain their existing behavior. Legacy ACS promotions require
separately reviewed preparation and cannot be automatically upgraded by recovery.

Registry consistency is checked before materialization: semantic state IDs must
equal registry keys; inventory identities/namespaces/products must agree;
existing history requires a current pointer; pointers must resolve exactly to
that product. Current state must retain every immutable acquisition parent from
all stored states of the product, rejecting stale pointers, conflicting history
and ambiguous metadata. Duplicate JSON keys are rejected at the production
loader. Historical Census corrections remain unsupported and require separate
governance.

Workflow activation, approved governance publication/bootstrap, and the
acceptance/CAS adapter that advances current historical state remain separate
follow-ups. No production workflow is activated here. Steady-state materialization
still resolves the current state through `materialize_current`, without a
human-entered prior-state ID. Production execution requires the governed remote
snapshot and immutable packages to be available; failures stop rather than
weakening trust or rebuilding history from prior-state rows.

Pinned snapshot capabilities are recovery-only. They cannot authorize new
publication, new prepared evidence or durable-plan storage; those boundaries
require current main governance. Only revalidation of an already bound promotion
may use historical main ancestry. A changed snapshot requires new preparation
for a new plan, while recovery preserves its original evidence.

Promotion evidence explicitly includes the ACS, source-set and canonical catalog
content/package identities. Every execution step compares those bindings with
the freshly read catalog before attempting CAS, preventing identity drift between
validation and a recovery operation. Recovery-only capabilities cannot create
prepared records or authorize durable-plan storage; mixed target kinds reject.

## New-write governance fence and repository binding

Legacy compatibility is read-only: every durable cohort plan contains ACS and
must carry a v3 reconstruction attestation and its exact process-local capability
to authorize a store write, regardless of the caller's schema label. Removing or
downgrading a schema cannot authorize storage. The August NRC correction adapter
fails closed for ACS-containing plan writes before publishing its NRC correction;
its legacy raw JSON writer is not an ACS authorization boundary.

Capabilities retain the independently loaded governance object, including its
repository identity. Family-resolution storage/publication, cohort-plan storage,
prepared-object storage and promotion execution reject a different destination
repository before destination I/O. Fixture governance cannot issue these rights.

New authoritative writes reread remote main and its registry through the retained
constrained read-only production transport. An unchanged registry on a later
unrelated main commit is valid; any changed registry requires new reconstruction.
ACS-facing stores and ACS-related catalog publication create a Git tree/commit
with that verified main tip as its sole parent, check the destination blob at that
exact tip, then update main with `force=false`. A concurrent main advance makes
the proposal non-fast-forward and rejects the authoritative write. No automatic
rebase masks a governance advance. Concurrent failures may leave unreferenced Git
objects or staged immutable release packages; they cannot advance authoritative
main records. New publication uses this fence for catalog acceptance as well as
resolution, plan and prepared-record persistence. Generic Contents/CAS primitives
are unchanged and remain trusted application-code tools.

Recovery verifies the durable envelope, its pinned main ancestry and immutable
packages, then uses the existing pointer/CAS reconciliation. It intentionally
does not require the old registry to remain current and cannot create new prepared
objects or durable plans. Recovery still requires current local geography/metric
configuration to match the pinned authority; configuration drift fails closed.
Removing that availability limitation is a separately reviewed production-
activation follow-up, not permission to substitute configuration or governance.
