# Initial ACS historical scope proposals

This is proposal generation and validation only. Nothing here grants authority,
installs `config/acs_historical_governance.json`, advances a pointer, accepts
physical state, or authorizes compatibility, materialization, serving or Macro.
The proposed scope content hashes are not established scope authorities.

## Generate and validate

From the repository root:

```sh
python -m jobs.monthly_refresh.acs_scope_bootstrap --output /tmp/acs-scope-review
python -m jobs.monthly_refresh.acs_scope_bootstrap --check /tmp/acs-scope-review
python -m jobs.monthly_refresh.acs_scope_bootstrap --check docs/proposals/acs_historical_scope_bootstrap
```

Generation requires a new directory and refuses the production `config` tree.
There is no approve, install, publish, materialize or activate command, no network
transport and no credential use. Check mode is read-only. Successful validation
means exact, deterministic review inputs; its output explicitly reports no
production authority, physical-state acceptance or activation. It does not mean
the proposed scopes pass the approved-scope contract.

Three review artifacts are generated:

- `census_acs1.scope.proposal.json`: wrapper plus existing-contract scope record,
  range 2005–2024, null approval provenance and explicit unresolved 2020 decision.
- `census_acs5.scope.proposal.json`: wrapper plus existing-contract scope record,
  range 2009–2024, complete proposed partition and null approval provenance.
- `initial-registry.proposal.json`: wrapper containing an empty registry template,
  hashed references to the two separate proposals, current configuration/input
  hashes and generator digest. All active `scopes`, `current_scope`,
  `gap_evidence`, `states` and `current_state` maps are empty.

Do not copy the wrapper or its scope proposals into the active registry. The
production loader does not define a pending/approved status on historical scope
records: reviewed-main membership plus current pointer and genuine approval
provenance are the existing authority boundary. Keep unapproved proposals outside
those active maps, even after this implementation merges.

## Existing contracts and sources

Use `jobs/monthly_refresh/acs_scope.py` and
[historical materialization v2](../contracts/acs_historical_materialization_v2.md).
Scope records use `acs_historical_scope_authority_v1` and the existing
`identify`/`check_identity` rules, not a new approval identity system. Every field
other than `authority_id`, including approval provenance, participates in the
semantic SHA-256. Adding a real approval therefore changes the final scope ID.

The review wrapper is only an artifact format, not a governance capability.
Validation recomputes exact proposals from repository inputs, rejecting injected
approvals, gaps, scope changes, configuration drift and active pointers. Existing
`ACSGovernance` remains the sole production consumer; tests exercise its actual
approval, partition, namespace and evidence-membership checks.

Adopted `config/acs_backfill_reviews/plan-*.json` and original August cycle
results establish the intended range evidenced by the repository. They do not
approve that range. The generator checks that their range bounds agree with the
explicit intended ACS1 2005–2024 and ACS5 2009–2024 scope.

Physical product/variable mappings are in `sources/census_acs/artifact.py` and
`config/source_metric_registry.csv`. Both products propose population
`B01003_001E` and median household income `B19013_001E`, under physical product
metric IDs. Geography comes from `config/geo_manifest.generated.csv` through
`geography_plan()`: 223 requested geographies (163 counties, 54 CBSAs, five states,
one nation). The ten Metropolitan Divisions remain explicitly excluded under
`CANONICAL_CONCEPT_MISMATCH`; the exact exclusion records are retained.

The four existing `governed_config_hashes()` inputs bind geography, physical
metrics, source revision policy and monthly policy. Proposal files also pin
request membership, planning evidence, catalog and product-code digests. No
provider payloads, source observations or original qualification reports are
copied into these artifacts.

## Unresolved partition and coverage

The existing scope contract requires expected available vintages and approved
evidence-backed whole-vintage gaps to partition the inclusive range exactly.
It has no approved unresolved-year category.

ACS1's proposed available list contains the intended years except 2020. Its
`governed_gaps` is empty; 2020 is recorded only in the wrapper's unresolved
partition and decision list. This record is deliberately incomplete and cannot
become valid approved scope by filling in reviewer provenance. Independent
provider evidence and a separate human-reviewed decision must establish a valid
`acs_governed_gap_evidence_v1` record for the unavailable vintage, or independently
justify a different available-vintage decision. No such evidence/decision is
manufactured here. If an unresolved year is to be allowed in approved scope,
that would require a separate explicit contract change; this PR does not do it.
Do not narrow to 2024 or call 2020 acquired to bypass this blocker.

ACS5's proposed available list includes 2023. Missing catalog acquisition is not
provider unavailability and is not a governed gap. The scope can be represented
under the existing contract without that acquisition being complete, but remains
unapproved. Package acquisition/verification, scope approval and physical-state
acceptance are distinct gates. Catalog presence in a proposal is informational,
not a claim of verified bytes, complete coverage, accepted inventory or state.
No ACS1 2020 or ACS5 2023 implementation is included.

## Human review and remote-main installation

This is a documented later human process, not an action performed by the tool:

1. Review the generated range, variables, membership/exclusions, configuration and
   input hashes in a separate scope-governance PR. Preserve the unresolved ACS1
   blocker until independent evidence and an explicit human decision resolve it.
2. Have the repository's authorized human governance reviewers decide each scope
   and any whole-vintage gap independently. Record genuine reviewer identities,
   decision, review/PR references and exact evidence commitments in approval
   provenance. Do not use a model-generated reviewer or a proposal hash as an
   approval. The repository's existing reviewed-main process supplies trust;
   the Python validator does not authenticate reviewers or signatures.
3. For a valid, explicitly approved scope only, construct the existing-contract
   scope record with real provenance and the resolved complete partition. For
   any approved gap, construct the independently reviewed gap record and compute
   its `evidence_id` before referencing it. Recompute final scope `authority_id`
   with existing semantic rules after every change. Check the production record
   identity with `check_identity`. For offline structural regression, use an
   explicitly `test_only` copy (recompute its separate fixture identity) through
   `ACSGovernance.from_fixture(...).scope(...)` to exercise the existing partition,
   configuration and gap-membership checks. Such a fixture proves neither
   production approval nor reviewer authenticity. Keep any remaining unapproved
   product outside active maps; fixture records must never be installed.
4. In a separately authorized reviewed-main change, initialize the existing
   `acs_historical_governance_registry_v1` file with production namespace. Add
   only approved gap records to `gap_evidence`, approved scopes keyed by their
   exact final IDs to `scopes`, and corresponding `current_scope` pointers.
   Keep `states` and `current_state` empty. Review the exact file/digests and
   current main before installation; normal protected-main PR merge is the
   initial registry commit, not an unreviewed runtime writer. If a registry has
   appeared meanwhile, reconcile it rather than overwrite or rebootstrap it.
5. After authorized installation, use `ACSGovernance.load_production` to resolve
   remote main and validate each installed scope against current configuration.
   A local file, raw dictionary, digest or `from_fixture` instance cannot prove
   production approval. Any later configuration or scope change requires fresh
   review/reconstruction under the existing contract.

Scope approval does not authorize the 2024 compatibility exception. Once genuine
scope IDs exist, PR #302 can separately bind them into its inactive compatibility
proposal for review; it is not modified by this PR. Full readiness must still
verify every required original acquisition, appropriate independent-provider or
separately authorized compatibility evidence, complete physical inventories and
continuity. Later physical BOOTSTRAP, publication and atomic state advancement
need their separately reviewed production procedure. Source Sets, serving and
Macro remain further authorization gates.
