# Proposed standard ACS1 2020 whole-vintage gap

This is documentary evidence and a proposed governance decision, not approval.
It preserves ACS1 2005–2024 and changes no production registry, accepted parent,
Source Set, qualification report, PR #302, serving artifact or Macro input.

## Authoritative provider evidence

Independently retrieved official Census pages establish three distinct facts:

1. [Changes for 2020 ACS 1-Year Estimates](https://www.census.gov/programs-surveys/acs/technical-documentation/user-notes/2021-02.html),
   technical user note labeled July 2021: the standard 2020 ACS1 estimates were
   not published because pandemic-related collection issues prevented them from
   meeting Census quality standards. This is explicit provider documentation,
   not inference from a missing API response or acquisition.
2. [Census Bureau Releases Experimental 2020 American Community Survey 1-Year Data](https://www.census.gov/newsroom/press-releases/2021/experimental-2020-acs-1-year-data.html),
   November 30, 2021, CB21-TPS.139: the experimental ACS1 release is a distinct,
   limited release. It does not establish availability of the standard product
   and is not substituted into this scope.
3. [New Statistics Available From the 2016–2020 American Community Survey 5-Year Estimates](https://www.census.gov/newsroom/press-releases/2022/acs-5-year-estimates.html),
   March 17, 2022, CB22-48: Census separately published the standard ACS5 release.
   The ACS1 2020 nonpublication claim does not apply to ACS5.

`docs/audit/acs1_2020_nonpublication_sources.json` retains publisher, exact URLs,
titles, publication labels, passage locators, short whitespace-normalized
excerpts and excerpt-byte digests. The observation date is recorded once in the
capture; generation does not refresh it or add timestamps. The capture's exact
file hash is pinned in `acs_2020_gap.py`. Its digest binds the captured metadata,
not the complete live HTML page or an independently signed provider attestation.
Reviewers can retrieve the cited pages independently. Changed or absent captures
fail closed; there is no network or inference fallback in generation.

## Record fields and identity

`census_acs1.2020.gap.proposal.json` uses the existing
`acs_governed_gap_evidence_v1` schema and semantic `evidence_id` rule. It contains:

- `source_id=census_acs1`, `product=acs/acs1`, `vintage=2020`, production namespace,
  explicit `status=proposed`, null `approval_provenance`, and no activation.
- Provider evidence classified as standard whole-vintage nonpublication, backed
  by the three exact official references and pinned documentation capture.
- `requested_provider_release_id=acs/acs1:2020` and the ordinary requested
  endpoint identity. This is the repository's requested product/vintage label,
  not a claim that Census advertised or published a dataset with that ID.
  `provider_published_dataset_identifier` is null; no endpoint response is asserted.
- Both governed variables, the 223-geography request membership digest, ten exact
  exclusions, source/vintage and all current configuration digests. Nonpublication
  applies to the standard product; it does not label geographies provider-ineligible.
- Explicit exclusions of experimental ACS1/ACS5 substitution and of missing
  acquisitions, HTTP 404/204 or geography omissions as a whole-vintage inference.

Existing `ACSGovernance.scope()` requires gap schema, source, vintage, namespace,
semantic identity, nonempty provider evidence, genuine approval provenance and
membership in the approved registry. Its available and gap years must exactly
partition the approved range, without duplication or overlap.

The existing generic gap schema does not prescribe a provider-release identifier,
request-scope structure, citation schema, document signature or automatic source
verification. This bounded proposal validator adds exact review commitments for
these fields; it does not change the production scope/gap contracts. Human
review must assess documentary support, standard/experimental product identity,
request applicability and provenance. A self-consistent hash authenticates none
of those decisions by itself.

## Conditional partition, without approval

The existing `governed_gaps` semantics require approved evidence, so the base
ACS1 scope proposal is not populated with an unapproved governed gap. Instead,
its wrapper references the proposed gap and the separate
`census_acs1.scope.if-gap-approved.json` candidate. The candidate preserves
2005–2024, lists 19 expected available vintages, and references 2020 through the
proposed evidence identity. It is structurally complete but null-approved.
All production `scopes`, `current_scope`, `gap_evidence`, `states` and
`current_state` maps remain empty inside the review-only registry template.

The test-only namespace is used solely to demonstrate that the conditional
partition satisfies the unchanged existing scope validator when both scope and
gap have explicit offline fixture approvals. Proposed production records still
fail approval checks, and fixture governance cannot authorize production.
`status=proposed` is an explicit review label, not a new enforcement mechanism:
the existing consumer does not inspect gap status. Null provenance, absence from
active registries, current-main verification and the existing capability model
remain the approval boundary.

## Exact remaining human decisions

1. Independently verify the Census references and captured passages. Approve or
   reject the proposed standard ACS1 2020 whole-vintage unavailable treatment;
   record real reviewer identity, decision, review reference and evidence hashes.
2. Separately approve ACS1 historical scope 2005–2024, its variables, geographies,
   exclusions and complete available/gap partition. ACS5 2009–2024 remains a
   separate scope decision; 2023 is still an expected but missing acquisition.
3. Only after explicit authorization, finalize gap status/provenance and compute
   its new `evidence_id`. Rebind the scope to that final approved identity, add
   genuine scope provenance and recompute its final `authority_id`. Conditional
   candidate/proposal IDs are not approved identities and must not be reused
   after content changes.
4. Install only those explicitly approved records and pointers through a separate
   human-reviewed main-registry change, following the bootstrap runbook. No such
   installation, approval or pointer mutation occurs in this PR.

Scope/gap approval is separate from PR #302's two-package 2024 compatibility
exception, physical-state acceptance, Source Set publication and downstream
activation. None is supplied or implied here.
