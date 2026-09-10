# ACS-B local provider verification follow-up v0.2

**Status:** `ACS_B_COMPLETE_PHYSICAL_CONTRACTS_FROZEN` (2026-09-10)

**Decision:** **ACS-B is complete.** The corrected physical fact acquisition and
the exact credential-free `summary.json` results resolve all evidence-bound
questions for the initial `census_acs1` and `census_acs5` contracts. ACS-C was
not begun by this closure. This follow-up does not reinterpret the empty files
from the first local run as provider evidence.

## Successful local evidence incorporated

Local catalog discovery resolved 2024 as the latest vintage for both `acs/acs1`
and `acs/acs5`. Non-empty official dataset, geography, variables, `B01003`, and
`B19013` metadata responses were captured for both products, and their SHA-256
checks passed.

The first anonymous fact run returned HTTP 200 HTML containing Census `Missing
Key`. This is provider authentication evidence, not product unavailability.
Supplying `CENSUS_API_KEY` through the runtime environment returned valid Census
tabular JSON and completed the bounded verifier. It reported
`authentication=environment_key`, 152 available `census_acs1` geographies, 219
available `census_acs5` geographies, and 742 canonical rows. The arithmetic is
internally consistent: 371 independent physical memberships times two metrics
equals 742 rows. SHA-256 verification passed for `canonical.csv`, every captured
ACS1/ACS5 provider JSON response, and `summary.json`.

The authoritative summary schema is `acs_b_local_verification_v1`, its vintage
is 2024, `credential_persisted=false`, and its exact diagnostics and comparison
results are incorporated below. Provider response bodies remain external
verification evidence rather than production repository state.

## Frozen physical contract decisions

The following initial physical contract shape is frozen:

* There are exactly two independent products: `census_acs1` at `acs/acs1` and
  `census_acs5` at `acs/acs5`. This phase has no logical ACS family resolver.
* The four metric identities are `census_acs1_pop_total`,
  `census_acs1_median_household_income`, `census_acs5_pop_total`, and
  `census_acs5_median_household_income`. Population uses `B01003_001E`; median
  household income uses `B19013_001E` in each product.
* Each observation is an annual period estimate. Survey vintage 2024 maps to
  observation date `2024-12-31`; publication/retrieval time is not the
  observation date. Canonical property fields are `property_type_id=all` and
  `property_type=all`.
* Membership is product-specific and direct-provider-backed. For the verified
  2024 instance, ACS1 has 152 geographies and ACS5 has 219. Membership is never
  forced equal, filled from the other product, or synthesized. Each immutable
  pin must enumerate its exact canonical membership and credential-free request
  plan; counts alone are not a substitute for that enumeration.
* The API key may enter only from `CENSUS_API_KEY` in the runtime environment and
  only at request transport. It must never enter pins, manifests, catalogs,
  request evidence, URLs, hashes, diagnostics, fixtures, semantic identities, or
  artifacts.
* Normal execution dynamically discovers each product's latest eligible
  official vintage and freezes an independent pin before acquisition.
  Resume/replay consumes the existing pin without rediscovery; later catalog
  changes cannot alter an in-progress or replayed run.
* Product, vintage, credential-free request plan, exact membership, governed
  metadata hashes, member response hashes, and aggregate canonical content hash
  form the immutable physical identity. A same-vintage content change creates a
  distinct revision/content identity and never overwrites or silently reuses the
  prior identity.
* Unsupported or ineligible identities are not synthesized. No physical product
  may silently substitute for the other.

These decisions and the verified 2024 instance are frozen as the initial
physical contracts. Later implementation belongs to ACS-C and must consume—not
silently reinterpret—these product-specific boundaries.

## Exact 2024 membership reconciliation

| physical source | available | CBSA | county | nation | state | provider-ineligible | valid zero rows | division exclusions | evaluated |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `census_acs1` | 152 | 43 | 103 | 1 | 5 | 71 | 0 | 10 | 233 |
| `census_acs5` | 219 | 50 | 163 | 1 | 5 | 4 | 0 | 10 | 233 |

The reconciliations are exact: ACS1 is `152 + 71 + 10 = 233`; ACS5 is
`219 + 4 + 10 = 233`. Across products, 371 available physical memberships times
two governed variables produced exactly 742 canonical rows. Product membership
is intentionally unequal and remains independently pinned.

## Bounded geography decisions

The canonical authority contains 233 Census-enabled identities: 1 nation, 5
states, 163 counties, and 64 identities represented as `cbsa_metro`. The ten
known Metropolitan Division codes are frozen as
`CANONICAL_CONCEPT_MISMATCH` and
`EXCLUDED_FROM_INITIAL_ACS_GOVERNED_CONTRACT`. ACS-B adds no bespoke hierarchy
acquisition, synthesis, or global taxonomy change. Their future path remains a
shared cross-source taxonomy correction or later clean ordinary integration;
this is not a finding that Census does not support them.

Code 13720 (`big_stone_gap_va_metro_area__cbsa_metro`) was queried through the
same ordinary metropolitan/micropolitan path as every non-division canonical
CBSA and returned `provider_ineligible_no_content` in both `census_acs1` and
`census_acs5`. That is its frozen 2024 physical-product disposition. It is not a
canonical-concept mismatch, is not synthesized, receives no bespoke hierarchy
path, and does not expand ACS-B into a taxonomy investigation.

## Sentinel and missing-value boundary

Both products recorded `sentinels={}` and `valid_zero_rows=0`. The observed run
produced exactly 742 rows for 371 memberships with two requested variables, so
no sentinel/null omission occurred among available responses. The frozen
empirical rule is: accept finite numeric estimates; retain defensive omission of
recognized sentinel/null values with diagnostics; fail closed on unknown
tokens; never coerce missing or sentinel values to zero. No provider-specific
sentinel meaning is claimed because this run exercised none.

## Same-vintage revision result

The deterministic proof recorded `hash_changed=true`. At the same logical 2024
vintage, governed canonical content changed from SHA-256
`e9fcc2ec50b389d537ec04f6a9d2eb12beb218c40d3c8e406af5cc545c2f3d6e` to
`70beffb6c97c8e20b8aaa0c0b47f8e9cd6c2f3ec137bfaf74b098cb69a00460e` after
the controlled mutation. A same-vintage governed-content change therefore must
create a distinct immutable revision and must not overwrite the prior identity.

## Legacy equivalence

| database | exact | revised | provider-only | legacy-only | legacy rows |
|---|---:|---:|---:|---:|---:|
| serving | 742 | 0 | 0 | 0 | 742 |
| public | 0 | 0 | 742 | 0 | 0 |

The complete governed 2024 provider result exactly matches the serving-market
2024 ACS physical footprint. The public database contains no 2024 ACS physical
rows, so its 742 provider-only rows reflect vintage absence, not conflicting
values.

## Credential-safe verification boundary

The verifier writes only to a newly created directory outside the repository,
saves and hashes only JSON-validated bodies, emits the seven-column isolated
canonical snapshot, opens legacy DuckDB files read-only, and performs no
publication or durable-state mutation. Invalid responses include precise
credential-free request context and a sanitized preview. The key is added only
to transient transport parameters and is defensively redacted from errors.

## Closure

No further local verification run or evidence handoff is required for ACS-B.
PR #233 is ready to merge as the ACS-B physical-contract closure. Candidate
publication, accepted-state mutation, Source Set integration, production
workflow dispatch, legacy retirement, and any logical family resolver remain
outside this phase and require later authorization.

## Side-effect statement

No production candidate or pointer was created; ACS was not added to Source Set;
canonical and serving markets were not mutated; no production workflow was
dispatched; the legacy path was not retired; and global geography taxonomy was
not changed.
