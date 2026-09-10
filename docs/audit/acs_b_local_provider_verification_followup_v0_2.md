# ACS-B local provider verification follow-up v0.2

**Status:** `PENDING_LOCAL_EVIDENCE_DETAIL_INCORPORATION` (2026-09-10)

**Decision:** The corrected physical fact acquisition succeeded, but ACS-B
remains open and ACS-C remains prohibited until the evidence details already in
the locally generated `summary.json` are supplied and incorporated. This
follow-up does not reinterpret the empty files from the first local run as
provider evidence.

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

The successful `summary.json` itself is not present in this hosted checkout.
Aggregate counts do not disclose the exact membership lists, per-geography
non-available classifications, sentinel diagnostics, legacy comparison, or
concrete revision hashes. Those fields are not inferred below.

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

These decisions freeze the contract shape. Exact 2024 membership lists,
ordinary non-available dispositions, legacy equivalence results, and concrete
revision-proof hashes remain evidence-bound contract fields and cannot be frozen
from aggregate counts alone.

## Bounded geography decisions

The canonical authority contains 233 Census-enabled identities: 1 nation, 5
states, 163 counties, and 64 identities represented as `cbsa_metro`. The ten
known Metropolitan Division codes are frozen as
`CANONICAL_CONCEPT_MISMATCH` and
`EXCLUDED_FROM_INITIAL_ACS_GOVERNED_CONTRACT`. ACS-B adds no bespoke hierarchy
acquisition, synthesis, or global taxonomy change. Their future path remains a
shared cross-source taxonomy correction or later clean ordinary integration;
this is not a finding that Census does not support them.

Code 13720 was queried through the same ordinary metropolitan/micropolitan path
as every non-division canonical CBSA. Its final disposition is present in either
`diagnostics.<product>.available_membership` or the corresponding `errors` in
the successful local `summary.json`. Because that file is unavailable here,
13720 remains `PENDING_EVIDENCE_DETAIL`; absence from a response-filename listing
is not treated as unsupported evidence. No special investigation is opened.

## Sentinel and missing-value boundary

The verifier fail-closes on unknown non-numeric values, omits null/blank missing
values with diagnostics, and recognizes candidate numeric tokens `-666666666`,
`-888888888`, and `-999999999` as omitted sentinels. The observed run produced
exactly 742 rows for 371 memberships with two requested variables, proving that
no sentinel/null observation was omitted among the available responses. Thus
the empirically supported initial rule is: accept finite numeric estimates;
omit recognized sentinel/null values with diagnostics; fail on unknown tokens;
never coerce missing or sentinel values to zero. The run did not exercise a
sentinel value, so provider-specific meanings beyond those behaviors are not
claimed.

## Same-vintage revision result

Successful bundle publication means the verifier completed its deterministic
same-vintage mutation check and wrote `hash_changed=true`: changing governed
canonical bytes while holding the 2024 vintage fixed changed the SHA-256 content
identity. The original and mutated hash strings remain in the unavailable local
`summary.json`; they must be copied into the final evidence record rather than
reconstructed or guessed.

## Credential-safe verification boundary

The verifier writes only to a newly created directory outside the repository,
saves and hashes only JSON-validated bodies, emits the seven-column isolated
canonical snapshot, opens legacy DuckDB files read-only, and performs no
publication or durable-state mutation. Invalid responses include precise
credential-free request context and a sanitized preview. The key is added only
to transient transport parameters and is defensively redacted from errors.

## Required evidence handoff (no provider rerun)

No metadata or fact request needs to be repeated. From the existing successful
evidence directory, print the credential-free review fields:

```bash
cd /path/to/acs-b-2024-facts
python - <<'PY'
import json
from pathlib import Path

s = json.loads(Path("summary.json").read_text(encoding="utf-8"))
review = {
    "schema_version": s["schema_version"],
    "vintage": s["vintage"],
    "authentication": s["authentication"],
    "credential_persisted": s["credential_persisted"],
    "canonical_rows": s["canonical_rows"],
    "diagnostics": s["diagnostics"],
    "legacy_equivalence": s["legacy_equivalence"],
    "same_vintage_revision_proof": s["same_vintage_revision_proof"],
}
print(json.dumps(review, indent=2, sort_keys=True))
PY
```

The full `diagnostics` are necessary because they contain each product's exact
`available_membership`, `available_by_level`, `provider_ineligible`,
`valid_zero_rows`, `sentinels`, and per-geography `errors`. These fields resolve
13720 and every non-available ordinary identity without guessing.
`legacy_equivalence` and `same_vintage_revision_proof` are also required exactly
as emitted. No key, URL, or raw response body is requested. Until these fields
are incorporated, PR #233 is not ready to merge as ACS-B closure.

## Side-effect statement

No production candidate or pointer was created; ACS was not added to Source Set;
canonical and serving markets were not mutated; no production workflow was
dispatched; the legacy path was not retired; and global geography taxonomy was
not changed.
