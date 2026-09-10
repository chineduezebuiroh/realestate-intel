# ACS-B local provider verification follow-up v0.2

**Status:** `PENDING_CORRECTED_LOCAL_FACT_VERIFICATION` (2026-09-09)

**Decision:** ACS-B remains open and ACS-C remains prohibited. This follow-up
supersedes only the live-evidence status in v0.1; it does not reinterpret the
empty fact files from the first local run as provider evidence.

**2026-09-10 diagnostic checkpoint:** The corrected Mac run reached an HTTP 200
response but could not parse its body as JSON. HTTP success does not establish a
valid Census response, and this result is neither product unavailability nor an
authentication failure. The original exception omitted the request identity and
response shape, so the exact cause cannot yet be identified.

## Evidence incorporated from the first local run

Local dynamic discovery resolved 2024 as the latest catalog vintage for both
`acs/acs1` and `acs/acs5`. Non-empty official dataset, geography, variables,
`B01003`, and `B19013` metadata responses were captured for both products, and
their recorded SHA-256 checks passed. This establishes the two product paths and
metadata availability, but the evidence bundle itself was not supplied to this
hosted task, so its byte hashes are not copied into repository contracts.

The direct Metropolitan Division and 13720 files were empty. They are
**inconclusive**: hashing an empty file establishes only the identity of zero
bytes. It does not establish a valid Census response, geography ineligibility,
or lack of provider support. The original shell capture therefore cannot freeze
fact membership, variables in observed facts, authentication, sentinels,
canonicalization, equivalence, or response-content pins.

Hosted anonymous ordinary ACS1 and ACS5 national queries were attempted again.
Both were rejected by the task network with HTTP 403 before usable provider JSON
was returned. This is an environment limitation, not an ACS authentication
result.

## Bounded geography decision

The canonical authority contains 233 Census-enabled identities: 1 nation, 5
states, 163 counties, and 64 identities represented as `cbsa_metro`. The ten
known Metropolitan Division codes are classified
`CANONICAL_CONCEPT_MISMATCH` and
`EXCLUDED_FROM_INITIAL_ACS_GOVERNED_CONTRACT`. Querying them requires a
different Census geography concept than their current canonical level. ACS-B
will not add provider-specific hierarchy logic or change global taxonomy. They
retain a future path after cross-source taxonomy correction or clean ordinary
provider integration. This is not a finding that Census does not support them.

Code 13720 remains in the ordinary `cbsa_metro` verification set. The corrected
utility queries it through the same metropolitan/micropolitan clause as every
other ordinary canonical CBSA. A valid response will include it; a valid
zero-row or no-content provider response will classify it for later governance
review. No special query path or synthesis is introduced.

## Corrected local utility contract

`scripts/acs_b_verify.py` performs individual ordinary fact requests for the
223 bounded identities in each physical product. It:

* reads `CENSUS_API_KEY` only from the environment and adds it only to the
  transport parameters;
* never prints or persists the key, a keyed URL, or transport request headers;
* fails on HTTP errors, empty bodies, invalid JSON, malformed tabular JSON,
  multiple rows for an individual request, and unknown non-numeric values;
* distinguishes HTTP 204 provider-ineligible responses, valid header-only zero
  rows, and valid observations;
* hashes and saves only JSON-validated provider bodies;
* writes only to a newly created directory outside the repository;
* emits the required seven-column isolated canonical snapshot with `all`
  property fields and independent physical source/metric IDs;
* opens both legacy DuckDB files read-only for vintage equivalence; and
* demonstrates that a one-byte same-vintage content change changes the content
  hash.

The utility is verification-only. It does not discover or freeze production
pins, publish a candidate, mutate DuckDB, alter durable state, or dispatch a
workflow.

For every HTTP 2xx body that is empty, invalid JSON, non-tabular JSON, malformed,
or unexpectedly multi-row, the utility now raises `INVALID_PROVIDER_RESPONSE`
with the physical source/product, vintage, canonical identity, geography level,
Census code, credential-free request parameters, HTTP status, Content-Type,
byte length, and a whitespace-normalized 300-byte preview. A transport key is
removed from parameters and redacted from the preview defensively.

The query construction was rechecked against the repository manifest and the
existing Census adapter: nation uses `for=us:1`; two-digit states use
`for=state:<code>`; five-digit counties split into state and county components;
and ordinary CBSAs use the Census metropolitan/micropolitan geography name. No
deterministic construction error is evident without the missing response body,
so query semantics are unchanged rather than being altered speculatively.

## Sentinel status

The verifier currently fail-closes on non-numeric values, omits null/blank
missing values with diagnostics, and recognizes the candidate numeric Census
special values `-666666666`, `-888888888`, and `-999999999` as omitted
sentinels. This is deliberately **not yet a frozen official sentinel contract**.
The corrected evidence must contain the observed classifications and the final
ACS-B review must bind them to official provider documentation before freeze.

## Required corrected local run

Metadata need not be recaptured. From the authoritative branch on the user's
Mac, with repository dependencies installed, run:

```bash
cd /path/to/realestate-intel
OUT="$(mktemp -d)/acs-b-2024-facts"
python scripts/acs_b_verify.py --vintage 2024 --output "$OUT"
(cd "$OUT" && shasum -a 256 -c SHA256SUMS)
printf 'ACS-B evidence: %s\n' "$OUT"
```

For this diagnostic rerun, do not set `CENSUS_API_KEY`. If the command fails,
return the complete single-line `INVALID_PROVIDER_RESPONSE` diagnostic. Run the
`shasum` command only after the verifier completes and publishes `SHA256SUMS`.

Anonymous execution is intentional for the first run because it resolves the
authentication question directly. If and only if Census returns an explicit
quota/key response, rerun without echoing the credential:

```bash
cd /path/to/realestate-intel
OUT="$(mktemp -d)/acs-b-2024-facts-keyed"
CENSUS_API_KEY="$CENSUS_API_KEY" python scripts/acs_b_verify.py --vintage 2024 --output "$OUT"
(cd "$OUT" && shasum -a 256 -c SHA256SUMS)
printf 'ACS-B keyed evidence: %s\n' "$OUT"
```

Return `summary.json`, `SHA256SUMS`, `canonical.csv`, and the captured JSON files
as out-of-repository review input. Until that evidence is incorporated, exact
ACS1/ACS5 governed memberships and counts, valid fact semantics, authentication,
legacy equivalence, sentinel freeze, response hashes, and independent discovery/
pin contracts remain unresolved. Consequently ACS-B is not complete.

## Side-effect statement

No production candidate or pointer was created; ACS was not added to Source Set;
canonical and serving markets were not mutated; no production workflow was
dispatched; the legacy path was not retired; and global geography taxonomy was
not changed.
