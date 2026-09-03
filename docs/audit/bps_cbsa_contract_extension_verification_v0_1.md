# BPS CBSA contract-extension verification

**Cycle:** `monthly_cycle__2026-07__7cab1c5df177a1e4`  
**Decision:** **CBSA GOVERNANCE DECISION DEFERRED PENDING HOSTED EXACT-PIN VERIFICATION**
**Boundary:** verification only; no logical-family resolution

## Evidence outcome and stop condition

The repository persists exact physical-input pins for compiled release `202604`
and provisional release `2607`. This pass consumed those pin records as the
authority and did not perform latest-release discovery or alter either pin.
The current execution environment rejected the attempt to acquire the exact
compiled Census ZIP at the network proxy (HTTP 403 before a Census response).
The immutable published compiled artifact contains only the already-governed
nation/state/county canonical output, not the excluded raw metro inventory.
Consequently, there is no honest repository-local substitute from which to
establish compiled CBSA presence, history, identity counts, tokens, duplicates,
or compiled/provisional identity agreement.

This triggers the prompt's explicit stop condition for the local execution, but
it is not evidence for a negative governance decision. CBSA is **not rejected**:
no provider-contract or canonical-identity reason against it was established.
The remaining gate is exact compiled-provider evidence. Prior live GitHub Actions
processing retrieved and processed this same persisted 202604 pin at its exact
458,582,881-byte size, so GitHub Actions is the appropriate environment in which
to complete the gate.

## What is established

* The provisional layout is provider-native and headerless after three heading
  lines. Its identity fields are `CSA`, five-digit `CBSA`, `HHEADER`, and name,
  followed by the same 24 estimate/reported measure positions used for state
  and county. The observation month is encoded by `SURVEY_DATE` (`YYYYMM`).
* Provisional authoritative total units are the sum of the four estimate-side
  unit fields (1, 2, 3–4, and 5+ structures) only when all are numeric. Reported
  fields are not substituted, missing values are not made zero, and zero itself
  is a valid numeric observation.
* Prior live 2607 closure established one common observation month,
  `2026-07-01`, no observed nonnumeric tokens, and no identical or conflicting
  duplicates across the complete provisional provider state. That earlier
  verification classified CBSA as outside the then-governed registry; it did
  not create the CBSA-to-canonical evidence requested here.
* The shared canonical `cbsa_metro` universe contains 64 identities and exposes
  a five-digit `census_code`. Redfin uses the same five-digit code directly;
  LAUS and CES series identifiers embed provider area identities and bind to the
  same canonical `geo_slug`; ACS uses `census_code`. BPS should reuse this
  namespace only after exact-code equivalence is proven.
* Labels in that shared universe include metro-area and metro-division
  identities. Names therefore cannot prove that a BPS `CBSA` row represents the
  same statistical area. The exact five-digit identifier must be authoritative;
  name differences may only be diagnostic. No fuzzy matching is permitted.

## Missing evidence

The following requested claims remain unresolved rather than guessed:

1. whether compiled monthly `location_type=Metro` rows are populated in 202604;
2. their exact `cbsa_code` population, identity count, and canonical coverage;
3. compiled observation bounds, observations per identity, materially different
   starts, and any temporal geography-definition changes;
4. CBSA-specific `TOTAL_UNITS` token and duplicate behavior;
5. full pinned-2607 provisional CBSA inventory and the count of the 64 canonical
   market identities represented;
6. the exact compiled/provisional classification table (`EXACT_MATCH`,
   `COMPILED_ONLY`, `PROVISIONAL_ONLY`, `UNMAPPED`, `AMBIGUOUS`).

The new read-only verifier emits the three requested inventory/crosswalk CSVs
and diagnostics JSON when supplied the exact files. It checks file hashes
against the persisted pins, streams the large compiled member, selects monthly
`Metro` rows, uses `TOTAL_UNITS`, rejects unsafe/ambiguous identifiers and
conflicting duplicates, and joins only on exact five-digit codes. It has no
discovery fallback and no mutation/publication surface. No generated raw or
incomplete evidence output is committed.

## Registry discrepancy

`config/source_metric_registry.csv` still advertises
`state|county|place` for five legacy compiled BPS metrics. For the governed
`census_bp_total_units` metric this omits verified compiled nation applicability
and advertises ungoverned `place`. This pass does not promote place and does not
change that metadata while CBSA remains blocked. If exact-pin evidence clears
the CBSA gates, the smallest follow-up change is to set the governed total-units
entry to `nation|state|county|cbsa_metro` and remove `place`; the other four
legacy metric rows must not be silently represented as governed by this
total-units-only decision.

## Applicability matrix

| Physical/logical source | nation | state | county | cbsa_metro |
|---|---:|---:|---:|---:|
| compiled `census_bps` | yes | yes | yes | **unverified** |
| provisional `census_bps_provisional` | no | yes | yes | provider-native, **not governed** |
| logical `bps` | yes | yes | yes | **not governed** |

No proposed four-level matrix can truthfully be frozen until compiled and
provisional exact-pin inventories complete. This is a deferred decision, not a
CBSA rejection. Existing nation/state/county semantics remain unchanged.

## Hosted exact-pin path and family-resolution prompt change

Manually dispatch `.github/workflows/bps-cbsa-contract-verification.yml` with a
`cycle_id`. The workflow reads both pin JSON files beneath that cycle, validates
the expected acceptance releases, retrieves only their exact recorded URLs,
checks both SHA-256 values, and runs the verifier. It uploads the compiled and
provisional inventories, exact-code crosswalk, canonical coverage, compiled
history distribution, applicability matrix, and diagnostics for review. It does
not promote the contract. For this acceptance run, supply
`monthly_cycle__2026-07__7cab1c5df177a1e4`.

Manual trigger command:

```bash
gh workflow run bps-cbsa-contract-verification.yml \
  -f cycle_id=monthly_cycle__2026-07__7cab1c5df177a1e4
```

The final review should return `diagnostics.json`, both inventory CSVs, the
exact-code crosswalk, canonical coverage, compiled history distribution, and
the applicability matrix. It should quote `recommendation` and
`recommendation_basis`, enumerate every unresolved or ambiguous canonical
identity, and attach the workflow run URL. `PROMOTE_CBSA` is actionable only
after that evidence is reviewed; `BLOCK_CBSA` must identify a concrete
provider/canonical mismatch. Missing diagnostics is an infrastructure blocker,
not a CBSA rejection.

The subsequent family-resolution prompt must **not yet assume CBSA**. It must
retain the current nation/state/county applicability and state that the CBSA
decision is deferred—not rejected—pending the hosted exact-pin evidence record. After a later
promotion, amend that prompt to include `cbsa_metro` in both physical-parent
applicability rows exactly as proven, apply compiled-wins/provisional-fill to
CBSA keys without synthesis or gap filling, require exact five-digit canonical
identity, and consume the promoted registry rather than embedding a new BPS
namespace.

## Safety record

No provider release discovery, pin movement, physical rerun/publication,
family resolution/acceptance, accepted-pointer movement, Source Set creation,
DuckDB write, Redfin readiness consumption, cron enablement, place expansion,
or canonical-geography redesign occurred.
