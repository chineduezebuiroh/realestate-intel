# Governed BLS CES source contract v1

## Status and boundary

This contract freezes the CES-A provider edge.  It governs deterministic request
planning, acquisition, canonicalization, and diagnostics only.  It does not
authorize a bootstrap, artifact publication, accepted-pointer movement, database
mutation, cohort integration, scheduling, or retirement of the legacy loader.

## Metric dependency audit and frozen scope

The physical governed scope is exactly:

| Physical metric | Classification | Evidence and role |
|---|---|---|
| `ces_total_nonfarm_sa` | `GOVERNED_REQUIRED` | Enabled physical mapping for logical `ces_total_nonfarm` in `source_metric_registry.csv`; the logical metric is the primary/fallback Demand employment input in `metric_dimension_registry.csv` and owns production features in `feature_registry.csv`. It is mandatory for source-target advancement. |
| `ces_total_private_sa` | `GOVERNED_DIAGNOSTIC` | Enabled physical mapping for logical `ces_total_private`; the dimension registry explicitly marks it diagnostic-only and gives it zero production weight. It remains governed evidence but is optional for target advancement because SA availability is sparse. |
| `ces_construction_sa` | `GOVERNED_DIAGNOSTIC` | Enabled physical mapping for logical `ces_construction`; the dimension registry marks it diagnostic-only/zero weight pending Supply adoption. It remains governed evidence but is optional for target advancement because SA availability is sparse. |

The other 23 physical IDs found in legacy DuckDB state are `LEGACY_ONLY`:

* `ces_total_nonfarm_nsa`, `ces_total_private_nsa`, and
  `ces_construction_nsa`;
* SA and NSA variants of `ces_mining_logging`, `ces_manufacturing`,
  `ces_trade_transport_utilities`, `ces_information`,
  `ces_financial_activities`, `ces_prof_business_services`,
  `ces_education_health_services`, `ces_leisure_hospitality`,
  `ces_other_services`, and `ces_government`.

Executable/configuration search found no authoritative physical mapping for those
IDs in `source_metric_registry.csv`.  Downstream governed feature, indicator, and
dimension configuration names only the three logical bases above and resolves
them to the three SA physical IDs through the source registry.  UI labels,
serving/public builders, and forecasting inventory may display or copy legacy
rows generically, but none declares an excluded physical ID as a required
governed production input.  Collection history is not a governance declaration.

## Units and values

CES employment is governed at provider numeric scale:

```text
unit = thousands_of_jobs
scale_transform = none
```

The adapter parses a finite provider number and does not multiply it by 1,000.
Thus legacy stored values remain numerically comparable when they came from the
same BLS series without transformation.  CES-B must prove that equivalence before
accepting bootstrap state.  Legacy dimension metadata is not authority for this
adapter; governed source-registry unit labels use `thousands_of_jobs`.

## Geography and membership

`config/ces_series.generated.csv`, itself derived through the exact
`bls_ces_area_code` crosswalk in `config/geo_manifest.generated.csv`, is the
series/geography authority.  Only tracked seasonally adjusted rows mapping to the
three governed physical metrics are selected.  Expected membership is the exact
selected series set—not a metric × geography product.  A configured absence is
valid; an omitted configured series is diagnosed; an unexpected returned series
is rejected.

## Mandatory and optional release completeness

Every configured `ces_total_nonfarm_sa` series is mandatory for target
advancement.  These series cover the configured CES geography universe and are
the active Demand employment signal.  Sparse `ces_total_private_sa` and
`ces_construction_sa` series are governed but optional for target advancement.

```text
observation_max = minimum(per-series maximum for every configured
                          ces_total_nonfarm_sa series)
target_month = YYYY-MM(observation_max)
```

Optional-series lag is reported in whole calendar months relative to that common
maximum.  Configured optional absence does not block.  A configured optional
series omitted entirely or lagging is explicit diagnostic evidence for later
publication policy; it never silently changes the target.  A later runner must
fail closed/quarantine if the mandatory common maximum regresses relative to the
prior governed artifact.

The Redfin catalyst/cycle month, execution month, retrieval time, and wall clock
never determine CES target month.

## Revision and reconciliation

CES is `revisionary_current_truth`.

* `ordinary_overlap` uses explicit deterministic bounds spanning at least three
  inclusive calendar years.  The caller—not the planner's wall clock—chooses the
  governed bounds.
* `deep_reconciliation` uses an explicit start no later than 1960 and an explicit
  end year, partitioned into deterministic windows.  This covers all CES history
  observed in repository evidence (the earliest legacy public row is 1966), is
  materially deeper than the five-year minimum, and honors the existing policy's
  annual deep/full intent without a wall-clock-derived lookback.  It exists for
  annual benchmark review but is not scheduled by CES-A.
* A returned canonical key from a completely successful acquisition is current
  provider truth and wins on reconciliation.
* A prior-only governed key is preserved.  Provider omission is not deletion.
* CES-A defines no retraction/deletion semantics and performs no reconciliation
  or artifact production; it exposes pure current truth for the existing shared
  reconciliation infrastructure in a later phase.

## Request and provider identity

The canonical plan contains sorted exact series metadata, explicit bounds and
mode, ordered non-overlapping year windows of at most 20 inclusive years, ordered
series batches of at most 50, the BLS v2 endpoint, `annualaverage=false`, adapter
contract version, unit/scale, and hashes of:

* `config/ces_series.generated.csv`;
* `config/geo_manifest.generated.csv`;
* `config/source_metric_registry.csv`;
* `config/source_refresh_revision_policy_v0_2.json`.

`source_request_identity` is `bls-ces-v2:` plus SHA-256 of that canonical plan.
The API key is required for governed acquisition but is never part of the plan or
identity.

The ordinary BLS response boundary used here exposes no proven immutable release
identifier.  A later artifact runner should use
`ordinary-current:<sha256(canonical request plan + canonical acquired response)>`
unless an official stable BLS release identifier is proven, in which case it may
be included without discarding the content identity.

## Canonical facts and validation

Output columns are exactly:

```text
geo_id, metric_id, date, property_type_id, value, source_id, property_type
```

`source_id="ces"`, both property fields are `"all"`, values use the unit contract,
and `M01`–`M12` map to calendar month-end.  `M13` is deliberately discarded and
never converted to December.  Output is canonical-key sorted and response order
cannot affect it.

The adapter rejects unexpected or duplicate series blocks, request-membership
drift, duplicate canonical keys, contradictory seasonality, missing mapping
metadata, malformed periods/response schema, observations outside their request
window, and null/non-numeric/non-finite values.  A failed request batch aborts the
whole acquisition; partial truth is never returned.

Timeouts, connection failures, HTTP 408/429/5xx receive at most three total
attempts with bounded backoff.  Other HTTP errors and deterministic schema,
identity, or value failures fail immediately.  A malformed HTTP-success response
is not reclassified as transport failure.

Diagnostics report exact requested/returned/missing membership, rows by metric
and geography, observation range, per-series maxima, mandatory common maximum,
target month, optional lag, and duplicate/invalid counts.  CES-A does not publish,
so later publication thresholds beyond these fail-closed invariants remain a
CES-C decision.
