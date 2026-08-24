# Production source-set contract v2

## Boundary

`source_set_manifest_v2` is additive. It does not modify or replace the partial
vertical-slice `source_set_manifest_v1`, and no production workflow selects v2
yet.

## Identity and exact pins

A complete v2 set contains sorted required/included inventories and sorted
source entries. Each entry pins source/artifact IDs, exact `artifact://` URI,
package and semantic content SHA-256, provider release ID, observation maximum,
validation and monthly status, release tag, numeric asset ID, and publication
receipt ID. `latest` and any URI not ending in the exact artifact ID fail.

Semantic identity hashes all governed fields except operational `created_at`,
provenance-only `builder_git_sha`, and the derived `source_set_id`. Consequently,
a retry at another time/commit with identical consumed contracts is reusable,
while a changed artifact, status, eligibility decision, config, contract, or
family resolution produces a different ID. A caller attempting different bytes
under the same semantic ID must be treated as a reproducibility failure by its
publication layer.

## Governance hashes

The strict contract requires exactly these currently consumed files, using
repository-relative paths and full SHA-256:

* `config/monthly_refresh_policy.json`;
* `config/source_refresh_revision_policy_v0_2.json`;
* `config/source_metric_registry.csv`;
* `config/geo_manifest.generated.csv`.

The list is not empty and is part of semantic identity. No artificial canonical
assembly config was added: current assembly behavior is versioned by
`canonical_market_assembly_v1`, while metric/geography behavior is already
machine-readable. A future genuinely consumed assembly policy must be added to
the required set in a new contract revision.

## Monthly outcomes and eligibility

* `refreshed`: successful current-cycle check produced a new artifact; it is not
  carried forward.
* `unchanged`: successful current-cycle acquisition/reconciliation proved the
  exact prior artifact remains current; it records that exact retained artifact.
* `provider_still_stale`: a successful provider availability check proved no new
  state; both `carried_forward` and `carry_forward_policy_allowed` must be true.
* `failed`: never eligible in v2 and blocks creation.

Every included source must have `validation_status=passed` and
`cycle_check_succeeded=true`. This prevents technical failure from being encoded
as provider staleness. A complete set currently requires required and included
inventories to match; future optional/family behavior requires an explicit
contract revision rather than silent omission.

The governed Redfin policy is strict: the entry must be `refreshed` and its
artifact ID must pin the source set's exact target month. V2 defines no Redfin
carry-forward.

## Compatibility and activation

V2 has its own create/validate functions in `source_set_v2.py`. Existing v1
functions and generic assembly are untouched. A future cloud coordinator may
select v2 only after the catalog-backed GitHub resolver exists and full
production inventory/eligibility acceptance passes.
