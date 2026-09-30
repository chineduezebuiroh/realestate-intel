# BEA-D shared cohort integration audit v0.1

## Disposition

**DISPOSITION A — EXISTING SHARED COHORT CONTRACT SUFFICIENT.**

The existing `monthly_source_execution_result_v1`, durable cycle-result registry,
physical barrier, and Source Set v2 direct-entry contracts already support an
independent governed source. No common schema change or provider-family resolver
is required.

## Reconnaissance findings

* `config/monthly_source_execution_registry.json` controls the hosted physical
  barrier. `config/monthly_refresh_policy.json` identifies automated sources
  whose compatible durable results can satisfy normal/resume planning.
* `jobs/monthly_refresh/cohort.py` validates the common result contract, resolves
  exact catalog-backed durable results, preserves accepted pointers, and builds
  the barrier evidence.
* `jobs/monthly_refresh/cohort_promotion.py` owns the exact physical inventory,
  direct candidate-to-entry mapping, family mappings, and complete logical Source
  Set inventory. Its direct path already preserves artifact, content/package,
  provider release, observation coverage, and publication receipt identities.
* Only BPS and ACS use physical-to-logical family mappings. Nothing in Source Set
  v2 requires all products from one provider to resolve to one logical family.
* The accepted-pointer convention is `accepted.source.<source_id>`. The future
  pointers are therefore `accepted.source.bea_gdp_qtr` and
  `accepted.source.bea_gdp_ann`.
* BEA-C already supplies `monthly_source_execution_result_v1` and uses the
  `monthly_source_input_pin_v1` lifecycle. Normal discovery occurs before exact
  pin execution; resume/replay recover the existing embedded normalized snapshot
  and do not call BEA.

## Integration

`bea_gdp_qtr` and `bea_gdp_ann` are separately required in the execution
registry, hosted fan-out, barrier, durable-result recovery, and logical Source Set
inventory. The stable refresh policy remains unchanged because its file hash is
part of the existing Redfin cycle identity; BEA remains declared there as a
recordable slower-cadence source. Each result is mapped directly to its own
Source Set v2 entry. Neither is in `PHYSICAL_FAMILY_SOURCES`, neither appears in
a family-resolution map, and no `bea` source exists.

The reusable hosted workflow calls the existing BEA-C adapter for either exact
source ID. It retains `main` as durable authority. The master passes the normal,
resume, or replay mode unchanged and accepts either a new immutable result or an
exact durable reused result at the common barrier.

## Deferred promotion safety

The ACS-inclusive hosted promotion adapter was already preflight-only. Its
fail-closed mutate guard now explicitly covers the ACS/BEA-inclusive cohort and
runs before API access or publication. BEA-D does not publish or accept a Source
Set, canonical market, or source pointer; does not consume Redfin readiness; and
does not alter serving state. Routine scheduling remains disabled.

## Evidence

Focused tests cover independent registration, absence of logical `bea`, direct
two-entry Source Set construction, missing and duplicate barrier members,
catalog-backed durable resume reuse, hosted dispatch, and the preflight-only
mutation guard. The existing cohort promotion smoke fixture now includes both
BEA entries alongside the unchanged ACS and BPS family behavior.

No accepted state was mutated. BEA-D is shared-cohort integration only and is
not a production promotion.
