# Phase 2 final full-cohort orchestration closure

Phase 2 closes orchestration only. It does not accept, promote, materialize, or
publish downstream state.

## Contracts

The governed physical execution registry and common fail-closed barrier contain
exactly, in execution order: `redfin`, `fred_macro`, `ces`, `laus`,
`census_bps`, `census_bps_provisional`, `census_acs1`, `census_acs5`,
`bea_gdp_qtr`, `bea_gdp_ann`, and `census_nrc`.

Only after all eleven successful results expose immutable candidate identities,
the master invokes the existing BPS resolver with the exact two BPS candidates
and the existing ACS resolver with the exact ACS1 and ACS5 candidates. Neither
resolver performs provider discovery or changes an accepted pointer. Physical
family parents remain lineage, not final cohort members. ACS1 is preferred per
observation and ACS5 is used only when ACS1 is unavailable; values are never
blended or synthesized.

The resulting non-promoting logical/direct input plan contains exactly:
`fred_macro`, `ces`, `laus`, `redfin`, `bps`, `acs`, `bea_gdp_qtr`,
`bea_gdp_ann`, and `census_nrc`. It rejects physical BPS/ACS members and the
legacy `census_nrc_fred` source.

## Barrier and modes

Missing, duplicate, malformed, unsuccessful, unpublished, identity-less, or
unexpected physical results fail closed. Normal mode may discover inputs only
inside each physical source's established lifecycle and pins them before
execution. Resume reuses validated durable results/pins and runs only missing
members. Replay consumes the explicit cycle's pins under existing replay
semantics. Family resolution always starts from barrier outputs, so resume and
replay cannot rediscover or substitute a parent.

## Phase boundary

The final job writes a validation plan artifact only. Every mutation flag is
false: no `accepted.source.*`, `accepted.source_set`, canonical-market, or
serving-market state is changed, and Redfin readiness is not consumed.
Promotion and serving remain Phase 3. No fresh Redfin data was acquired while
implementing or validating this change.
