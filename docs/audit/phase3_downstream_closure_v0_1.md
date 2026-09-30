# Phase 3 governed monthly downstream closure

## Scope and reuse

Phase 3 extends, rather than replaces, Source Set v2, canonical assembly,
immutable object publication, the prepared cohort-promotion record, catalog
blob compare-and-swap, and operation-at-a-time recovery proven by the July
promotion. Provider discovery, Redfin acquisition, and ordinary source jobs are
not part of this boundary.

The Phase 2 workflow now persists its completed plan create-once at
`config/monthly_logical_cohort_plans/<cycle_id>.json` on the `main` control
plane. The deterministic `plan_id` binds all eleven physical candidate pins,
the ordered nine logical/direct outputs, complete BPS and ACS resolution
records and parents, and the explicit non-mutation flags. A contradictory
rewrite is rejected. Resume, replay, and Phase 3 preparation load this exact
record and its exact cycle results/resolution records; they never consult a
`latest` pointer or rediscover a provider release.

## Preparation contract

Preparation consumes the exact `monthly_logical_cohort_plan_v1` emitted after
the physical barrier and BPS/ACS resolution. Its ordered inventory is
`fred_macro`, `ces`, `laus`, `redfin`, `bps`, `acs`, `bea_gdp_qtr`,
`bea_gdp_ann`, and `census_nrc`. Missing, duplicate, unexpected, physical-family,
or legacy `census_nrc_fred` members fail closed. Exact content and package hashes
must agree with immutable catalog records. The Source Set retains physical BPS
and ACS parents only in family lineage.

Canonical assembly consumes that one Source Set through the existing assembly
resolver. The manifest binds the Source Set identity and semantic/package hashes,
retains `database_sha256`, and is published immutably without moving accepted
authority.
The hosted adapter derives the target month from the arbitrary cycle identity,
publishes and verifies both objects with the existing GitHub Release/catalog
backend, persists the final promotion record, emits its bound authorization
token, and stops. The former July cycle, BPS resolution, republication IDs,
candidate IDs, build timestamp, and ACS workflow input are not operational
constants. July fixtures remain regression evidence only.

## Promotion and authorization

Preflight is observational and produces a content-addressed promotion record
containing cycle, Source Set, canonical artifact, nine source targets,
expected-old accepted pointers, and exact unconsumed Redfin readiness. The human
authorization token is the SHA-256 of the complete immutable promotion record;
changing any plan field invalidates authorization.

Recovery applies one guarded transition per durable CAS boundary in this order:

1. `accepted.source_set`
2. `accepted.canonical_market`
3. `accepted.source.fred_macro`
4. `accepted.source.ces`
5. `accepted.source.laus`
6. `accepted.source.redfin`
7. `accepted.source.bps`
8. `accepted.source.acs`
9. `accepted.source.bea_gdp_qtr`
10. `accepted.source.bea_gdp_ann`
11. `accepted.source.census_nrc`
12. consume Redfin readiness

An expected-old contradiction fails closed. Exact targets are no-ops on resume,
so completed work is neither duplicated nor rolled back. Redfin consumption is
rejected unless all preceding targets are already accepted.

## Serving transaction

Serving remains a separate recoverable transaction after cohort promotion. The
builder consumes the accepted canonical database, validates the result, and
creates an immutable serving manifest carrying canonical artifact and database
lineage. Publication precedes a guarded `accepted.serving_market` transition.
Canonical drift or serving expected-old drift fails closed; a serving failure
does not roll back accepted Source Set, canonical, sources, or consumed readiness.
The manual hosted serving workflow reads `accepted.canonical_market`, downloads
that exact numeric Release asset, checks package/member/manifest/database
hashes, materializes and validates the snapshot, publishes and verifies the
immutable serving package, and persists a create-once serving promotion record.
It then stops. A second invocation must present the record-derived token; it
uses the recorded expected-old serving pointer and accepted canonical as CAS
preconditions, verifies the durable target again, advances only
`accepted.serving_market`, and performs post-transition remote verification.
Live cohort and serving invocations consume their durable prepared records and
do not rebuild artifacts, making interruption and exact rerun recoverable.

## Mutation boundaries

Phase 2 handoff persistence and Phase 3 preparation add immutable evidence and
catalog records only. They do not move accepted pointers or consume readiness.
The explicitly authorized cohort transaction alone moves its eleven pointers
and consumes Redfin last. The separately authorized serving transaction alone
moves `accepted.serving_market`. Neither workflow is scheduled.

## Remaining debt

Known deterministic DuckDB-byte and NRC Parquet-byte reproducibility hardening
remain non-blocking. Phase 3 does not weaken semantic identity or remove
`database_sha256` to mask those differences.
