# NRC-D shared cohort integration audit v0.1

## Scope and disposition

NRC-D integrates the governed physical `census_nrc` source into shared cohort
planning, validation, hosted execution, durable resume/replay classification,
and Source Set candidate construction. NRC remains one physical source and one
direct Source Set entry. No logical NRC family or resolver exists, and the
legacy `census_nrc_fred` lineage is not part of the production source plan.

The versioned hosted execution registry now names `census_nrc`. The stable
refresh policy remains unchanged because its hash is part of existing Redfin
cycle identity; the NRC adapter supplies its already-governed recording
authorization without changing that source plan. Its direct entry retains the existing artifact,
provider-release, content/package, and observation identities. Cohort assembly
requires evidence for the two governed metrics, five governed geographies,
month-end canonical source schema, `thousands_of_housing_units_saar` units, a
numeric scale factor of 1, and the frozen NRC parser contract. Missing or
contradictory evidence fails closed.

## Resume/replay and smoke 181

Normal execution discovers and durably pins the two Census workbooks before
candidate execution. Resume and replay consume that existing pin without
provider rediscovery. Smoke 181 previously asserted the pre-BEA physical plan
even though the production execution registry already contained both BEA
sources. NRC-D legitimately adds `census_nrc`; the smoke now derives the full
missing-source expectation represented by the current registry, including the
already-valid BEA entries and the new NRC entry.

## Deferred endgame

NRC-D performs no accepted-state mutation and creates or promotes no Source Set,
canonical-market artifact, or serving-market artifact. Accepted promotion stays
deferred until the final intended initial full cohort. Fresh Redfin acquisition
and the full normal monthly lifecycle remain the intended final production
endgame after all source integrations are complete.
