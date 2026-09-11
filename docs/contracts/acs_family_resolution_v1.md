# Governed ACS logical family resolution v1

**Status:** Candidate construction only. ACS-E does not authorize an accepted
pointer, Source Set, canonical-market or serving mutation, or scheduling.

## Contract and immutable parents

`census_acs1` and `census_acs5` remain authoritative, independent physical
Census statistical products. The manually dispatched resolver takes one exact
artifact ID for each product, finds each in the durable artifact catalog, and
validates catalog, package, content, data, manifest, source, and contract
identity before reading it. Parent choice is therefore an explicit orchestration
input rather than provider discovery or a hard-coded one-time pair. The workflow
may execute code from the dispatched migration ref, but catalog reads, parent
resolution, logical artifact publication, and durable resolution-record writes
all use `main`, the production control-plane authority branch.

The derived source is `acs`. Physical metrics map to
`census_acs_pop_total` and `census_acs_median_household_income`. At each
`(geo_id, logical_metric_id, date, property_type_id)` key, a valid physical ACS1
row wins; ACS5 supplies the key only when ACS1 has no row. Values are never
averaged, blended, interpolated, or synthesized. Unlike BPS compiled/provisional
resolution, this is best-available selection between distinct statistical
products, not selection between release states of one statistic.

Every logical row records the winning physical source, metric, parent artifact
and content hash, presence and values for both products, and its governed reason
code. Consequently a longitudinal logical series may visibly and legitimately
switch physical products between vintages.

## Geography, diagnostics, and identity

The resolver rereads the physical ACS geography contract and rejects parent
rows outside it. Metropolitan Divisions remain excluded; provider-ineligible
and unavailable observations remain absent rather than triggering acquisition
or fabrication. Parent coverage evidence is copied into diagnostics.

Diagnostics count parent and output rows, only/overlap keys, ACS1 wins, ACS5
fallbacks, equal and differing overlaps, duplicate output keys, geography union
and type coverage, exclusions, provider-ineligible evidence, parent identities,
and metric mapping. Differing overlaps are evidence, not failure.

Artifact identity binds resolver and policy versions, the physical contract,
governed config hashes, exact parent artifact/content/package/data identities,
and the metric mapping. The separately stored
`config/acs_family_resolutions/<resolution_id>.json` record is create-once:
identical reuse is a no-op and contradiction fails closed. It explicitly records
that accepted state, Source Sets, DuckDB, serving data, and provider discovery
were not mutated.
