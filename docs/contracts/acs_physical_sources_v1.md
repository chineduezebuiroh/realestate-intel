# ACS physical sources v1

`census_acs1` (`acs/acs1`) and `census_acs5` (`acs/acs5`) are independent
physical governed sources. Normal execution discovers each product's latest
eligible annual vintage, persists an exact content-addressed snapshot pin, and
then builds an immutable candidate. Resume and replay resolve that pin without
discovery. There is no cross-product fallback or logical ACS resolver.

Each pin records the exact product/vintage, canonical request-plan identity,
available provider membership, provider-ineligible membership, excluded
Metropolitan Division diagnostics, and the normalized snapshot SHA-256. A
same-vintage content change therefore has a different immutable pin and
candidate identity.

The only governed variables are `B01003_001E` and `B19013_001E`, transformed to
the four product-qualified metric IDs. Dates are `{vintage}-12-31` and both
property-type fields are `all`. No absent geography is synthesized. The ten
Metropolitan Divisions remain `CANONICAL_CONCEPT_MISMATCH` and are excluded;
no hierarchy-specific acquisition is permitted.

`CENSUS_API_KEY` is required runtime transport state. It is added only to the
HTTP parameter map and is forbidden from URLs, pins, snapshots, evidence,
artifacts, identities, diagnostics, and logs.

Candidate publication uses the shared immutable artifact publisher/catalog,
but does not update accepted pointers, Source Sets, canonical/serving markets,
or production databases.
