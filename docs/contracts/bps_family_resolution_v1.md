# Governed BPS logical family resolution v1

**Status:** Candidate construction only. This contract does not authorize
acceptance, Source Set construction, canonical assembly, serving promotion, or
routine scheduling.

## Boundary and immutable inputs

The resolver consumes only the exact cataloged and remotely verified compiled
`src__census_bps__2026-04__r2__993afaddb934ce4f` and provisional
`src__census_bps_provisional__2026-07__r2__61c56540953237cb` source artifacts.
It validates their complete content and package hashes against the frozen
identities before reading canonical data. It never reads or discovers provider
inputs. Both physical artifacts remain unchanged and independently cataloged.

Their governed compatible-CBSA relationship is release-variable rather than an
equivalence requirement: 39 shared identities, compiled-only `15680`, `31460`,
and `36140`, provisional-only `15700`, `17340`, `18860`, `20660`, `21700`,
`32300`, `39780`, `43760`, `45000`, `46020`, and `46380`, a 53-code union, and
none absent from both. This physical inventory does not alter the frozen
53-compatible-code universe. These counts and sets validate only this frozen r2
parent pair; they are regression evidence, not acceptance criteria for a future
compiled or provisional release. Every future governed parent pair derives its
physical intersection, differences, union, and absence dynamically inside the
compatible universe.

## Merge and identity

Resolution occurs at `(geo_id, metric_id, date, property_type_id)`. The output
is the union of keys: compiled-only and provisional-only values are retained,
and compiled wins an identical-key collision. Different months for the same
geography are different keys, so compiled history through April and a
provisional July observation coexist. No gap month, value, or geography is
synthesized. Every output row is rewritten to `source_id=bps`.

`bps_family_resolver_v1` creates an ordinary immutable source artifact through
the common source-artifact builder and publication lifecycle. Its semantic
identity binds the active `bps_governed_source_v1` contract, governed config
hashes, resolver policy/version, both parent artifact/content/package/data
hashes, row-level lineage, and canonical output Parquet hash. Row lineage names
the winning physical parent and retains both physical values for collisions.

## Durable record and side effects

The create-once `bps_family_resolution_record_v1` is stored under
`config/bps_family_resolutions/<resolution_id>.json`. Its deterministic
resolution ID binds cycle context, resolver/source contracts, governed config
hashes, exact parents, and output artifact identity/hash. It records the output
package hash, deterministic diagnostics, and false assertions for accepted
pointer movement, Source Set creation, DuckDB mutation, Redfin consumption,
and provider discovery. Exact reruns reuse the artifact and record; any
same-identity contradiction fails closed.

The manually dispatched `bps-family-resolution.yml` is deliberately absent
from the monthly fan-out and has no schedule or push trigger.
