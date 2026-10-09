# Macro serving input compatibility v1

The Macro Engine consumes an explicit, locally materialized, verified serving
DuckDB. Accepted artifact resolution, package verification and materialization
belong outside the engine. The engine never discovers accepted pointers, queries
the monthly artifact catalog, downloads packages or contacts providers.

`regime/serving_input.py` owns the serving-to-canonical observation boundary,
before derived metrics and features. Legacy physical-only observations retain
the existing canonical source resolver. Governed logical ACS population/income
enter `population`/`median_household_income`; governed logical BPS enters existing
BPS canonical metrics through registry membership. Diagnostic metrics remain
excluded. No analytical registry, transform, weight or policy changes.

Before filtering, registered source/metric ownership and explicit logical-family
equivalents authorize incoming identities independently of analytical membership.
Unknown metrics under supported sources and registered metrics under unauthorized
sources fail, including optional active metrics. Registered diagnostics pass
identity validation without becoming scoring inputs. Unsupported physical BPS
provisional observations fail explicitly; family resolution remains upstream.
Genuine source absence retains existing fallback behavior and missingness rules.

ACS logical identity authority is `logical_source_metric_registry.csv`. Logical
ACS origins are `acs:<metric_id>`; BPS origins are `bps:<metric_id>`. These do not
assert a winning physical parent. ACS1/ACS5 selection and compiled/provisional
BPS selection remain upstream; the engine neither repeats them nor synthesizes
missing observations.

Each family must have one representation across the entire input database.
Logical ACS with either physical ACS source, or logical BPS with either physical
BPS source, fails even for disjoint histories. Supported observations must be
unique at `(geo_id, date, source_id, metric_id)` before projecting property type
away; duplicate rows or multiple property types at that grain fail. Legacy
schemas without a property-type column are supported only when that same grain
is unique. Canonical observations must also be unique by geography/date/metric.

Configuration validation performs no database access. Provider ownership rows
need not be analytical dimension members; feature/dimension references and
canonical feature coverage remain validated. The adapter opens only the
explicit database, read-only, checks schema and representations, then returns
canonical observations. Production runner preflight requires input-wide coverage
of required active canonical source metrics and existing derived-metric
components, accepting logical equivalents. Optional/diagnostic identities and
per-geography/month completeness are not required. Frozen temporal missingness,
normalization maturity and availability renormalization remain unchanged.

`--expected-serving-db-sha256` optionally binds the input to a verified hash.
Mismatch fails before creating a run directory. Provenance records independently
computed `serving_db_sha256`, supplied `expected_serving_db_sha256`, adapter version,
and the logical registry hash when ACS logical rows consume it. Reserved run and
provenance fields cannot be overridden by `--metadata-json`. The runner scores the
same loaded observation frame validated in preflight. Input hashing brackets its
read to detect changes; callers must keep the materialized input immutable.

Existing immutable runs remain unchanged. Equivalent selected physical and logical
values use the same frozen feature/scoring path; logical lineage remains distinct.
