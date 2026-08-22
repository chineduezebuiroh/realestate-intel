# Redfin ingestion v2 contract

## Governed filesystem and baseline

`data/redfin/raw` is the only production raw root:

```text
baseline/2026-07/   immutable seven-file history
drops/YYYY-MM/      untouched manual full-history releases
current/            metadata pointers and append-only history only
quarantine/         failed drops
```

Bootstrap may create directories, but missing governed inputs fail closed. Raw
files and candidate artifacts are ignored by Git. `data/raw/redfin` survives only
below `_legacy`; production jobs and workflows must not read it. Deletion guards
protect the raw root and the baseline, current, and quarantine roots.

`config/redfin_baseline_manifest.json` manifest version 1 permanently records
the seven supplied July 2026 filenames and SHA-256 hashes. Expected hashes are
never regenerated. Exact coverage is 2012-01 through 2026-07 for nation, state,
metro, county, and city, and 2012-03 through 2026-07 for ZIP and neighborhood.
Validation rejects either earlier or later floors and any endpoint mismatch. The
per-file manifest records are the authoritative floors (including city at
2012-03); the grouped coverage entry is descriptive only. The manifest refers
to `config/redfin_metric_domain_contract.json`, the sole machine-readable
numeric percentage-domain authority.

## Families, geographies, metrics, and units

Registration uses explicit filename tokens: `country`, `states`, `metros`,
`counties`, `cities`, `neighborhoods`, and `zips`. Exactly one token must match
each file and exactly seven distinct families are required. All seven hashes stay
governed even when production does not load all seven.

Candidate construction first reads the governed geo manifest, then loads only
families with active `include_redfin` geographies. Joins are family-scoped:
nation→nation, state→state, metro→cbsa_metro/metro_area, county→county,
city→city/place, neighborhood→neighborhood, and ZIP→zip/zip_code. Redfin codes
are never assumed globally unique. Ungoverned city or neighborhood files remain
hash-governed but are not read into pandas.

Production has exactly eleven metrics:

```text
average_sale_to_list_ratio       homes_sold
inventory                        median_days_on_market_days
median_sale_price_nsa            median_sale_price_per_sqft
months_of_supply                 new_listings
pending_sales                    percent_off_market_in_two_weeks
share_sold_above_original_list
```

`active_listings` supplies `inventory` only when `inventory` is absent; it never
becomes a metric. Percentages remain Redfin percentage points and are never
clipped, normalized, or converted to fractions.

Metric validation has three explicit layers: structural/schema errors are hard
failures; source-domain conformance is a hard boundary; and expected-range
anomalies are aggregated business warnings. Values outside the nominal business
range are not considered economically normal, but may still be faithfully
retained when they fall inside the empirically governed Redfin source envelope.

The immutable seven-file July 2026 evidence encoded in contract version 1 is:

| Metric | Observed July extrema | Expected business range | Accepted source envelope |
|---|---:|---:|---:|
| `average_sale_to_list_ratio` | 50.00–200.00 | 50–150 | 50–200 |
| `share_sold_above_original_list` | -1.73–103.47 | 0–100 | -2–105 |
| `percent_off_market_in_two_weeks` | -7.57–100.00 | 0–100 | -8–100 |

The fail-closed loader requires contract version 1, baseline `2026-07`, exactly
the three governed metrics, `percentage_points` units, numeric and ordered
bounds, expected ranges inside source envelopes, and observed extrema inside
those envelopes. Baseline validation aggregates all exposed percentage columns
across all seven active and preserved-only files and additionally requires its
actual extrema to reproduce the governed observed extrema within `1e-9`.
Future drops never expand or rewrite these boundaries: inside-expected values
are normal, inside-envelope anomalies warn and remain unchanged, and values
outside the source envelope fail. Expansion requires an explicit governance
change.

## Memory-safe validation and lifecycle

Baseline and drop validators hash files, read headers, and stream only date and
percentage columns in 100,000-row chunks. They do not materialize large city or
neighborhood exports. Every drop family must end at the registered month.
Validation artifacts aggregate, by metric, actual extrema, expected and source
ranges, counts below/above each range, total source violations, and status; they
do not emit warning strings per row. Candidate and serving validation use the
same contract. Candidate validation cannot advance lifecycle state when a
source violation exists. Serving validation treats expected-range anomalies as
warnings but source-envelope breaches as corruption.

The lifecycle is:

```text
incoming → registered → validated → candidate_built → candidate_validated
→ serving_refreshed → downstream_validated → published → promoted
```

Failures may move to quarantine. Identical registration and repeated validation
are deterministic; conflicting same-month hashes fail closed. Missing monthly
registration returns `waiting_for_manual_redfin` and performs no mutation.

Candidate validation requires nonempty data, required/non-null columns, numeric
values, unique canonical keys, exactly eleven metrics, complete governed
geography and metric membership, exact family floors/latest month, percentage
bounds, and baseline/drop precedence. It writes a JSON report with old-only,
new-only, matched, changed, and historical-revision counts (including revisions
by metric) when a comparison database is supplied. Diagnostics are reporting,
not rejection scores.

`python -m sources.redfin.validate` unambiguously validates serving rows and
requires `--db` and `--expected-latest`. Separate scripts validate baseline,
drops, candidates, and serving.

## Merge, transaction, publication, and retention

Canonical history is immutable baseline plus the selected validated drop. The
drop wins overlapping canonical keys; baseline supplies older missing history.
Only a `candidate_validated` artifact may apply. Apply begins a transaction,
deletes existing Redfin rows, inserts the candidate, checks row count, exact
metrics, latest month, duplicates, and nulls, then commits. Any failure rolls
back. Reapplying a recorded `serving_refreshed` candidate is a safe no-op.

Publication requires serving refresh and explicit downstream validation.
Promotion updates metadata-only `current/current.json` and append-only
`current/history.json`, including baseline/drop identity, timestamp, candidate
rows, governed geographies, latest month, source hashes, and publication state.
Repeated promotion of the current drop is a no-op.

Retention keeps the permanent baseline plus the latest three successfully
published **and promoted** raw drops. A newer incoming/validated/unpublished drop
blocks raw cleanup. Before deleting an eligible `drops/YYYY-MM` directory,
metadata is retained in `current/drop_history.json` (at least twelve records).
Only exact month directories are eligible. Dry-run is default. Quarantine raw
data is separately eligible after 90 days; promotion and drop history are never
removed.

## Local baseline-only acceptance sequence

Run from the repository root. These commands do not download Redfin data. The
first command validates the real seven local files, supplied hashes, exact floors,
July endpoint, schemas, and percentage contracts:

```bash
python -m scripts.validate_redfin_baseline

python -m scripts.build_redfin_candidate 2026-07 \
  --output data/redfin/redfin_candidate_2026-07.parquet

python -m scripts.validate_redfin_candidate 2026-07 \
  --candidate data/redfin/redfin_candidate_2026-07.parquet \
  --compare-db data/market.duckdb
```

Review `data/redfin/redfin_candidate_2026-07.validation.json` for matched keys,
revisions, production-only keys, candidate-only keys, and the June/July extension.
Do not mutate serving until that review succeeds. Then, intentionally:

```bash
DUCKDB_PATH=data/market.duckdb python -m scripts.apply_redfin_candidate 2026-07 \
  --candidate data/redfin/redfin_candidate_2026-07.parquet

python -m scripts.validate_redfin_serving \
  --db data/market.duckdb --expected-latest 2026-07

python -m sources.redfin.validate \
  --db data/market.duckdb --expected-latest 2026-07
```

Do **not** publish or promote the July-equivalent acceptance run. For a later
manual drop, use:

```bash
python -m scripts.register_redfin_drop 2026-08
python -m scripts.validate_redfin_drop 2026-08
python -m scripts.redfin_drop_status 2026-08
python -m scripts.build_redfin_candidate 2026-08 --output data/redfin/redfin_candidate_2026-08.parquet
python -m scripts.validate_redfin_candidate 2026-08 --candidate data/redfin/redfin_candidate_2026-08.parquet --compare-db data/market.duckdb
python -m scripts.retain_redfin_drops
python -m scripts.redfin_drop_status --current
```

Only intentional downstream orchestration may subsequently apply, validate,
publish, and promote a monthly drop. Downloads, CBSA expansion, analytical
features, regime calibration, forecasting, and dashboard publication are outside
this contract.
