# LAUS-B governed adapter and bootstrap implementation v0.1

**Status:** local implementation and fixture validation only. No live BLS request,
GitHub publication, catalog mutation, accepted-pointer change, or hosted monthly
integration occurred.

## Implementation boundary

`config/laus_governed_series_v1.csv` is the implementation-owned request registry.
It contains exactly 820 NSA series: 615 required and target-controlling plus 205
diagnostic, across 205 geographies and four physical metrics. It contains the
provider area code, physical metric, unit, scale transform, governance class and
target flag. The adapter does not read the LAUS-A audit CSV and never selects the
20 legacy SA series.

`sources/bls_laus/artifact.py` is DuckDB-independent. It owns strict registry
validation; explicit ordinary/deep/bootstrap bounds; deterministic 50-series and
20-year partitioning; registered BLS v2 invocation with bounded transient retry;
all-or-nothing membership/schema/value validation; unconditional M13 removal;
month-end canonicalization; the 615-series common target; deterministic request
and provider identities; and provider-wins/prior-preserving reconciliation. It has
no database, catalog, Release, pointer, orchestration, or wall-clock planning side
effect. CES code was used as a proven design reference, but no common abstraction
or accepted CES implementation was changed.

## Controlled bootstrap

`jobs/monthly_refresh/laus_bootstrap.py` exposes `audit`, `recover`, `publish`,
`activate`, and `verify` phases. Audit requires an explicit end year, reads legacy
DuckDBs in read-only mode, persists the complete all-or-nothing acquired BLS
responses before transform/report/artifact work, hashes intermediate evidence, and
can resume without another provider call. Workspace request/end-year, catalog
preflight, hashes, required diagnostics and internal identities fail closed.

The equivalence report classifies exact match, provider revision, provider newer,
provider historical-only, legacy prior-only, identity mismatch, unexplained
numeric mismatch and systematic 1000x unit mismatch at a fixed `1e-12` technical
tolerance. Provider revision is permitted only for the same configured identity,
canonical key, unit and transform under the explicit bootstrap revision policy.
Identity, scale, unexplained numeric, completeness, uniqueness and numeric gates
block candidate construction.

Publication is separately gated by passed audit, common artifact validation, and
an explicit authenticated remote-inventory completion flag. Activation is
separate, resolves exactly one cataloged LAUS artifact, protects CES/FRED/Redfin
pointers, and uses the common catalog CAS. Verification performs fresh durable
resolution. None of these remote phases was executed here.

## Persisted live acquisition corrective finding (2026-08-31)

Unlike fixture evidence, the already-persisted live bootstrap responses contain
820 explicit provider-unavailable observations: exactly one for every governed
series, all at `2025-10`. Every datum has provider marker `"-"`, BLS footnote code
`X`, and the explanation “Data unavailable due to the 2025 lapse in
appropriations.” Inspection found zero series with an entirely omitted interior
month. The corrective adapter preserves these facts in provider identity and
diagnostics while omitting them from numeric canonical rows; it does not treat
omitted periods or unexplained nonnumeric content as this state.

## Preflight findings

The local artifact catalog still has no immutable LAUS record and no
`accepted.source.laus`. GitHub CLI is unauthenticated, so read-only inventory of
`source-artifact/laus/*` could not be performed and remains a publication-blocking
preflight gate. `BLS_API_KEY` is unset, so no live acquisition was attempted.

Read-only extraction (with file hashes unchanged) found:

| database | four governed physical metric rows | configured v1 identity | legacy identity drift |
|---|---:|---:|---:|
| `data/market_serving.duckdb` | 210,348 | 196,008 | 14,340 |
| `data/market_public.duckdb` | 36,256 | 0 | 36,256 |

These are structural preflight counts, not live provider equivalence results.
They predict hard identity-review work for the legacy public aliases; no gate was
weakened and neither database was modified.

## Human execution command

After authenticated remote tag inventory confirms no collision and a registered
BLS key is available, run audit/equivalence only:

```bash
BLS_API_KEY='<registered-key>' PYTHONPATH=. python -m jobs.monthly_refresh.laus_bootstrap audit \
  --end-year 2026 \
  --output-root artifacts/bootstrap/laus/laus-bootstrap-1976-2026
```

If a post-acquisition local phase fails, replay from persisted evidence without
BLS access:

```bash
PYTHONPATH=. python -m jobs.monthly_refresh.laus_bootstrap recover \
  --end-year 2026 \
  --output-root artifacts/bootstrap/laus/laus-bootstrap-1976-2026
```

Stop after reviewing `acceptance.json`, `equivalence.json`,
`secondary_equivalence.json`, `equivalence_detail.parquet`, completeness,
identities and the validated local candidate. Do not publish or activate without a
new human authorization.

## Remaining OPEN items

* authenticated read-only inventory of governed/historical LAUS Release tags;
* live BLS bootstrap/equivalence evidence and resolution of any hard legacy
  identity/equivalence findings;
* exact annual deep-reconciliation production calendar month (LAUS-C or later).

The next action is human execution/review of the LAUS-B audit command above. Only
after acceptance should a separately authorized publication/activation exercise
occur. Routine hosted LAUS orchestration remains LAUS-C and is not implemented.
