# Macro Regime publication reporting month

The published reporting month is the latest accepted Redfin **observation
month** for the selected governed monthly cycle. It is distinct from source
vintage, download/ingestion time, and Macro evaluation execution time.
Other sources can have earlier or later observations; publication does not
require all sources to align to Redfin's month. Only persisted evidence on or
before the reporting month-end may contribute to the rendered assessment.
Existing production as-of alignment, freshness, scoring, weights, registries,
and missingness contracts remain authoritative.

## Explicit publication boundary

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. python -m scripts.build_macro_regime_site \
  --run artifacts/regime/runs/macro_regime_production_202608_v1 \
  --reporting-month 2026-08 \
  --output /tmp/macro-regime-site-august-2026
```

The explicit override selects exactly that month's persisted month-end state
for Washington, DC. Missing month-end state, incomplete weighted dimensions,
or metric evidence that cannot reconcile the persisted dimensions fails;
there is no forward/backward fallback. The same cutoff controls all drivers,
interpretation, freshness, trajectory, and charts. No artifacts are rewritten
and no scoring is executed. Omission retains the existing latest-common-date
behavior for compatibility; governed publication should pass an explicit month.

## Reusable Redfin resolution and automation boundary

Source Set v2 already pins each entry's `observation_max` under its semantic
identity and validates current-cycle eligibility. Its Redfin contract requires
a refreshed exact target-month artifact. This provides sufficient metadata to
resolve an observation month without introducing governance infrastructure:

```python
import json
from visualization.reporting_month import redfin_reporting_month

# Resolve the exact manifest through existing governed artifact verification.
# Supply the accepted Source Set ID for the selected cycle, not an inferred ID.
source_set = json.loads(exact_verified_source_set_manifest.read_text())
month = redfin_reporting_month(source_set, accepted_source_set_id=accepted_id)
# Pass month to build(..., reporting_month=month), or to --reporting-month.
```

The helper validates the full Source Set v2 contract and semantic identity,
checks the caller's accepted ID, and reads only Redfin's ISO observation date.
It does not itself certify remote publication or acceptance. Source Set target
month, artifact names, provider release IDs, and execution timestamps are not
substitutes for `observation_max`. Explicit overrides remain validated against
the requested run's persisted evidence, even when differing from policy.

Automatic CLI resolution is deferred. The artifact-only renderer has no governed
contract binding a Macro run to an accepted Source Set and monthly cycle. The
pipeline records the input database SHA and adapter provenance. Existing
immutable catalog records can link a verified serving SHA through canonical
market to Source Set and cycle, but the renderer currently consumes none of
those records or exact remote manifests. Consulting the mutable
current catalog would select the wrong cycle for historical runs. A follow-up
must decide how to supply or resolve that verified binding before wiring an
automatic default; this PR changes no Source Set governance, orchestration,
scoring, or publication/deployment workflows.

## August 2026 evidence and local validation

Implementation baseline: `40b950b4edda9dff30a93754735654aab0da28e1`.
The checked-in accepted catalog matches the requested cycle's Source Set,
canonical, and serving identities:

- Cycle: `monthly_cycle__2026-08__a9e022a980d29cd7`
- Source Set: `source_set__2026-08__v2__b429053765fb9fed`
- Canonical: `market__2026-08__r2__7793c3e80612bf80`
- Serving: `serving__market__2026-08__r2__7793c3e80612bf80__58610cddf4015d11`

The accepted logical cohort plan records Redfin `observation_max=2026-08-31`;
its FRED source extends into September. This corroborates the month boundary
problem but does not prove August Macro dimension completeness.

The immutable run `macro_regime_production_202608_v1` is absent in Codex.
Its reported complete status, 29 verified artifacts, September scores, and
accepted serving database SHA (also matched in catalog metadata)
`68f9bbb01c5b597a0dedb8db42891c76ed170a745ed8384a293ea04eeec1a7ea`
are operator-supplied evidence, not independently reverified here. The exact
accepted Source Set package was not downloaded. Fixture tests demonstrate
faithful August selection under the existing complete-data contracts, including
reconciliation of metric contributions to dimensions. They cannot establish
that the real August evidence satisfies those contracts.

Gate 1 therefore passes at code/fixture level only; real-run acceptance remains
pending the command above in the Mac worktree containing the immutable run.
If that command fails completeness or reconciliation, stop for review; do not
rerun the pipeline or relax missingness/scoring contracts. Gate 2 supplies a
reusable metadata resolver; automatic run-to-cycle selection is a separate
contract decision. Nothing has been merged or deployed.
