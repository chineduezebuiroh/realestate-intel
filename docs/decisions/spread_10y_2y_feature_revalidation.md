# `spread_10y_2y` feature-policy revalidation

## Status

Human decision pending. No recommendation, winner, registry mutation, or
production promotion is made by this diagnostic.

## Corrected baseline and scope

The sole authoritative empirical input is
`artifacts/regime/runs/capital_markets_spread_polarity_repair_20260818`. It
governs the canonical spread as `treasury_10y - treasury_2y`; the physical FRED
series is retained as `treasury_2y - treasury_10y` and inverted before feature
construction. The earlier P7 evidence was produced from inverted chronology and
therefore cannot establish a feature-weight decision on the corrected series.

That corrected run was intentionally materialized with the temporarily retained
P7 feature policy (35/10/55). Consequently, P7 is the corrected persisted-run
arithmetic baseline: it must exactly reconstruct the persisted metric,
Capital Markets dimension, Demand axis, and Supply axis. Every one-metric
scenario is propagated as its score delta from P7. This reconstruction role does
not make P7 a winner; its governance status remains `revalidation_required`.

P0 remains the historical 60/20/20 feature-policy reference in the unchanged
P0-P9 review grid. Candidate-versus-P0 evidence may describe distance from that
historical policy, but P0 is not the corrected production baseline and is not
required to reconstruct corrected persisted artifacts.

Only `spread_10y_2y` is reopened. Level remains MA9, Short remains the lag-3
arithmetic difference of MA9, Long remains the lag-12 arithmetic difference of
MA9, and normalization remains positive. The closed grid is P0 through P9, with
the controlled Long ladder P4 → P5 → P8 → P9 → P7. P7 is
`revalidation_required`, not an incumbent winner.

The other five Capital Markets feature policies are provisionally valid. Metric,
family, Demand, and Supply weights remain fixed. Family-weight calibration
remains invalidated and blocked pending this human feature-policy decision and a
subsequent corrected rerun. Historical ADR-012 remains unchanged as historical
evidence.
