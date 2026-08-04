# Level-Biased Feature-Weight Experiment Contract v1

`level_biased_feature_weight_experiment_v1` is an immutable, diagnostic-only
contract. It covers exactly `active_inventory`, `permit_activity`,
`permit_intensity`, `median_sale_price`, `median_ppsf`, `price_to_income`, and
`payment_burden` across the governed seven-county review set.

The production registry resolves one enabled level, short-term, and long-term
feature for each metric before challenger construction. Alternative A is
`0.50 / 0.25 / 0.25`; Alternative B is `0.45 / 0.25 / 0.30`; Alternative C is
`0.60 / 0.20 / 0.20`. Each challenger intervenes on one metric only. The
experiment therefore contains seven incumbent metric baselines and 21
single-metric challengers: seven for each immutable alternative. It consumes
incumbent normalized features and uses production causal-splice outputs for
downstream evidence; it implements no alternate production engine or combined
intervention.

All unaffected artifacts require exact, duplicate-safe, schema-safe, dtype-safe,
and null-safe parity. Leading warmup is reportable, while interior/trailing gaps
and challenger-only dates fail closed. Evidence is descriptive at metric level:
there is no universal score, automatic recommendation, registry mutation, or
promotion.

## Acceptance criteria

The evidence is accepted only when all seven registry-resolved incumbent
baselines and all 21 single-metric challengers are present in deterministic
incumbent/A/B/C order; each policy sums exactly to `1.0`; decomposition
arithmetic reconciles; coverage and unaffected parity pass; and the review
retains the diagnostic-only, no-recommendation, no-promotion state.

## Completion report contract

Completion reporting must identify the authoritative incumbent weights and
feature identities, all three challenger definitions, metric-level stability
and level-influence findings, coverage/warmup, downstream dimension and axis
effects, regime changes, materially different metric behavior, contract
identity, runtime and bundle size, tests, Git/PR records, and confirmation that
neither production policy nor promotion state changed. Synthetic validation
must not be presented as authoritative findings.
