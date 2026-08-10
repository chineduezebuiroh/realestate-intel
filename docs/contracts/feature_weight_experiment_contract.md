# MA12 Structural Definition × Feature-Weight Experiment Contract v1

`ma12_structural_feature_weight_experiment_v1` is an immutable, diagnostic-only
contract. It covers exactly `active_inventory`, `permit_activity`,
`permit_intensity`, `median_sale_price`, `median_ppsf`, `price_to_income`, and
`payment_burden` across the governed seven-county review set.

The production registry resolves one enabled level, short-term, and long-term
feature for each metric before challenger construction. Alternative A is
`0.50 / 0.25 / 0.25`; Alternative B is `0.45 / 0.25 / 0.30`; Alternative C is
`0.60 / 0.20 / 0.20`. Each challenger intervenes on one metric only. The
experiment therefore contains seven incumbent metric baselines and 28
single-metric challengers: seven MA12 structural families at incumbent weights
and seven for each immutable alternative. Each MA12 family is rebuilt once with
the governed structural implementation, cached across its four policies, and
normalized with the production normalizer. It does not merely reweight incumbent
feature scores. It consumes incumbent and rebuilt normalized features and uses production causal-splice outputs for
downstream evidence; it implements no alternate production engine or combined
intervention.

MA6 and MA9 are outside this experiment. All unaffected artifacts require exact, duplicate-safe, schema-safe, dtype-safe,
and null-safe parity. Leading warmup is reportable, while interior/trailing gaps
and challenger-only dates fail closed. Evidence is descriptive at metric level:
there is no universal score, automatic recommendation, registry mutation, or
promotion.

## Acceptance criteria

The evidence is accepted only when all seven registry-resolved incumbent
baselines and all 28 single-metric challengers are present in deterministic
incumbent/MA12-incumbent/MA12-A/B/C order; each policy sums exactly to `1.0`; decomposition
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

## Settled production promotion lineage

Promotion version: `settled_ma12_feature_policy_promotion_2026_08_05`.

Evidence source: `ma12_structural_feature_weight_experiment_v1`.

Human-approved status: approved for the five settled direct/derived supply and
price metrics listed below only.

Promotion scope:

| Canonical metric | Registry metric key | Prior production definition | Promoted production definition | Prior weights | Promoted weights |
|---|---|---|---|---|---|
| `active_inventory` | `redfin_inventory` | `level_zscore`; `mom_zscore`; `yoy_zscore` | `ma_level` (`12m`); `ma_pct_change` (`12m/lag3m`); `ma_pct_change` (`12m/lag12m`) | `0.25 / 0.35 / 0.40` | `0.50 / 0.25 / 0.25` |
| `permit_activity` | `bps_total_units` | `ma12_level`; `ma3_vs_ma12_pct`; `ma12_yoy_pct` | `ma_level` (`12m`); `ma_pct_change` (`12m/lag3m`); `ma_pct_change` (`12m/lag12m`) | `0.25 / 0.35 / 0.40` | `0.50 / 0.25 / 0.25` |
| `permit_intensity` | `derived_permit_intensity` | `ma12_level`; `ma3_vs_ma12_pct`; `ma12_yoy_pct` | `ma_level` (`12m`); `ma_pct_change` (`12m/lag3m`); `ma_pct_change` (`12m/lag12m`) | `0.25 / 0.35 / 0.40` | `0.50 / 0.25 / 0.25` |
| `median_sale_price` | `redfin_median_sale_price` | `level_zscore`; `mom_zscore`; `yoy_zscore` | `ma_level` (`12m`); `ma_pct_change` (`12m/lag3m`); `ma_pct_change` (`12m/lag12m`) | `0.20 / 0.40 / 0.40` | `0.50 / 0.25 / 0.25` |
| `median_ppsf` | `redfin_median_ppsf` | `level_zscore`; `mom_zscore`; `yoy_zscore` | `ma_level` (`12m`); `ma_pct_change` (`12m/lag3m`); `ma_pct_change` (`12m/lag12m`) | `0.20 / 0.40 / 0.40` | `0.50 / 0.25 / 0.25` |

Promoted governed structural feature definitions:

```text
level = trailing MA12(raw metric)
short = trailing MA12(raw metric) / lag3(trailing MA12(raw metric)) - 1
long  = trailing MA12(raw metric) / lag12(trailing MA12(raw metric)) - 1
```

Direct metrics (`active_inventory`, `permit_activity`,
`median_sale_price`, and `median_ppsf`) apply this feature family directly to
their canonical raw observations. `permit_intensity` preserves the established
derive-then-smooth order: raw `permit_activity` plus raw/carried-forward
`population`, then derived `permit_intensity`, then one governed MA12 structural
feature family.

No metric-to-dimension weights, dimension-to-axis weights, Supply metric
weights, source precedence, geography policy, normalization method, regime
geometry, transition logic, or cancellation logic changed in this promotion.
`price_to_income`, `payment_burden`, Capital Markets metrics, and Supply metric
reweighting remain pending and outside this promotion.

## Corrective MA12 structural implementation audit

The governing MA12 structural contract is the same-state lag contract above.
`ma12_structural_feature_weight_experiment_v1` used the correct lag3-of-MA12
formula for its MA12 challenger construction: direct non-price targets were
rebuilt through `build_smoothed_metric_features_wide()` with
`level_window=12`, `short_window=12`, and `short_lag_periods=3`; price-family
targets were rebuilt through the linked Price-family shared implementation,
which calculates short features from the structural level lagged three
observations. The prior promotion registry and promotion-lineage prose used the
ambiguous/incorrect `ma3_vs_ma12_pct` short transform and therefore required
correction before the production promotion could be considered valid.

| Transform or implementation | Current formula | Intended formula | Affected metrics | Production usage | Experiment usage | Correction required |
|---|---|---|---|---|---|---|
| `ma_level` with `12m` | `MA12(raw)` | `MA12(raw)` | Five promoted metrics | Selected for promoted level features | Used by generic MA policies | No |
| `ma_pct_change` with `12m/lag3m` | `MA12(raw) / lag3(MA12(raw)) - 1` | `MA12(raw) / lag3(MA12(raw)) - 1` | Five promoted metrics | Selected for promoted short features | Used by feature-weight MA12 reconstruction | No |
| `ma_pct_change` with `12m/lag12m` | `MA12(raw) / lag12(MA12(raw)) - 1` | `MA12(raw) / lag12(MA12(raw)) - 1` | Five promoted metrics | Selected for promoted long features | Used by feature-weight MA12 reconstruction | No |
| `ma12_level` | `MA12(raw)` | `MA12(raw)` | Legacy/specialized MA12 rows only | Not used by the five promoted metrics after correction | Legacy smoke/registry coverage | No for level semantics |
| `ma3_vs_ma12_pct` | `MA3(raw) / MA12(raw) - 1` | Not a valid MA12 structural short formula | Previously selected short rows for the five promoted metrics | Removed from the five promoted metrics | Not used by `ma12_structural_feature_weight_experiment_v1` MA12 reconstruction | Yes; production registry corrected |
| `ma12_yoy_pct` | `MA12(raw) / lag12(MA12(raw)) - 1` | `MA12(raw) / lag12(MA12(raw)) - 1` | Legacy/specialized long rows only | Replaced by explicit `ma_pct_change` `12m/lag12m` for the five promoted metrics to avoid shorthand ambiguity | Legacy smoke/registry coverage | No for formula; replaced for clarity |
| Linked Price-family shared implementation | structural level / lag3(structural level) - 1; structural level / lag12(structural level) - 1 | Same | `median_sale_price`, `median_ppsf`, plus linked affordability candidates | Production linked affordability observation policy | Feature-weight experiment and price-family smokes | No |

Because the experiment implementation used the correct formula, its synthetic
formula tests remain implementation-valid. Any authoritative results generated
from the transient incorrect production registry promotion must not be reused;
registry-backed production evidence should be rerun under the corrected
`ma_pct_change` `12m/lag3m` short definition.
