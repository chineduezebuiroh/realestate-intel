# BPS Short-Horizon Production Policy

## Decision

The approved production policy is `BPS-H-LAG6`, recorded under the stable
decision identity `bps_short_horizon_lag6_2026_08_09`. The governed feature
contract is:

| Feature | Transform | Window | Weight |
|---|---|---|---:|
| `bps_total_units_level` | `ma_level` | `12m` | 0.50 |
| `bps_total_units_short` | `ma_pct_change` | `12m/lag6m` | 0.25 |
| `bps_total_units_long` | `ma_pct_change` | `12m/lag12m` | 0.25 |

Thus level is `MA12(raw bps_total_units)`, short is
`MA12 / lag6(MA12) - 1`, and long is `MA12 / lag12(MA12) - 1`. The transform
family remains ratio. Existing positive expanding-percentile normalization is
unchanged, as are the 50/25/25 feature weights and the Supply metric weight of
0.20.

## Evidence and human selection

The lag1, lag3, and lag6 diagnostic remains historical evidence rather than
being rewritten as though lag6 had always been production. Lag1 was rejected as
too reactive: it materially increased short-feature dominance and metric
volatility. Lag3 substantially improved on lag1. Lag6 was selected by a human
because it delivered the strongest stability and contribution balance across
the governed evidence, without using a numeric winner score or an automated
winner.

The corrected responsiveness evidence establishes the accepted cost. Lag6
captures fewer qualified structural turns than lag3 and has a worse
responsiveness tail. Both have approximately one-month median absolute lag, and
their within-three-month responsiveness among matched turns is comparable. The
tail cost is accepted because BPS supplies a structural regime signal rather
than a maximally turn-sensitive signal.

Governance is `recommendation_state=selected`, `promotion_state=promoted`,
`human_decision=approved`, and `bps_short_horizon_calibration_state=closed`.
Lag1 and lag3 are `not_selected`; lag6 is `selected`.

## Scope and next stage

Only the `bps_total_units_short` window changed, from `12m/lag3m` to
`12m/lag6m`. There is no change to the level or long horizon, transform family,
feature weights, normalization, Supply metric or dimension weights, source
precedence, ingestion, NSA treatment, other dimensions, or other Supply
metrics.

The next calibration stage is the BPS feature-weight family. `BPS-H-LAG6` is
frozen throughout that work; this decision does not begin feature-weight
calibration.

## Subsequent feature-weight settlement

This lag decision remains historical and unchanged. The later human-approved
feature-weight settlement promoted `BPS-FINAL-80` (80/10/10) without changing
the lag6 architecture. The current complete BPS contract is documented in
[`bps_production_policy.md`](bps_production_policy.md).
