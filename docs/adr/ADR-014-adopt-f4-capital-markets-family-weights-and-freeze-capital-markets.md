# ADR-014: Adopt F4 Capital Markets Family Weights and Freeze Capital Markets

**Status:** Accepted  
**Date:** 2026-08-18

## Context

The decision closes an evidence chain that began with MW-TEMPERED-C and Supply closure, then proceeded through Capital Markets Phase 1 anatomy and native feature-weight calibration. That work exposed an inverted physical 2Y-10Y source being treated as canonical 10Y-2Y. The canonical source boundary was repaired and proved, corrected `spread_10y_2y` P0-P9 revalidation promoted P6, and the F0-F9 family grid was rerun from the corrected immutable baseline `capital_markets_feature_policy_corrected_production_20260818`. Defect-era family evidence remains invalid history and was not used.

## Decision

Promote the human-approved F4 family policy: 35% Long-Term Rates, 10% Fed Funds, and 55% Spreads. This is a policy selection within a practical plateau, not a mathematical optimum; `automated_winner = false`.

Equal within-family weighting produces these exact production metric weights:

| Metric | Weight |
|---|---:|
| `mortgage_30y` | 0.11666666666666667 |
| `mortgage_15y` | 0.11666666666666667 |
| `treasury_10y` | 0.11666666666666667 |
| `fedfunds` | 0.10 |
| `spread_10y_2y` | 0.275 |
| `spread_10y_fedfunds` | 0.275 |

The frozen native policies remain P4 (55/10/35), P2 (60/10/30), P1 (60/15/25), P5 (50/10/40), P6 (60/05/35), and P9 (40/10/50), respectively. No transform, window, lag, normalization, membership, or axis weight changes. Canonical `spread_10y_2y = treasury_10y - treasury_2y` continues to invert physical `fred_spread_2y_10y = treasury_2y - treasury_10y`. Demand and Supply Capital Markets weights remain 0.10 and 0.15; Supply remains frozen at S8.

## Rationale

Against F0 (45/10/45), corrected F4 evidence had fewer full-history reversals, lower two- and three-month whipsaw, greater persistence, lower cancellation, better net-to-gross behavior, and materially stronger recent stability, without a material responsiveness penalty. Correct polarity makes the Spread pair coherent rather than mirror-imaged.

F2 (40/10/50) approximately begins the practical plateau. F4 is the preferred point within it because the extra Spread allocation delivered meaningful full-history stability improvement without material responsiveness loss. Fed Funds remains 10%: increases to 15%, 20%, or 25% reduced some concentration/cancellation but were not sufficiently rewarded in responsiveness or stability, while 5% established no compelling advantage.

Corrected Spread-pair and long-rate evidence is sufficiently coherent to retain equal weights. Extra intra-family optimization would add dimensionality and governance complexity without evidence of material expected gain, so `intra_family_metric_weight_calibration = not_required` unless material future evidence warrants reopening.

## Rejected alternatives

- **F0:** credible incumbent, but leaves supported stability improvement unrealized.
- **F2:** begins the plateau, but F4 adds supported stability improvement.
- **F3/F1:** retain excessive Rates allocation relative to corrected evidence.
- **F5/F9:** 5% Fed Funds is not compelling.
- **F6/F7/F8:** higher Fed Funds is not sufficiently rewarded.
- **Intra-family optimization:** expected gain does not justify complexity.

## Consequences

Capital Markets is fully frozen: no feature, metric/family weight, normalization, polarity, membership, or axis-weight calibration remains pending. Reopening requires material evidence enumerated by `capital_markets_frozen_f4_2026_08_18`; preference-driven retuning is insufficient. The next active workstream is **Macro Regime Visualization MVP**.
