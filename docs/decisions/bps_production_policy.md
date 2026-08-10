# BPS Production Policy

**Decision identity:** `bps_feature_weight_80_10_10_2026_08_09`  
**Selected policy:** `BPS-FINAL-80`  
**BPS calibration:** complete

## Settled production contract

The human-approved production policy uses the existing scorer and registry-driven
feature weighting without a special case:

| Feature | Production construction | Transform | Weight |
|---|---|---|---:|
| `bps_total_units_level` | `MA12(raw bps_total_units)` | `ma_level`, `12m` | 0.80 |
| `bps_total_units_short` | `MA12 / lag6(MA12) - 1` | `ma_pct_change`, `12m/lag6m` | 0.10 |
| `bps_total_units_long` | `MA12 / lag12(MA12) - 1` | `ma_pct_change`, `12m/lag12m` | 0.10 |

The transform family remains ratio. Normalization remains the existing positive
expanding-percentile production normalization. The `bps_total_units` Supply
metric weight remains 0.20. Supply dimension weights, source precedence, BPS
source ingestion and NSA treatment, `permit_intensity`, every other Supply
metric, Capital Markets, Affordability, and Demand are unchanged.

## Calibration and human decision

The preserved evidence path is: raw BPS diagnostic, transform comparison,
short-horizon comparison, lag6 promotion, five-policy weight family, 70-versus-80
finalist incremental-value review, human selection of 80/10/10, and final
production promotion. Historical diagnostic artifacts and their challenger
identities remain historical context; they are not rewritten as production.

The ratio transform was retained over arithmetic difference. Lag6 was selected
over lag1 and lag3. The five-policy sweep then narrowed review to
`BPS-FINAL-70` (70/15/15) and `BPS-FINAL-80` (80/10/10). The human decision
selected `BPS-FINAL-80`: it provided the better combined stability and
structural-turn tradeoff while short momentum remained a nontrivial movement
driver and momentum-block ablation remained material. `BPS-FINAL-70` remains a
viable but not-selected finalist. The 90/5/5 challenger was rejected because it
suppressed momentum too strongly and approached a level-only architecture.

The evidence includes a mixed recent-36-month volatility field in which 80/10/10
is somewhat worse. This caveat is explicit and was not treated as outweighing
the broader stability and structural-turn evidence. No automated winner score or
ranking determined the selection.

## Governance and promotion evidence

Governance is `recommendation_state=selected`, `promotion_state=promoted`,
`human_decision=approved`, `bps_feature_weight_calibration_state=closed`, and
`bps_calibration_state=complete`; `automated_winner=false`.
`BPS-FINAL-80` is selected and production, while `BPS-FINAL-70` is not selected.

The finalist runner emits compact promotion registry, exact config-diff, parity,
human-decision, and runtime CSVs. Parity fails closed at `1e-12` and compares the
production chronology to the selected `BPS-FINAL-80` chronology across the same
seven governed counties for raw features, normalized features, effective
weights, contributions, and final metric score. A fixture run validates this
contract in hosted environments; an authoritative result may be claimed only
from the frozen source artifact.
