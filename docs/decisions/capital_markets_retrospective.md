# Capital Markets Calibration Retrospective

## Status

**Status:** Complete  
**Production policy:** `capital_markets_mw_tempered_c_2026_08_07`  
**Calibration state:** Closed  
**Promotion state:** Promoted  
**Human decision:** Approved  

Capital Markets calibration is complete. Future changes should require evidence of a production defect or materially changed source behavior, not preference-driven retuning.

---

## 1. Final Production Architecture

| Metric | Structural policy | Feature weights | Metric weight |
|---|---|---:|---:|
| `mortgage_30y` | MA12 ratio structural | 60 / 20 / 20 | 15.0% |
| `mortgage_15y` | MA12 ratio structural | 60 / 20 / 20 | 15.0% |
| `treasury_10y` | MA12 ratio structural | 60 / 20 / 20 | 15.0% |
| `fedfunds` | MA3 ratio structural | 60 / 20 / 20 | 10.0% |
| `spread_10y_2y` | MA9 arithmetic-difference structural | 60 / 20 / 20 | 22.5% |
| `spread_10y_fedfunds` | MA9 arithmetic-difference structural | 60 / 20 / 20 | 22.5% |

Family totals:

```text
Long-rate family   = 45%
Policy-rate family = 10%
Spread family      = 45%
```

---

## 2. Final Feature Definitions

### Rate metrics

For `mortgage_30y`, `mortgage_15y`, `treasury_10y`, and `fedfunds`:

```text
level = MAx(raw)
short = MAx(raw) / lag3(MAx(raw)) - 1
long  = MAx(raw) / lag12(MAx(raw)) - 1
```

where:

```text
x = 12 for mortgage_30y
x = 12 for mortgage_15y
x = 12 for treasury_10y
x = 3  for fedfunds
```

### Spread metrics

For `spread_10y_2y` and `spread_10y_fedfunds`:

```text
level = MA9(raw)
short = MA9(raw) - lag3(MA9(raw))
long  = MA9(raw) - lag12(MA9(raw))
```

Arithmetic-difference short/long features remain in the source percentage-point scale before normalization.

---

## 3. Spread Formula and Polarity Decision

Canonical spread identities:

```text
spread_10y_2y = treasury_10y - treasury_2y
spread_10y_fedfunds = treasury_10y - fedfunds
```

Naming convention:

```text
spread_<long leg>_<short or policy leg>
```

Economic interpretation:

```text
positive / steeper spread = more favorable
negative / inverted spread = less favorable
```

Accordingly:

```text
rate metrics   -> lower values are more favorable
spread metrics -> higher / more-positive values are more favorable
```

---

## 4. Why Rates and Spreads Use Different Transforms

Ratio structural features remain appropriate for rate levels because those series are positive-valued and proportional movement remains interpretable.

Yield-curve spreads legitimately cross zero. A ratio such as `current MA / lagged MA - 1` can become unstable near zero and can imply the wrong movement direction when the denominator is negative.

The spread review exposed near-zero denominator exposure, extreme ratio magnitudes, direction conflicts between economic movement and ratio-implied movement, and sign-crossing sensitivity.

Arithmetic differences remove those pathologies while preserving economically meaningful curve movement. The settled spread policy is therefore **MA9 structural arithmetic difference** for both active spread metrics.

---

## 5. Why Different MA Windows Were Selected

The six Capital Markets metrics did not support a single universal smoothing window.

- `mortgage_30y`, `mortgage_15y`, and `treasury_10y` use MA12 because the objective is structural financing-condition state rather than short-term monthly rate noise.
- `fedfunds` uses MA3 because monetary-policy changes occur in discrete policy cycles and slower treatment introduced excessive structural lag.
- `spread_10y_2y` and `spread_10y_fedfunds` use MA9 because curve shape benefited from substantial smoothing while MA12 was unnecessarily aggressive relative to the useful movement retained by MA9.

This classification is local to Capital Markets and is not a project-wide macro-series taxonomy.

---

## 6. Feature-Weight Decision

The settled feature-weight review evaluated:

```text
FW-A = 50 / 25 / 25
FW-B = 60 / 20 / 20
FW-C = mixed policy with Fed Funds at 50 / 25 / 25
```

The selection criterion was not similarity to the incumbent. Candidates were judged on absolute metric stability, turning-point behavior, Capital Markets dimension stability, extreme-jump behavior, cancellation, and trend preservation.

`FW-B` was selected:

```text
level = 60%
short = 20%
long  = 20%
```

for all six metrics.

---

## 7. Metric-Weight Experiments

The original production weights were materially concentrated:

```text
mortgage_30y        35%
mortgage_15y         5%
treasury_10y        15%
fedfunds             15%
spread_10y_2y       20%
spread_10y_fedfunds 10%
```

An initial equal-family design allocated one-third each to long rates, Fed Funds, and spreads. It materially reduced concentration and improved some turning-point statistics, but Fed Funds at one-third of the dimension proved too aggressive. It materially increased upper-tail dimension volatility, P90/P99 movement, sign flips, Fed Funds contribution magnitude, and Fed Funds dominance of the largest Capital Markets jumps.

The equal-family experiment therefore established an upper-bound warning for policy-rate weight rather than a viable final policy.

---

## 8. Tempered Metric-Weight Finalists

The final review compared:

```text
MW-INCUMBENT   = 55 / 15 / 30  (long rates / policy / spreads)
MW-TEMPERED-C = 45 / 10 / 45
MW-TEMPERED-A = 40 / 20 / 40
MW-TEMPERED-B = 40 / 25 / 35
```

`MW-TEMPERED-C` was selected:

```text
mortgage_30y        = 15.0%
mortgage_15y        = 15.0%
treasury_10y        = 15.0%
fedfunds            = 10.0%
spread_10y_2y       = 22.5%
spread_10y_fedfunds = 22.5%
```

---

## 9. Why MW-TEMPERED-C Won

Relative to the prior incumbent, the selected policy materially reduced configured and realized contribution concentration, lowered median monthly Capital Markets movement and rolling volatility, reduced qualified Capital Markets turning points from 18 to 10, reduced recent-36-month turning points from 5 to 3, materially reduced Fed Funds contribution magnitude, and reduced Fed Funds attribution among the largest monthly jumps and qualified turning points.

The selected policy produced a larger single maximum monthly jump than the incumbent. That isolated event did not outweigh the broader evidence because P90 and P99 movement remained approximately comparable to incumbent while median stability, rolling volatility, turning-point behavior, and contribution concentration improved materially.

No similarity-to-incumbent reward was used.

---

## 10. Production Promotion

The selected diagnostic policy was promoted into production as:

```text
capital_markets_mw_tempered_c_2026_08_07
```

Production parity validation confirmed active metric membership, transform family, MA windows, 60/20/20 feature weights, promoted metric weights, exact promoted-versus-selected Capital Markets chronology, unchanged Capital Markets dimension weight, unchanged axis weights, unchanged Supply and Affordability policies, and unchanged source precedence.

Production support for `ma_difference` was added so the promoted spread policy can be executed by the standard feature engine.

---

## 11. Fed Funds Availability Lesson

Historical Fed Funds contribution summaries can be distorted during early periods when not all six Capital Markets metrics are available. Production scoring correctly renormalizes weights across available metrics, but historical maximum and P99 contribution statistics may then reflect availability renormalization rather than configured steady-state policy weight.

For policy comparison, the final review therefore also used an all-six-metrics-available chronology. Under that fully observed comparison, the promoted 10% Fed Funds allocation materially reduced median, P90, P99, and maximum absolute Fed Funds contribution as well as Fed Funds attribution among top Capital Markets jumps and qualified turning points.

---

## 12. Engineering Lessons

1. **Transform choice must respect source mathematics.** Ratio transforms should not be applied mechanically to zero-crossing spreads.
2. **Stability is not similarity.** Challengers should be judged on absolute structural quality, not incumbent resemblance.
3. **Family balance is useful, but equal weighting is not automatically optimal.** A one-metric family can become dominant if given the same family weight as multi-metric families.
4. **Level chronology and movement chronology are different contracts.** Lag/diff warmup must not truncate valid level observations.
5. **Availability renormalization must be preserved.** Diagnostics must distinguish unavailable observations from zero values.
6. **Evidence must remain policy-generation consistent.** Parent and child covariance/decomposition chronologies must come from the same policy generation.
7. **Diagnostics should fail closed.** Repair inconsistent evidence rather than weakening validators.

---

## 13. Calibration Closure

Capital Markets is now a production specification rather than an open calibration area.

```text
Status: COMPLETE
```

Future modifications require evidence of a production defect, materially changed source behavior, degraded out-of-sample stability, incorrect economic polarity, unacceptable chronology behavior, or another substantive production failure. Marginal preference changes are not sufficient reason to reopen calibration.

---

## 14. Final Production Specification

| Metric | Window | Transform | Feature weights | Metric weight |
|---|---:|---|---:|---:|
| `mortgage_30y` | MA12 | ratio | 60 / 20 / 20 | 15.0% |
| `mortgage_15y` | MA12 | ratio | 60 / 20 / 20 | 15.0% |
| `treasury_10y` | MA12 | ratio | 60 / 20 / 20 | 15.0% |
| `fedfunds` | MA3 | ratio | 60 / 20 / 20 | 10.0% |
| `spread_10y_2y` | MA9 | arithmetic difference | 60 / 20 / 20 | 22.5% |
| `spread_10y_fedfunds` | MA9 | arithmetic difference | 60 / 20 / 20 | 22.5% |

Family totals:

```text
Long rates  = 45%
Policy rate = 10%
Spreads     = 45%
```

---

## 15. Next Calibration Stage

The next active stage is **Affordability**:

1. confirm the derivation-order contract for `price_to_income` and `payment_burden`;
2. retain MA12 as the structural smoothing window;
3. compare the remaining feature-weight finalists;
4. promote the resulting Affordability policy only after diagnostic review.

Building Permit Survey visual diagnostics remain a separate subsequent task.
