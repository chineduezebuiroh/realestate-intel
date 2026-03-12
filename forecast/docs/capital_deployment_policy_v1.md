# Capital Deployment Policy v1
## Forecast-Based Decision Framework

This document defines the first operational decision policy derived from the forecasting system.  
The goal of the system is **capital deployment timing**, with acquisition timing as a secondary use.

The policy combines multiple forecast layers to produce a structured decision output.

---

# 1. System Architecture

## Core Metrics

| Role | Metric | Horizon | Model |
|-----|------|------|------|
| Payoff (primary) | median_ppsf | 12m | SARIMAX univariate (tuned) |
| Confirmation | median_sale_price | 12m | SARIMAX univariate (tuned) |
| Tactical timing | median_ppsf | 6m | SARIMAX univariate (incumbent / old) |
| Market friction | median_dom | current + short trend | observed state |
| Persistence (future) | median_ppsf | 24m | TBD |

---

# 2. Model Assignments

### Core Payoff

median_ppsf
horizon = 12 months
model = sarimax_univariate_tuned_h12

Purpose:
- Measure underlying housing economics
- Primary driver of capital deployment decision

---

### Confirmation

median_sale_price
horizon = 12 months
model = sarimax_univariate_tuned_h12

Purpose:
- Validate economic signal from PPSF
- Ensure broader market price action agrees

---

### Tactical Timing

median_ppsf
horizon = 6 months
model = sarimax_univariate_old

Purpose:
- Detect short-term tension vs the core signal
- Used for **entry timing adjustments**

---

### Market Friction Modifier

median_dom

Purpose:
- Capture liquidity and execution friction
- Not used as a primary forecast regime signal

Inputs used:

- current DOM percentile
- 3-month DOM change
- 6-month DOM change

---

### Persistence Layer (Future)

median_ppsf
horizon = 24 months
model = TBD

Purpose:
- Determine regime persistence
- Increase or reduce conviction

---

# 3. Decision Framework

The system produces a structured output with four components.

## Output Fields

| Field | Meaning |
|-----|-----|
| Action | deployment recommendation |
| Confidence | strength of signal |
| Timing State | entry timing recommendation |
| Friction State | liquidity conditions |

---

# 4. Base Action (Layer 1)

Derived from:
- median_ppsf forecast (12m)

Regime buckets are calculated from historical forward-return quantiles.

| Condition | Action |
|------|------|
| positive direction + top regime | DEPLOY |
| positive direction + mid-high regime | DEPLOY_LIGHT |
| neutral regime | HOLD |
| negative direction | DEFENSIVE |

This layer determines the **base capital posture**.

---

# 5. Confirmation Layer (Layer 2)

Input:
- median_sale_price forecast (12m)

Rules:

### Agreement
If sale_price agrees with PPSF:

- increase confidence level

### Weak Agreement

If direction agrees but regime is weaker:

- leave base action unchanged

### Disagreement

If direction disagrees:

downgrade action one level

DEPLOY -> DEPLOY_LIGHT
DEPLOY_LIGHT -> HOLD
HOLD -> DEFENSIVE

---

# 6. Tactical Timing Layer (Layer 3)

Input:
- median_ppsf forecast (6m)

Compare with 12m PPSF signal.

### Agreement
If 6m agrees with 12m:
- Timing State = ACT_NOW

### Disagreement
If 6m disagrees:
- Timing State = STAGE_ENTRY

Meaning:

- smaller initial deployment
- wait for confirmation on next refresh

---

# 7. Friction Modifier (Layer 4)

Input:
- median_dom

Interpretation:

| DOM Condition | Friction State |
|------|------|
| falling / improving | LOW_FRICTION |
| stable | NORMAL_FRICTION |
| rising rapidly | HIGH_FRICTION |

Rules:

- High friction caps deployment size
- Low friction allows full size

DOM **cannot override directional signal**.

---

# 8. Persistence Layer (Layer 5)

Future feature.

Uses:
- median_ppsf forecast (24m)

Purpose:

- detect regime durability

Rules (planned):

| Condition | Effect |
|------|------|
| agrees with 12m | increase conviction |
| disagrees with 12m | cap aggressiveness |

---

# 9. Final Output Example

Example scenario:
- ppsf_12m = bullish
- sale_price_12m = confirms
- ppsf_6m = bearish
- DOM = rising

Output:
- Action: DEPLOY_LIGHT
- Confidence: MEDIUM
- Timing State: STAGE_ENTRY
- Friction State: HIGH_FRICTION

---

# 10. Model Promotion Rule

A tuned model replaces the incumbent only if it wins on **full evaluation criteria**, including:

- directional accuracy
- regime accuracy
- blowup rate
- calibration stability

Tuning results alone do not determine promotion.

---

# 11. Current Model Winners

| Metric | Horizon | Model |
|------|------|------|
| median_ppsf | 6m | SARIMAX old |
| median_ppsf | 12m | SARIMAX tuned |
| median_sale_price | 12m | SARIMAX tuned |
| median_dom | modifier only | observed state |

---

# 12. System Objective

The forecasting system is designed to support:
- capital deployment timing

Secondarily:
- acquisition timing

The goal is not perfect forecasts but **robust decision signals under uncertainty**.
