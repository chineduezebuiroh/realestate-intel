# Affordability production policy

**Decision:** human-approved and promoted
**Selected policy:** `MA12/P4`
**Production feature weights:** 35/20/45
**Governance state:** calibration closed
**Date:** 2026-08-16

The production policy applies exactly to `price_to_income` and `payment_burden`. Both use Level `MA12(raw derived metric)` at 0.35, Short `MA12 / lag3(MA12) - 1` at 0.20, and Long `MA12 / lag12(MA12) - 1` at 0.45.

The derive-first contract is unchanged. Price-to-income is derived from raw median sale price and canonical forward-filled household income. Payment burden additionally uses canonical raw monthly `mortgage_30y`. Mortgage rates are not smoothed before derivation, and Capital Markets structural mortgage state cannot cross into Affordability.

The former `AFF-FW-A` 50/20/30 policy is superseded while its history remains auditable. P3 and P4 were the credible finalists; P5 tested the aggressive Long boundary without justifying expansion; and MA9's incremental responsiveness did not offset its stability and whipsaw costs. MA12/P4 is the durable shared policy and the search is closed. See ADR-009 and `config/affordability_policy_promotion_2026_08_16.json`.

Only material source/metric architecture changes, out-of-sample failure, county-invalidating CBSA evidence, or structural frequency changes may reopen the decision. Marginal in-sample differences are insufficient. Metric weights, dimension/axis weights, normalization, Price, Labor, Supply, and Capital Markets are unchanged.
