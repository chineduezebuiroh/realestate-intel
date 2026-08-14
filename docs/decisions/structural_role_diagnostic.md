# Structural role architecture diagnostic

## Scope and governance

This diagnostic compares exactly three cases: **A**, S5/C95 blended inside the
65% Demand dimension; **B**, 100% Labor numeric Demand with Structural excluded;
and **C**, the same numeric chronology as B while retaining governed Structural
history as a zero-weight, non-scoring `Market Context` surface. B and C must be
numerically identical. LF-IN, calendar MA9, B3 (40/15/45), Price 17.5%,
Affordability 7.5%, Capital Markets 10%, and the 65% Demand budget are fixed.
There is no weight grid and no production registry change.

Governance is `recommendation_state = none`,
`promotion_state = current_production_unchanged`,
`human_decision = structural_role_review_pending`, `automated_winner = false`,
and `production_policy_changed = false`.

## Mathematical parity

For Structural share `s`, Labor score `L`, Structural score `S`, and unchanged
other-axis contribution `O`, linearity gives:

`0.65 × ((1-s)L + sS) + O = 0.65(1-s)L + 0.65sS + O`.

At S5 this is `0.6175L + 0.0325S + O`. Merely moving matched effective weights
up one hierarchy level is therefore not a new numeric architecture. Exact
parity also holds at an availability boundary only when both constructions use
the same observation mask and equivalent renormalization. It can break if one
level reallocates a missing component while the other preserves its missing or
zero allocation, if other dimensions join on a different availability mask,
or if normalization is performed over different component populations.

## Evidence and interpretation status

The implementation reuses the authoritative Structural/Cyclical balance
chronology builder, MA9+B3 reconstruction, shared reversal/whipsaw and turning
point helpers, governed metric and axis weights, production missing-feature
renormalization, persisted Structural scores, and existing Demand-axis
contribution machinery. It exports county, period, cancellation, turn,
Demand-axis, context, overlap, parity, governance, and review evidence without
writing production state.

Authoritative run artifacts are unavailable in the hosted environment. The
builder therefore failed closed before creating a review directory. No
empirical answer is asserted for stability, latency, cancellation, county or
period robustness, Demand-axis propagation, context usefulness, information
overlap, or whether A, B, or C is the more coherent role. Those questions remain
for human review after an authoritative execution; amplitude reduction alone
must not be counted as value and no composite or automated winner is produced.

Visualization should carry A-versus-Labor numeric overlays, the incremental S5
effect, responsiveness evidence, and the final Demand-axis comparison. If C is
retained, Structural must be shown separately with source/vintage freshness and
an explicit 0% scoring label so that it cannot be mistaken for a contribution.

## CBSA caveat and unresolved risks

The county conclusion must not automatically generalize to CBSA GDP. **Reassess
GDP independently during CBSA calibration because higher-frequency quarterly
GDP may contain usable cyclical information not present in county-level annual
GDP.** That experiment is out of scope here.

Remaining risks are authoritative artifact availability, source-date column
coverage on the persisted Structural surface, annual-series common-frequency
interpretation, and human judgment about contextual product usefulness. No
production policy was changed or promoted.
