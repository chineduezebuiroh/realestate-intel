# Structural/Cyclical Demand factorial decision evidence

## Status and scope

This record is a diagnostic decision layer over the governed 66-scenario,
seven-county Structural/Cyclical Demand factorial. It does not select a winner,
change a registry, change turning-point logic, or promote a production policy.
The underlying Level, Short, and Long feature definitions are held fixed.

Governance remains:

```text
recommendation_state = none
promotion_state = none
human_decision = pending
automated_winner = false
production_policy_changed = false
```

## Structural turn-expression closeout

`structural_turn_expression_share` remains in the review exports as historical
lineage evidence, but its applicability is **not applicable** and it is not used
for scenario evaluation, ranking, recommendation, or qualitative preference.
The Structural block is dominated by annual or infrequently updating state
variables. Its monthly representation is consequently plateau-heavy, with
approximately 91–92% of monthly deltas equal to zero. That chronology is
incompatible with the shared turning-point detector's requirement for
contiguous non-zero directional monthly movement. Detector reconstruction
matches the shared helper across all seven counties, and the composite itself
is non-flat, so the zero expression value is neither an export defect nor
evidence that Structural has no information.

This closeout does not replace the measure, invent a responsiveness proxy, or
redesign the detector. The historical value and explicit non-applicability
fields preserve provenance while preventing misuse.

## Decision reading

The diagnostic exports three views of every main effect: a six-county decision
basis, Washington, DC as descriptive-only evidence, and a seven-county
descriptive pool. Conclusions below use the six-county view so DC cannot drive
them. Full-history and recent-36-month results remain separate.

### Labor Force membership

The evidence does not support dropping Labor Force solely for the cancellation
improvement produced by LF-OUT. LF-OUT is the cleaner, smaller signal, while
LF-IN preserves incremental labor-market movement and downstream Demand-axis
responsiveness at the cost of additional within-block cancellation. That
tradeoff persists across counties and is not resolved by either the pooled or
recent-window view. **LF-IN is therefore the evidence-favored retention family,
but LF membership remains a human tradeoff rather than a promoted decision.**

### LAUS feature weighting

The incumbent 25/35/40 policy is clearly weak: its change-heavy allocation
retains the most cancellation/noise without a compensating downstream benefit.
40/30/30 remains on the weak side of the frontier. Moving toward Level reduces
cancellation and reversals consistently; the useful finalist region is
60/20/20 through 80/10/10, with 50/25/25 a defensible transition case rather
than a leading family.

The improvement is not costless. At 80/10/10 the marginal cancellation benefit
over 70/15/15 is small relative to the further attenuation of score movement
and recent responsiveness. This is evidence of a practical knee, not proof
that 80/10/10 is invalid. **60/20/20 and 70/15/15 are the strongest balanced
finalist families; 80/10/10 remains a boundary finalist only if a human accepts
the responsiveness loss.**

### Structural/Cyclical balance

The extremes are disfavored for different reasons. BAL-S25-C75 exposes the
factorial most strongly to Cyclical cancellation and reversal/noise;
BAL-S75-C25 suppresses too much of the monthly labor response and attenuates
the downstream Demand axis. BAL-S35-C65, BAL-S50-C50, and BAL-S65-C35 form the
credible middle frontier. The incumbent-exact control remains useful for
parity and historical context but is not privileged as a finalist merely
because it is incumbent.

Within the middle frontier, Cyclical-heavier balance preserves responsiveness
and magnitude, while Structural-heavier balance improves stability. The
evidence does not define the utility function needed to choose between those
properties, so no balance is recommended or promoted.

### Material interactions and county consistency

The factors are not interchangeable. Higher LAUS Level weight partly tempers
the extra cancellation introduced by LF-IN, making LF retention more credible
in the 60–70% Level region than under 25/35/40. Conversely, combining LF-OUT,
80/10/10, and a Structural-heavy balance compounds attenuation; the separate
noise improvements should not be read as three independent reasons to choose
that corner. Cyclical-heavy balances make the LAUS and LF choices more
consequential, while Structural-heavy balances compress their differences.

The direction of the principal effects is broadly shared across the six
decision counties rather than created by one or two counties. County magnitudes
vary, and DC shows more idiosyncratic recent chronology, which is why it is
retained only in the descriptive view. Recent-36-month evidence strengthens
rather than removes the central tradeoff: the cleanest configurations also
show the greatest attenuation.

## What remains unresolved

No single scalar objective governs the trade among cancellation, magnitude,
reversal/noise, responsiveness, county consistency, and downstream Demand-axis
behavior. A human architecture decision therefore requires an explicit view of
how much responsiveness and magnitude may be exchanged for lower cancellation
and noise. Structural turn expression cannot resolve that trade under its
current non-applicable semantics.

## Roadmap

**Follow-on LAUS smoothing calibration:** After provisional selection of LF
membership, LAUS feature weights, and Structural/Cyclical balance, test whether
the MA windows underlying the LAUS Level/Short/Long features should change, but
only if residual noise, reversal, or responsiveness evidence justifies another
calibration layer.

That follow-on is not part of this factorial and is not implemented here.
