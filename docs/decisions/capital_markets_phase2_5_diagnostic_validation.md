# Capital Markets Phase 2.5 diagnostic validation

## Scope and governance

This change hardens the diagnostic implementation only. The closed P0–P9 grid,
settled transforms, MA windows, metric weights, and Demand/Supply axis weights
are unchanged. No winner is selected and Capital Markets metric-weight
calibration remains `not_started`.

## Long-weight boundary extension

P8 and P9 were added because P7 confounded Long-majority weighting with a large
reduction in Level. P8 (45/10/45) closes the Level/Long parity boundary, P9
(40/10/50) closes the moderate Long-majority boundary, and P7 (35/10/55)
remains the aggressive Long-majority stress. The final controlled Long ladder,
with Short fixed at 10%, is:

```text
P4  55/10/35
 -> P5  50/10/40
 -> P8  45/10/45
 -> P9  40/10/50
 -> P7  35/10/55
```

The grid is closed at exactly P0–P9. No production feature-weight policy has
yet been selected by this diagnostic extension.

## Cycle-reference finding

The prior `raw_value.diff()` reference is a valid description of an oriented
one-month raw move, but it is not a valid like-for-like reference for the
candidate score. The candidate is a weighted composite of normalized MA level,
lag-3 short, and lag-12 long features. In particular, its level component makes
comparison with a raw first difference a level-versus-change comparison. This
caused weak correlations and also sent two analytically different chronologies
through the same turning-point detector.

Phase 2.5 therefore uses the governed P0 normalized-feature composite as the
**incumbent chronology reference**. It is the smallest reference that preserves the
exact family-specific production semantics: MA12 ratios for long-term rates,
MA3 ratios for Fed Funds, and MA9 arithmetic differences for spreads, including
production normalization, polarity, availability renormalization, and the
60/20/20 governed mix. The old oriented one-month raw difference and its
correlation remain exported as legacy evidence. It remains useful only for the
separate material-raw-move responsiveness diagnostic. P0 is not an independent
economic-cycle truth set: its self-correlation is necessarily one, and divergence
from P0 may be desirable when absolute and marginal evidence demonstrates better
signal behavior.

## Turning points and delay

The former zero preservation/undefined delays followed from comparing turns in
the one-month raw-difference series with turns in the normalized composite,
rather than from changed candidate chronology. Phase 2.5 detects and matches
turns on comparable P0/candidate composite chronologies, retains the governed
one-month/type-consistent match rule, and exports qualified, rejected, matched,
and unmatched rows. Delay is emitted only for matches and carries an explicit
status for no reference turns, no candidate turns, or no matches.

## Performance design

The pre-change authoritative runtime was approximately 45 minutes 46 seconds.
The principal avoidable cost was repeated turning-point detection in downstream
county dimension and axis loops, even though those turns do not add native
feature-weight decision evidence. Phase 2.5 confines turning analysis to the
national native comparison, caches unchanged Demand/Supply baseline statistics,
precomputes axis panels, and retains vector arithmetic for propagation. County
stability and materiality evidence remains present; candidate scores,
contributions, dimension propagation, and axis propagation are unchanged.

The authoritative run is intentionally not fabricated when its ignored input
artifact is absent. Its post-change runtime and empirical old/new correlations
must come from the local authoritative rerun and are persisted in the
performance and comparison exports.

## Decision exports

The decision table is keyed by metric, period, and policy and combines identity,
stability, responsiveness, contribution structure, feature similarity, and
secondary incumbent-chronology distance evidence. Policy selection is governed
primarily by absolute and marginal stability, responsiveness, and contribution
evidence; incumbent similarity is a tradeoff diagnostic, not an objective.
The marginal table contains only the nine controlled comparisons (P0→P1,
P1→P2, P2→P6, P1→P3, P2→P4, P4→P5, P5→P8, P8→P9, and P9→P7) and uses
arithmetically exact deltas. Because no governed diagnostic thresholds exist,
`marginal_improvement_status` is `human_review_required`. Family plateau status
is likewise `indeterminate`; no composite score, rank, recommendation, or
automatic winner is produced. Plateau review considers marginal stability gains,
responsiveness costs, contribution shifts, and improvement flattening; it does
not automatically reward maximum incumbent correlation.
