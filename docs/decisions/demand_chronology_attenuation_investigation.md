# Demand chronology attenuation investigation

## Status and scope

This is diagnostic evidence only. It changes no production code, registry,
architecture, recommendation state, or promotion state. The comparison uses
the corrected production as-of alignment path and the seven governed counties
over May 2006–May 2026. Results are pooled equally across counties. The final
60 months are also inspected separately to distinguish architecture effects
from the unusually persistent recent labor chronology.

## Decision summary

> **Meaningful cyclical variation is first attenuated during MA smoothing.**

The clearest effect is directional rather than amplitude attenuation. Relative
to raw LAUS, MA6 reduces reversal counts by **48% for Labor Force (134 to 70),
65% for Employment (132 to 46), and 44% for unemployment (98 to 55)**. MA9
reduces them by **55%, 73%, and 46%**, respectively. Correlation with the raw
series remains high at MA6 (0.997, 0.988, and 0.916) and MA9 (0.996, 0.982, and
0.869), so smoothing removes short changes without erasing the broad raw
chronology.

There is **no evidence of a second meaningful amplitude collapse** at feature
weighting or Core Demand aggregation. Moving from 70/15/15 to 80/10/10 raises,
rather than reduces, full-history Cyclical standard deviation by 4.7% under
MA6 (0.269 to 0.282) and 4.1% under MA9 (0.267 to 0.278). Core Demand standard
deviation similarly rises 3.9% (MA6) and 2.9% (MA9). Demand-axis aggregation
retains 77–79% of Core Demand standard deviation, but adds reversals rather
than producing monotonicity; it therefore compresses magnitude modestly but is
not where cyclicality first disappears.

The recent visual impression is principally a **window-selection effect**.
Across the final 60 months, standard deviation is only 63–67% of its full-history
level at Core Demand and 42–43% at the Demand Axis for all four controls. The
same recent-history compression under A, B, C, and D, combined with the absence
of a controlled architecture cliff, indicates an unusually persistent recent
labor-market trend rather than a newly introduced scoring defect.

## Controlled comparison

| Control | MA | LAUS Level/Short/Long | Cyclical SD | Core SD | Demand-axis SD | Core reversals | Core persistence |
|---|---:|---:|---:|---:|---:|---:|---:|
| A | 6 | 70/15/15 | 0.269 | 0.298 | 0.229 | 76 | 0.620 |
| B | 9 | 70/15/15 | 0.267 | 0.307 | 0.240 | 59 | 0.701 |
| C | 6 | 80/10/10 | 0.282 | 0.310 | 0.235 | 76 | 0.620 |
| D | 9 | 80/10/10 | 0.278 | 0.316 | 0.244 | 57 | 0.711 |

The controlled marginal effects are:

- **A → B (MA effect):** Cyclical SD −0.7%, Core SD +2.8%, Core reversals
  −22%, and persistence +0.081.
- **A → C (weight effect):** Cyclical SD +4.7%, Core SD +3.9%, no change in
  Core reversals or persistence.
- **B → D (weight effect):** Cyclical SD +4.1%, Core SD +2.9%, Core reversals
  −3%, and persistence +0.010.
- **C → D (MA effect):** Cyclical SD −1.3%, Core SD +1.8%, Core reversals
  −25%, and persistence +0.091.

Thus MA9 materially reduces direction changes but does not reduce aggregate
score dispersion. The weight change also does not attenuate dispersion. Its
main measured effects are lower cancellation and slightly higher persistence.

## Stage attribution

| Stage | Quantitative reading | Attribution |
|---|---|---|
| Raw → MA6/MA9 | Reversals fall 44–73%; raw correlation remains 0.869–0.997 | **First meaningful attenuation** |
| MA → feature score | Feature SD remains 0.304–0.512; weight shift increases it | No amplitude cliff |
| Feature → Cyclical block | SD remains 0.267–0.282; 52–76 reversals | Aggregation preserves movement |
| Cyclical + Structural → Core Demand | SD rises to 0.298–0.316; cancellation falls to 0.148–0.179 | No attenuation |
| Core Demand → Demand Axis | SD falls 21–23%, while reversals rise to 99–103 | Modest magnitude compression, not monotonicity |

Cancellation declines monotonically across the controlled aggregation path:
Cyclical cancellation is 0.224–0.283, Core cancellation is 0.148–0.179, and
Demand-axis cancellation is 0.114–0.134. This is constructive netting, not
evidence that the axis eliminates the surviving chronology. Turning-point
counts are detector-sensitive because Structural inputs are plateau-heavy;
reversal count, persistence, dispersion, range, zero crossings, cancellation,
and raw correlation are therefore reported alongside turns rather than using
turns alone.

## Supporting visuals

1. Raw LAUS versus MA3/MA6/MA9: [Labor Force](demand_chronology_attenuation_visuals/01_raw_ma_labor_force.svg), [Employment](demand_chronology_attenuation_visuals/01_raw_ma_employment.svg), and [LAUS Unemployment](demand_chronology_attenuation_visuals/01_raw_ma_laus_unemployment_rate.svg).
2. [Feature-score chronology](demand_chronology_attenuation_visuals/02_feature_score.svg).
3. [Cyclical-block chronology](demand_chronology_attenuation_visuals/03_cyclical_block.svg).
4. [Structural-block chronology](demand_chronology_attenuation_visuals/04_structural_block.svg).
5. [Core Demand chronology](demand_chronology_attenuation_visuals/05_core_demand.svg).
6. [Demand Axis chronology](demand_chronology_attenuation_visuals/06_demand_axis.svg).
7. [A/B/C/D overlay](demand_chronology_attenuation_visuals/07_abcd_overlay.svg).
8. [A−B, A−C, B−D, and C−D differences](demand_chronology_attenuation_visuals/08_difference_plots.svg).

## Confidence and next step

**Confidence: medium-high.** Confidence is high that smoothing is the first
attenuation stage because the result is large, consistent across all three
LAUS measures, and preserved under both weight controls. Confidence is medium
on interpreting the most recent five years as a durable economic property:
that conclusion is descriptive, covers only one recent window, and is pooled
across seven counties.

No production change is supported. The recommended next step is only to retain
full-history context beside five-year views in future review visuals. If the
question is reopened, repeat this same fixed A/B/C/D diagnostic when another
12–24 months of observations are available; do not start a new optimization
campaign from the present evidence.

## Reproduction and governance

Run `scripts/build_demand_chronology_attenuation.py` against an immutable run
containing the source and persisted aligned score artifacts. The builder
validates current Demand membership and axis weights, rebuilds only the four
specified controls, uses the corrected shared as-of aligner through the LAUS
calibration implementation, and writes review-only CSV/SVG output. Generated
review output is not a production artifact and is not committed except for the
supporting SVGs linked above.
