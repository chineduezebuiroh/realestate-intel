# Demand chronology attenuation investigation

## Status and scope

This is diagnostic evidence only. It changes no production code, registry,
architecture, recommendation state, or promotion state. The comparison uses
the corrected production as-of alignment path and the seven governed counties
over May 2006–May 2026. County-first results are summarized equally, and pooled
plots use within-county standardization plus complete seven-county coverage. The final
60 months are also inspected separately to distinguish architecture effects
from the unusually persistent recent labor chronology.

## Decision summary

> **Meaningful cyclical variation is first attenuated during MA smoothing.**

Raw-to-MA statistics are now calculated within each county and only then
summarized. Mean MA6 reversal reductions are **49.0% for Labor Force, 57.6% for
Employment, and 44.1% for unemployment** (medians 51.2%, 56.7%, and 43.2%).
Mean MA9 reductions are **59.6%, 66.4%, and 51.8%** (medians 58.5%, 66.7%, and
51.0%). The former 48%/65%/44% and 55%/73%/46% pooled headlines are therefore
not retained. MA9 adds 10.5, 8.9, and 7.7 percentage points of mean reversal
reduction beyond MA6: a consistent but smaller increment than MA6.

The May 2026 endpoint is excluded because coverage falls from seven counties in
**April 2026** to DC and Los Angeles only. Both available counties' Labor Force
and Employment declined from April, so those former upward endpoint spikes
were entirely changing-composition artifacts. The unemployment jump was mostly,
but not entirely, composition: the two counties had a 5.3% April mean versus
4.46% across all seven, then each rose in May (DC to 6.0%, Los Angeles to 5.2%).
No missing source value is filled.

Attenuation is broad rather than driven by a few markets. MA6 reversal-reduction
ranges are 29.3–65.6% (Labor Force), 48.3–76.2% (Employment), and 30.5–62.5%
(unemployment); MA9 ranges are 42.9–76.8%, 55.0–87.1%, and 39.8–67.4%. Every
county attenuates. The diagnosis that directional attenuation first occurs at
MA smoothing still holds, while MA9 does not create a comparable second cliff
beyond MA6.

There is still no evidence of a second meaningful amplitude collapse at feature
weighting or Core Demand aggregation. The A/B/C/D definitions, corrected as-of
alignment, and downstream formulas are unchanged. The only downstream evidence
change is exclusion of incomplete May 2026 from pooled diagnostic plots and
statistics.

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
| Raw → MA6/MA9 | Mean county reversal reductions are 44–58% at MA6 and 52–66% at MA9 | **First meaningful attenuation** |
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

1. Washington, DC raw/MA: [Labor Force](demand_chronology_attenuation_visuals/01_dc_raw_ma_labor_force.svg), [Employment](demand_chronology_attenuation_visuals/01_dc_raw_ma_employment.svg), and [Unemployment](demand_chronology_attenuation_visuals/01_dc_raw_ma_laus_unemployment_rate.svg).
2. Complete-panel standardized seven-county raw/MA: [Labor Force](demand_chronology_attenuation_visuals/01_seven_county_standardized_raw_ma_labor_force.svg), [Employment](demand_chronology_attenuation_visuals/01_seven_county_standardized_raw_ma_employment.svg), and [Unemployment](demand_chronology_attenuation_visuals/01_seven_county_standardized_raw_ma_laus_unemployment_rate.svg).
3. [Feature-score chronology](demand_chronology_attenuation_visuals/02_feature_score.svg), [Cyclical block](demand_chronology_attenuation_visuals/03_cyclical_block.svg), [Structural block](demand_chronology_attenuation_visuals/04_structural_block.svg), [Core Demand](demand_chronology_attenuation_visuals/05_core_demand.svg), and [Demand Axis](demand_chronology_attenuation_visuals/06_demand_axis.svg).
4. [A/B/C/D overlay](demand_chronology_attenuation_visuals/07_abcd_overlay.svg) and [controlled differences](demand_chronology_attenuation_visuals/08_difference_plots.svg).

The [`demand_chronology_attenuation_evidence/`](demand_chronology_attenuation_evidence/)
directory contains the five reproducible CSV evidence exports: county
chronologies, county attenuation statistics, seven-county summary, monthly
coverage, and the complete-panel standardized pooled chronology.

## Confidence and governance conclusion

**Confidence: high** that the May Labor Force and Employment spike was a panel
composition defect and that attenuation first occurs at smoothing; all seven
county-level comparisons support the latter. No production policy, parameter,
scoring rule, registry, selected Demand architecture, or production artifact
changed. This diagnostic does not recommend changing MA9 or any production
parameter.

## Reproduction and governance

Run `scripts/build_demand_chronology_attenuation.py` against an immutable run
containing the source and persisted aligned score artifacts. The builder
validates current Demand membership and axis weights, rebuilds only the four
specified controls, uses the corrected shared as-of aligner through the LAUS
calibration implementation, and writes review-only CSV/SVG output. The committed review CSVs and SVGs are diagnostic evidence, not production
artifacts.
