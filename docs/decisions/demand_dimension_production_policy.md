# Demand Dimension Production Policy

## Status and authority

**Status:** Accepted production engineering policy
**Scope:** Core Demand dimension
**Production registry:** `config/metric_dimension_registry.csv`,
`config/feature_registry.csv`, and `config/normalization_registry.csv`

This is the single authoritative synthesis of the completed Labor, GDP, and
ACS Demand investigations. Registries remain the executable authority if this
document and configuration ever diverge. Price, Affordability, Capital Markets,
and Supply are separate dimensions and are not governed here.

## Accepted production behavior

### Canonical subcomponents and sources

| Canonical metric | Nominal weight | Accepted source behavior | Macro applicability | Local applicability |
|---|---:|---|---|---|
| `employment` | 0.1667 | CES total nonfarm first; LAUS employment fallback | CES | LAUS |
| `labor_force` | 0.1667 | LAUS labor force | LAUS | Not enabled |
| `laus_unemployment_rate` | 0.1667 | LAUS unemployment rate | LAUS | LAUS |
| `population` | 0.1667 | ACS1 first; ACS5 fallback | ACS1/ACS5 | ACS5 |
| `median_household_income` | 0.1667 | ACS1 first; ACS5 fallback | ACS1/ACS5 | ACS5 |
| `gdp_annual` | 0.1667 | BEA annual real GDP | BEA annual | BEA annual |

Available canonical subcomponents are equally weighted in intent; `0.1667`
registry values sum to `1.0002` because of decimal representation. Scoring
renormalizes available weights. There is no implicit source substitution:
source priority and fallback eligibility are registry-controlled.

Quarterly GDP, LAUS unemployment count, FRED unemployment, and CES total
private employment remain diagnostic-only or disabled and contribute no
production weight. Quarterly GDP is not silently substituted for annual GDP.

### Feature definitions and weights

The governed LAUS families use Level `0.80`, Short `0.10`, and Long `0.10`; other accepted core-Demand subcomponents retain Level `0.25`, Short `0.35`, and Long `0.40`.
Missing feature components are excluded and the remaining feature weights are
renormalized. Within Demand, an absent metric renormalizes the remaining metric
weights only inside its Structural or Cyclical block; the governed 25/75 block
allocation does not drift. No missing feature value is silently imputed.

| Source/metric family | Level | Short | Long |
|---|---|---|---|
| LAUS employment, labor force, unemployment rate | calendar MA9 (2/3 coverage) | `MA9 / lag3(MA9) - 1` | `MA9 / lag12(MA9) - 1` |
| CES employment | raw level | one-month change | twelve-month change |
| ACS population and income | raw level | annual change | three-year rolling annual change |
| BEA annual GDP | raw level | annual change | three-year rolling annual change |

The MA9 Labor transforms use exact calendar-month windows and the shared 2/3 coverage rule, with no forward-fill or zero-fill. Unemployment-rate features retain negative direction; all other
accepted Demand features retain positive direction.

### Structural/Cyclical production composition

`config/metric_dimension_registry.csv` is the single authoritative governance
surface. Its Demand-only `demand_block` and `block_weight` columns are blank for
non-Demand rows. `metric_weight` controls relative weight within a Demand block,
while `block_weight` controls weight between the blocks.

Core Demand is composed from unchanged Structural membership (population, median household income, and annual GDP) at `0.25` and Cyclical membership (labor force, employment, and LAUS unemployment rate) at `0.75`. Each available block is internally normalized by its unchanged metric weights; available block weights are normalized. Labor Force is explicitly retained.

### Normalization

Production normalization is expanding historical percentile scoring, clipped
to `[0.01, 0.99]` and mapped to the standard `[-1, 1]` score convention.

| Family | Lookback | Minimum periods | Direction |
|---|---:|---:|---|
| LAUS | 120 | 36 | Positive, except unemployment-rate overrides are negative |
| CES | 120 | 36 | Positive |
| ACS | 25 | 8 | Positive |
| BEA annual GDP | 25 | 8 | Positive feature-key overrides |

Normalization is historical within the metric series; no cross-geography
broadcast or normalization fallback is accepted for the core Demand metrics.

### Geography and freshness

The production Macro Regime evaluation calendar supports county and CBSA
geographies. Source availability differs: CES is the macro employment source,
LAUS supplies county/local employment and unemployment behavior, ACS5 provides
broad local coverage, and annual GDP has no CBSA production source.

Native-frequency scores are aligned backward onto month-end evaluation dates.
The original observation date and `metric_age_days` are preserved. Current
production does not silently impute a missing observation and does not apply an
undocumented hard staleness cutoff. Annual ACS income and population used by
derived metrics have separate warning/hard ages of 548/730 days under the
derived-input confidence policy; that policy does not silently alter the core
Demand score.

## Evidence for acceptance

### Labor

The earlier LAUS MA6 challenger passed chronology, volatility, contribution, cancellation, readiness, deterministic-run, lineage, and immutable acceptance checks. The subsequent completed evidence sequence informed an explicit human promotion of LF-IN, MA9, 80/10/10, and S25/C75; it was not an automated winner. The accepted maximum District of
Columbia Demand-axis lag was ten months because Labor is a structural regime
signal rather than a tactical nowcast. The immutable acceptance verified exact
formulas, non-LAUS parity, non-Demand isolation, and explicit coordinate/regime
movement.

### GDP and ACS

Persisted GDP and ACS observations are annual, complete within emitted rows,
and structurally slow-moving. They remain material contributors in the
mandatory county reviews and are not consistently redundant: Alameda showed
moderate reinforcement, while District of Columbia interactions were mostly
weak. Short and Long components retained useful dispersion. Level components
showed substantial upper-percentile saturation, but the completed diagnostic
did not establish that removal or an immediate weight change was superior to
the reproducible incumbent.

### Contribution, interaction, and weighting

The core-Demand diagnostic reconstructed the dimension from effective metric
weights within floating-point tolerance and measured cancellation within its
valid range. Equal nominal metric weights prevent any one of Labor, population,
income, or GDP from becoming the default structural driver, while availability
renormalization preserves computation across uneven geography coverage. The
evidence supports freezing the current engineering policy, not claiming that
the weights are globally optimal.

## Deferred challengers

The following are research challengers, not production recommendations:

1. Detrended, real/per-capita, or growth-relative Level transforms for GDP,
   population, and income to address expanding-percentile saturation.
2. Alternative Level/Short/Long weights evaluated only after a Level-transform
   challenger isolates feature construction from weighting effects.
3. Immutable counterfactual runs for any GDP/ACS feature or weight challenger,
   including coordinate, regime, transition, lineage, and non-target parity.
4. A quarterly-versus-annual GDP source challenger if a deterministic geography
   and fallback contract is designed.

## Explicit non-decisions for Production Calibration

This freeze does **not** decide:

- whether the six equal nominal metric weights are analytically optimal;
- whether slow-moving macro Level features should retain 25% indefinitely;
- CBSA Labor source correctness or a CBSA income/GDP architecture;
- ACS1-versus-ACS5 row-level lineage or revision/vintage policy;
- a hard staleness exclusion or confidence-weighting rule for core Demand;
- national broadcast behavior for GDP, ACS, or Labor;
- confidence scoring, forecast-regime behavior, or local-regime expansion;
- Price/Affordability, Capital Markets, Inventory, Supply, or axis calibration.

Those questions require isolated Production Calibration challengers and must
not be inferred as accepted changes from the diagnostic evidence.

## Change control

Future changes require registry updates, an immutable baseline/challenger
comparison, explicit lineage and artifact verification, relevant non-target
parity checks, and a superseding production decision. Diagnostic review exports
remain non-authoritative and should not be committed as production artifacts.
