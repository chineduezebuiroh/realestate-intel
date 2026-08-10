# Regime Engine v1.0 production freeze

**Release identity:** `regime_engine_v1_0`

**Candidate tag (created by a human only after validation on `main`):** `regime-engine-v1.0.0`

**Status:** candidate for merge to `main`

## Scope and authority

**Regime Engine v1.0 = historical/current county-level macro regime engine.**
It is the first durable benchmark for the deterministic pipeline from canonical
county observations through features, normalization, metric and dimension
scores, axes, coordinates, geometry, and persisted major/minor regime
assignments. Executable registries are authoritative; this document records
their settled release contract rather than overriding them.

It does **not** mean a CBSA macro regime, ZIP/local regime, forecast regime,
production UI/dashboard, confidence framework, or market
comparison/ranking engine.

## Production architecture inventory

### Geography, pipeline, and sources

The release target is U.S. county macro history/current state. National series
(Capital Markets) are aligned to county evaluation dates; county-capable source
series retain their canonical geography and lineage. Month-end evaluation uses
backward/as-of alignment: lower-frequency observations are carried forward until
the next observation, while observation date and age remain explicit.

The canonical pipeline is: source registry and observations → canonical source
resolution/derived metrics → feature engineering → expanding historical
percentile normalization → metric scoring → backward month-end alignment →
dimension scoring → axis scoring → coordinates → cycle-wheel geometry →
major/minor regime assignment → immutable artifacts and manifest.

Current source precedence is registry-controlled and has no implicit
substitution:

* Price, Inventory, transactions, and liquidity use Redfin direct series.
* Population and household income prefer ACS1 and fall back to ACS5 for missing
  geography/history.
* Employment prefers CES total nonfarm where applicable and falls back to LAUS;
  county Labor also uses LAUS labor force and unemployment rate. Annual real GDP
  uses BEA annual GDP; quarterly GDP is diagnostic, not a silent substitute.
* Permit activity uses Census BPS total units. `permit_intensity` is a distinct
  canonical derived metric (BPS total units divided by population), not a second
  raw BPS activity field.
* Capital Markets uses canonical monthly FRED mortgage, Treasury, Fed Funds, and
  explicitly constructed `10y - 2y` and `10y - fedfunds` spreads.

### Dimensions and axes

Active metric membership and configured weights are:

| Dimension | Canonical metric membership (configured metric weight) |
|---|---|
| Price | median sale price (0.50), median PPSF (0.50) |
| Transaction Activity | homes sold (0.50), pending sales (0.50) |
| Liquidity | days on market (0.3333), months supply (0.3333), sale-to-list ratio (0.3334) |
| Demand | population, household income, annual GDP, labor force, employment, LAUS unemployment rate (0.1667 each; available sources/metrics renormalize) |
| Supply | active inventory (0.60), permit activity (0.20), permit intensity (0.20) |
| Capital Markets | mortgage 30y (0.15), mortgage 15y (0.15), Treasury 10y (0.15), Fed Funds (0.10), 10y–2y spread (0.225), 10y–Fed Funds spread (0.225) |
| Affordability | price-to-income (0.50), payment burden (0.50) |

The exact axes are Demand = Demand 0.65 + Price 0.175 + Affordability 0.075 +
Capital Markets 0.10, and Supply = Supply 0.85 + Capital Markets 0.15. Missing
available components are handled by the existing scorer's explicit weight
renormalization, never silent value imputation.

### Active production feature contract

Each row below represents the level / short / long feature triplet. The cells
give `transform (window; feature weight)` and therefore compactly enumerate all
active production features resolved from the current registries.

| Metric / source family | Level | Short | Long | Dimension |
|---|---|---|---|---|
| median sale price; median PPSF | `ma_level` (12m; .50) | `ma_pct_change` (12m/lag3m; .25) | `ma_pct_change` (12m/lag12m; .25) | Price |
| active inventory | `ma_level` (12m; .50) | `ma_pct_change` (12m/lag3m; .25) | `ma_pct_change` (12m/lag12m; .25) | Supply |
| homes sold; pending sales | `level_zscore` (raw; .25) | `mom_zscore` (1m; .35) | `yoy_zscore` (12m; .40) | Transaction Activity |
| days on market; months supply; sale-to-list | `level_zscore` (raw; .25) | `mom_zscore` (1m; .35) | `yoy_zscore` (12m; .40) | Liquidity |
| ACS population; ACS household income; BEA annual GDP | `level_zscore` (raw; .25) | `yoy_zscore` (1y; .35) | `rolling_yoy_zscore` (3y; .40) | Demand |
| LAUS labor force; employment; unemployment rate | `ma_level` (6m; .25) | `ma_pct_change` (6m/lag3m; .35) | `ma_pct_change` (6m/lag12m; .40) | Demand |
| CES total nonfarm employment | `level_zscore` (raw; .25) | `mom_zscore` (1m; .35) | `yoy_zscore` (12m; .40) | Demand |
| BPS permit activity | `ma_level` (12m; .80) | `ma_pct_change` (12m/lag6m; .10) | `ma_pct_change` (12m/lag12m; .10) | Supply |
| permit intensity | `ma_level` (12m; .50) | `ma_pct_change` (12m/lag3m; .25) | `ma_pct_change` (12m/lag12m; .25) | Supply |
| mortgage 30y; mortgage 15y; Treasury 10y | `ma_level` (12m; .60) | `ma_pct_change` (12m/lag3m; .20) | `ma_pct_change` (12m/lag12m; .20) | Capital Markets |
| Fed Funds | `ma_level` (3m; .60) | `ma_pct_change` (3m/lag3m; .20) | `ma_pct_change` (3m/lag12m; .20) | Capital Markets |
| 10y–2y; 10y–Fed Funds spreads | `ma_level` (9m; .60) | `ma_difference` (9m/lag3m; .20) | `ma_difference` (9m/lag12m; .20) | Capital Markets |
| price-to-income; payment burden | `ma_level` (12m; .50) | `ma_pct_change` (12m/lag3m; .20) | `ma_pct_change` (12m/lag12m; .30) | Affordability |

Thus Inventory and the direct Price family use the settled full-window MA12
50/25/25 policy; Labor uses its governed full-window LAUS MA6 policy; Capital
Markets is `MW-TEMPERED-C` with its mixed windows/transforms and 60/20/20
features; Affordability is derive-first MA12/lag3/lag12 at 50/20/30; and the
final BPS policy is ratio-based MA12 level, lag6 short, lag12 long at 80/10/10,
with Supply metric weight 0.20.

## Derived-metric boundary

* **Price-to-income:** raw median sale price + canonical forward-filled household
  income → canonical derivation → full-window MA12 → lag3 / lag12 features.
* **Payment burden:** raw median sale price + canonical forward-filled household
  income + raw canonical monthly `mortgage_30y` → canonical derivation →
  full-window MA12 → lag3 / lag12 features.

Capital Markets' smoothed mortgage feature state does **not** cross into the
Affordability derivation boundary. Likewise, `permit_intensity` derives the raw
permits/population ratio first and then builds its MA12 family; raw BPS
`permit_activity` independently uses the final BPS feature policy above.

## Governance

The governing architectural records are [feature-transform governance](../adr/ADR-001-feature-transform-governance.md),
[macro regime architecture](../adr/ADR-002-macro-regime-architecture.md),
[deterministic artifact pipeline](../adr/ADR-003-artifact-first-deterministic-pipeline.md),
[canonical observations and lineage](../adr/ADR-004-canonical-observation-and-lineage.md),
and [production validation/challenger governance](../adr/ADR-005-production-validation-and-challenger-governance.md).

Settled production decisions are the [Price family and linked MA12 decision](../../regime/docs/PRICE_AFFORDABILITY_PRODUCTION_DECISION.md),
[Labor/Demand production policy](../decisions/demand_dimension_production_policy.md),
[Inventory/Supply weight and freeze decision](../decisions/supply_dimension_weight_promotion_and_freeze.md),
[Capital Markets decision](../decisions/capital_markets_production_policy.md),
[Affordability decision](../decisions/affordability_production_policy.md), and
[final BPS decision](../decisions/bps_production_policy.md). The executable
release inventory is `config/regime_engine_v1_0_release.json`.

## Reproducibility and release acceptance

The calibration evidence input is
`artifacts/regime/runs/macro_regime_v1_frozen_supply_20260806/source_metrics.parquet`.
It is the frozen source artifact against which promoted policy evidence is
interpreted; it is **not** a requirement that future routine production
refreshes reuse old observations. Routine refreshes use newly governed serving
inputs while preserving registry, lineage, batch identity, and data-as-of
identity.

`scripts/run_regime_pipeline.py` invokes the deterministic runner. A unique run
ID is immutable and existing IDs are rejected. The run manifest records run and
experiment identity, status, metadata, pipeline version, validation geographies,
MA policy snapshot, per-config SHA-256 hashes, and every artifact's integrity
metadata. The artifact store writes each stage and validation output, and
`verify_run` checks file hashes against the manifest. Reproduction therefore
requires the same source artifact/input database, config contents (matching
hashes), code revision, arguments, and run metadata; output verification uses
the persisted manifest rather than mutable ambient state.

Run the focused v1.0 acceptance suite from the repository root:

```bash
PYTHONPATH=. python -u scripts/smoke_tests/90_99/106_regime_engine_v1_freeze.py
PYTHONPATH=. python -u scripts/smoke_tests/90_99/105_bps_finalist_incremental_value.py
PYTHONPATH=. python -u scripts/smoke_tests/90_99/104_pandas_monthly_frequency_compatibility.py
PYTHONPATH=. python -u scripts/smoke_tests/90_99/97_settled_ma12_feature_policy_promotion.py
PYTHONPATH=. python -u scripts/smoke_tests/90_99/95_capital_markets_ma_decomposition.py
PYTHONPATH=. python -u scripts/smoke_tests/50_59/56_affordability_derivation_order.py
PYTHONPATH=. python -u scripts/smoke_tests/50_59/57_affordability_feature_weights.py
PYTHONPATH=. python -u scripts/smoke_tests/00_09/00_regime_config.py
PYTHONPATH=. python -u scripts/smoke_tests/10_19/15_artifact_io.py
# Requires a locally persisted run; substitute its immutable run ID:
PYTHONPATH=. python -u scripts/smoke_tests/10_19/16_pipeline_runner.py --run-id <run-id>
python -m compileall -q regime scripts
git diff --check
```

The tests collectively freeze production identity, BPS, Affordability, Capital
Markets, registry validation, pandas monthly-frequency compatibility, and the
deterministic artifact/manifest contract. Future challengers must compare an
immutable candidate against this v1.0 registry policy and the frozen calibration
source on common dates/geographies, demonstrate target effects and non-target
parity, preserve lineage, and pass the same acceptance contracts. No diagnostic
score or experiment name is itself production policy.

## Known caveats

* The final BPS decision retains a mixed recent-36-month volatility field in
  which 80/10/10 is somewhat worse; the human settlement accepted that caveat
  against the broader stability and structural-turn evidence.
* Geography and history are availability-governed. Sparse county history causes
  full-window warmup and available-component renormalization; it must not be
  silently filled. Coverage outside the governed county baseline is not implied.
* Annual ACS and BEA observations are carried backward-as-of onto monthly
  evaluation dates (effectively forward through evaluation time), with source
  observation date/age retained. Derived income freshness has governed warning
  and hard-age semantics; carry-forward is not a claim of monthly source updates.

## Not part of v1.0

* CBSA macro regime; ZIP/local regime; forecast regime generation.
* Confidence framework and visualization/product UI.
* Remaining data-refresh and freshness hardening.
* Deferred structural-metric transform research.
* Market balance, profile, comparison, or ranking engines.
* Diagnostic-module cleanup that is engineering debt rather than a release blocker.

The next stage after release is the **lightweight Visualization MVP**.

## Human merge and tag sequence

Hosted Codex must not merge or tag. After review, a human performs:

```bash
# after this freeze pass is merged to phase8c-inventory-calibration

git checkout main
git pull

# merge via GitHub PR preferred:
# phase8c-inventory-calibration -> main

git pull

# run v1.0 acceptance suite on main

git tag -a regime-engine-v1.0.0 -m "Regime Engine v1.0 county macro baseline"
git push origin regime-engine-v1.0.0
```
