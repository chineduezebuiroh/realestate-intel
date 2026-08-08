# Affordability Derivation-Order Diagnostic (Phase 4A)

Phase 4A tests derivation order only. Feature weights remain fixed at
50/20/30. No production policy is promoted here.

The diagnostic compares exactly two policies for `price_to_income` and
`payment_burden`:

* **AFF-DERIVATION-A:** full-window `MA12(raw median_sale_price)` is substituted
  at the canonical derivation boundary; preserved forward-filled annual income
  and raw monthly `mortgage_30y` are not smoothed. The resulting level is used
  for lag-3 and lag-12 structural changes.
* **AFF-DERIVATION-B:** the canonical metric is first derived from raw sale
  price, preserved forward-filled annual income, and (for payment burden) raw
  monthly `mortgage_30y`. A full-window MA12 is then applied to that derived
  chronology before lag-3 and lag-12 changes.

Both policies call `build_derived_metrics_with_lineage`; the economic formulas,
source lineage, source precedence, and downstream production configuration are
therefore not reimplemented or changed. Input and formula audits fail closed on
incomplete lineage or reconstruction error greater than `1e-12`. Duplicate
keys, invalid/non-finite required inputs, and interior monthly price gaps also
fail closed. Level rows remain available independently of lag and movement
warmup.

The policies are generally not mathematically equivalent. Mortgage payment is
nonlinear in rate, so deriving with a raw current rate after smoothing price is
not interchangeable with smoothing historical payments. Annual income update
boundaries likewise make `MA12(price) / income` differ from
`MA12(price / income)`.

The diagnostic produces the requested `affordability_derivation_*` CSV
namespace plus `affordability_derivation_order_review.html`. Empty downstream
context tables are explicit when only canonical source observations are
provided; the runner never invents or substitutes a production run. No rank,
composite, recommendation, or automatic winner is emitted. Recommendation and
promotion remain `none`; human decision remains `pending`.

Run locally against the intended immutable Price/Affordability source artifact:

```bash
python scripts/build_affordability_derivation_diagnostic.py \
  --source-metrics /path/to/intended-run/canonical_source_metrics.csv \
  --source-run-id '<immutable-source-run-id>' \
  --output-dir artifacts/regime/comparisons/affordability_derivation_phase4a
```

The output directory must not already exist. Authoritative review requires the
intended frozen source-run identity; a different available run is not a valid
substitute.

## Phase 4A settlement — COMPLETE

The approved human decision selects **`AFF-DERIVATION-B`**. Production now uses
canonical raw inputs → canonical derived metric → full-window MA12 structural
level → lag-3 / lag-12 structural features → normalization → metric score.

* `price_to_income`: raw `median_sale_price` plus canonical forward-filled
  `median_household_income` → canonical derivation → MA12 → lag3 / lag12.
* `payment_burden`: raw `median_sale_price` plus canonical forward-filled
  `median_household_income` plus raw canonical monthly `mortgage_30y` →
  canonical derivation → MA12 → lag3 / lag12. The smoothed Capital Markets
  mortgage feature state is not an Affordability derivation input.

The economic formulas, income forward-fill/source-date lineage, source
precedence, metric and dimension weights, Demand axis weights, and Capital
Markets, Supply, and Demand policies are unchanged. Affordability feature
weights remain level/short/long = **50/20/30**.

The human selected B without an automated winner score. For payment burden, B
improved median absolute MoM (0.013751 → 0.008056), P90 (0.048724 → 0.025198),
P99 (0.115755 → 0.051864), maximum jump (0.332526 → 0.206510), sign flips
(9074 → 5657), persistent turns (1853 → 1105), and recent-36m turns
(628 → 294). For price-to-income it improved median absolute MoM
(0.007126 → 0.005968), P90 (0.024967 → 0.018554), P99
(0.090871 → 0.045648), maximum jump (0.368174 → 0.200572), and sign flips
(7706 → 6876). Its persistent turns increased from 818 to 1142; that explicit
tradeoff was accepted because monthly and tail behavior improved, derive-first
has cleaner economic lineage, and nonlinear payment burden improved without a
turning-point penalty.

Governance identity is
`affordability_derivation_b_derive_first_ma12_2026_08_08` with recommendation
`selected`, promotion `promoted`, human decision `approved`, and calibration
stage `phase4a_closed`. **Derivation order is settled. Feature weights remain
subsequently settled in Phase 4B at the retained 50/20/30 production weights;
overall Affordability calibration is complete.** The
Phase 4A diagnostic retains `district_of_columbia_dc__county` and
`alameda_county_ca__county` as required focus geographies and retains its
eligibility/exclusion audit; it does not establish global production geography
or CBSA policy.

The compact `affordability_derivation_promotion_*` evidence namespace compares
production against diagnostic B for raw derived values, MA12 levels, lag-3, and
lag-12 features at tolerance `1e-12` and fails closed on any mismatch.

Authoritative local validation:

```bash
PYTHONPATH=. python -u scripts/build_affordability_derivation_diagnostic.py \
  --source-metrics artifacts/regime/runs/macro_regime_v1_frozen_supply_20260806/source_metrics.parquet \
  --source-run-id macro_regime_v1_frozen_supply_20260806 \
  --output-dir artifacts/regime/comparisons/affordability_derivation_phase4a_promotion
```
