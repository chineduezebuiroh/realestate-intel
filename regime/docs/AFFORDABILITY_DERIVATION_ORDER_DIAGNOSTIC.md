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
