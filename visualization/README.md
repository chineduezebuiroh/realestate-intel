# Visualization MVP v0.1.1

This presentation layer reads immutable Regime Engine run artifacts and renders
a standalone county macro-regime snapshot in HTML, plus a small product-facing
JSON audit payload. It uses pandas and Plotly, requires no server, and embeds the
Plotly runtime so the page works offline.

Build the Washington, DC snapshot from a locally available published run:

```bash
PYTHONPATH=. python -u scripts/build_regime_visualization_mvp.py \
  --run-dir artifacts/regime/runs/macro_regime_v1_0_release_20260810 \
  --geo-id district_of_columbia_dc__county \
  --market-name "Washington, DC" \
  --output-dir artifacts/product_mvp/v0_1
```

The visualization reads the five published `regime_assignments`, `coordinates`,
`axis_scores`, `dimension_scores`, and `metric_scores` Parquet artifacts plus
the governed axis and metric-dimension registries. It does not query a serving
database or recompute regime scores, coordinates, geometry, or assignments.

The regime plane uses the production cycle-wheel's four major angular sectors,
a fixed equal ±0.60 axis scale, and visual-only 0.25/0.50 radial references.
Signed metric drilldowns apply the production available-weight semantics and
fail closed unless their contributions reconcile the persisted dimension score.
The five-year chronology includes both the existing axis lines and a compact
four-color strip sourced directly from persisted major/minor assignments.
