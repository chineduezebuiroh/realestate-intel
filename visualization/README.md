# Visualization MVP v0.1

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

The visualization only reads the four published Parquet artifacts and the
governed axis registry; it does not query a serving database or recompute regime
scores and assignments.
