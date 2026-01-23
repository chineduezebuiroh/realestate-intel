# Forecast Architecture Rules (Phase C)

## Entrypoints
Only modules under forecast/cli/ are allowed as execution entrypoints.
Legacy runners remain temporarily for compatibility but must not be used directly.

## Determinism
No Phase C change may alter:
- artifact naming
- feature_id ordering
- as-of resolution behavior
- hashes in audit sidecars
