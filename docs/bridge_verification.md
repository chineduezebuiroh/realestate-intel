# Bridge Verification — Phase B Invariant Audits

This document defines **strict, invariant audit checks** for Phase B *bridge* runs.
If **any check fails**, the bridge is broken and Phase B is **not freeze-safe**.

This file is intentionally:
- Deterministic
- Minimal
- Non-exploratory
- Safe to re-run months later

---

## Scope

Applies to runs with:
- `model_name = 'sarimax_exog'`
- `model_version = 'v0_bridge_artifact'`
- `run_kind = 'bridge'`

---

## 1. Bridge Run Uniqueness

Exactly **one run** must exist per bridge batch.

```sql
select
  batch_id,
  count(*) as n_runs
from forecast_runs
where run_kind = 'bridge'
group by batch_id
having count(*) != 1;
```

**Invariant:** query returns **zero rows**.

---

## 2. Artifact Hash Integrity

The stored hashes must match the design-matrix audit.

```sql
select
  run_id,
  json_extract(algo_params_json, '$.design_matrix_sha256') as design_matrix_sha256,
  json_extract(algo_params_json, '$.feature_set_sha256') as feature_set_sha256
from forecast_runs
where run_kind = 'bridge';
```

Manually verify these match the values in the corresponding
`design_matrix__*.json` audit file.

**Invariant:** hashes match exactly.

---

## 3. Feature Contract Integrity

The model must record the **ordered feature_ids** used.

```sql
select
  run_id,
  json_array_length(json_extract(algo_params_json, '$.feature_ids')) as n_feature_ids
from forecast_runs
where run_kind = 'bridge';
```

**Invariants:**
- `feature_ids` exists
- Length > 0
- Length equals design matrix column count

---

## 4. Temporal Correctness

Forecast dates must begin **one month after train_end** and span the horizon.

```sql
select
  r.run_id,
  r.train_end,
  min(p.target_date) as first_pred,
  max(p.target_date) as last_pred,
  count(*) as n_preds
from forecast_runs r
join forecast_predictions p using (run_id)
where r.run_kind = 'bridge'
group by r.run_id, r.train_end;
```

**Invariants:**
- `first_pred = train_end + 1 month`
- `n_preds = horizon_max_months`

---

## 5. Determinism Re-run Check

Re-running the bridge with the **same inputs** must produce:

- identical `design_matrix_sha256`
- identical `feature_ids` (order preserved)
- identical forecast date range

If any differ, Phase B is **not deterministic**.

---

## Pass / Fail Rule

Phase B bridge is considered **valid** only if **all sections pass**.

No exceptions.
