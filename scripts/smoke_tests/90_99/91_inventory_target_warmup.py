"""Smoke Test 91: chronology-aware Inventory target replacement."""
from __future__ import annotations

import pandas as pd

from regime.pandas_compat import MONTH_END

from regime.experiments.in_memory_challenger import _target_replacement_reconciliation


KEYS = ("inventory_level", "inventory_short", "inventory_long")
GEOS = ("alpha__county", "beta__county")
DATES = pd.date_range("2024-01-31", periods=6, freq=MONTH_END)


def rows(starts: dict[tuple[str, str], int] | None = None) -> pd.DataFrame:
    starts = starts or {}
    return pd.DataFrame([
        {"geo_id": geo, "date": date, "canonical_metric_key": "active_inventory",
         "feature_key": feature}
        for geo in GEOS for feature in KEYS
        for date in DATES[starts.get((geo, feature), 0):]
    ])


def reconcile(candidate: pd.DataFrame) -> pd.DataFrame:
    return _target_replacement_reconciliation(
        rows(), candidate, experiment_id="inventory_ma_warmup",
        target_feature_keys=KEYS, campaign_geo_ids=GEOS,
    )


def must_fail(candidate: pd.DataFrame, reason: str) -> None:
    try:
        reconcile(candidate)
    except ValueError as exc:
        assert f"reason={reason}" in str(exc), str(exc)
        assert "candidate=inventory_ma_warmup" in str(exc)
        assert "sample=" in str(exc)
    else:
        raise AssertionError(f"Expected {reason}")


def main() -> int:
    # MA3/MA6-style starts vary independently by geography and component.
    starts = {
        ("alpha__county", "inventory_level"): 2,
        ("alpha__county", "inventory_short"): 5,
        ("alpha__county", "inventory_long"): 3,
        ("beta__county", "inventory_level"): 1,
        ("beta__county", "inventory_short"): 2,
        ("beta__county", "inventory_long"): 4,
    }
    candidate = rows(starts)
    assert len(candidate) < len(rows())  # exact-key equality would reject this.
    evidence = reconcile(candidate)
    assert evidence["warmup_reconciliation_pass"].all()
    assert set(evidence["failure_reason"]) == {"leading_warmup_valid"}
    assert evidence.set_index(["geo_id", "feature_key"]).loc[
        ("alpha__county", "inventory_short"), "leading_warmup_rows"
    ] == 5

    candidate_only_row = candidate.iloc[[0]].copy(deep=True)
    candidate_only_row["date"] = pd.Series(
        [pd.Timestamp("2024-07-31")],
        index=candidate_only_row.index,
        dtype="datetime64[ns]",
    )
    candidate_only = pd.concat(
        [
            candidate.copy(deep=True),
            candidate_only_row,
        ],
        ignore_index=True,
    )
    must_fail(candidate_only, "candidate_only_target_keys")
    must_fail(candidate[candidate.feature_key.ne("inventory_long")], "missing_target_series")

    interior = candidate.copy()
    mask = (interior.geo_id.eq("alpha__county") & interior.feature_key.eq("inventory_level")
            & interior.date.eq(DATES[3]))
    must_fail(interior[~mask], "interior_target_gap")

    trailing = candidate.copy()
    mask = (trailing.geo_id.eq("alpha__county") & trailing.feature_key.eq("inventory_level")
            & trailing.date.eq(DATES[-1]))
    must_fail(trailing[~mask], "trailing_target_gap")
    must_fail(pd.concat([candidate, candidate.iloc[[0]]], ignore_index=True), "duplicate_target_keys")
    outside = pd.concat([candidate, candidate.iloc[[0]].assign(geo_id="nation")], ignore_index=True)
    must_fail(outside, "out_of_scope_target_rows")

    # The reconciler only accepts available challenger rows; no incumbent
    # warmup key is synthesized or returned for mixed-universe backfill.
    assert sum(evidence.leading_warmup_rows) == len(rows()) - len(candidate)
    print("SMOKE TEST 91 — INVENTORY TARGET WARMUP: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
