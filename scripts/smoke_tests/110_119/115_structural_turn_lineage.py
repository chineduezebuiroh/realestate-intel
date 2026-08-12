"""Fast contract smoke for the diagnostic-only Structural turn lineage trace."""
from pathlib import Path
import tempfile

import numpy as np
import pandas as pd

from regime.experiments.demand_signal_attenuation import RUN_ID, GEOS, STRUCTURAL, WEIGHT_POLICIES
from regime.experiments.structural_turn_lineage import SCENARIO_ID, STAGES, build_review, build_tables
from regime.pandas_compat import MONTH_END

assert RUN_ID == "macro_regime_v1_0_1_candidate_20260810"
assert GEOS == (
    "district_of_columbia_dc__county", "essex_county_nj__county",
    "montgomery_county_md__county", "prince_george_s_county_md__county",
    "fairfax_county_va__county", "san_francisco_county_ca__county",
    "los_angeles_county_ca__county",
)
assert STRUCTURAL == ("population", "median_household_income", "gdp_annual")
assert SCENARIO_ID == "LF-IN__LAUS-W-70-15-15__BAL-INCUMBENT-EXACT"
assert "LAUS-W-80-10-10" in WEIGHT_POLICIES

dates = pd.date_range("2019-01-31", periods=30, freq=MONTH_END)
wave = np.array((list(range(8)) + list(range(8, -1, -1))) * 2, dtype=float)[:30]
score_rows, labor_rows = [], []
for offset, geo in enumerate(GEOS):
    for i, date in enumerate(dates):
        for j, metric in enumerate(STRUCTURAL):
            score_rows.append({"geo_id": geo, "date": date, "metric": metric,
                               "score": wave[i] + offset/10 + j/20})
        for metric in ("labor_force", "employment", "laus_unemployment_rate"):
            labor_rows.append({"geo_id": geo, "date": date, "metric": metric,
                               "score": wave[i] + offset/10})
scores = pd.DataFrame(score_rows + labor_rows)
labor = pd.DataFrame(labor_rows)
base = pd.Series({"population": .15, "median_household_income": .15, "gdp_annual": .20,
                  "labor_force": .15, "employment": .15, "laus_unemployment_rate": .20})
first = build_tables(scores, labor, base)
second = build_tables(scores, labor, base)
chron = first["structural_turn_lineage_structural_chronology"]
assert chron.equals(second["structural_turn_lineage_structural_chronology"])
assert not chron.duplicated(["geo_id", "date"]).any()
# Structural contributions retain their governed share of the complete parent;
# they are not renormalized to 100% after the block is selected.
expected_first = wave[0] * .5 + (.15 * .05 + .20 * .10)
assert np.isclose(chron.iloc[0].structural_score, expected_first)
assert len(first["structural_turn_lineage_detector_input"]) == len(GEOS) * len(dates)
assert {"geo_id", "turn_date", "turning_point_type", "qualified"}.issubset(first["structural_turn_lineage_detected_turns"])
source = Path("regime/experiments/structural_turn_lineage.py").read_text()
assert "from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points" in source
expression = first["structural_turn_lineage_expression_reconstruction"]
for row in expression.itertuples():
    expected = row.matched_same_direction_turns / row.structural_qualified_turns if row.structural_qualified_turns else 0.0
    assert np.isclose(row.expression_share, expected)
comparison = first["structural_turn_lineage_export_comparison"]
assert comparison.status.eq("match").all()
root = first["structural_turn_lineage_root_cause"]
allowed = {"NO_STRUCTURAL_SCORE_VARIATION", "NO_STRUCTURAL_TURNS_DETECTED", "STRUCTURAL_TURNS_NOT_QUALIFIED",
           "MATCHER_INPUT_EMPTY", "MATCHER_SEMANTICS_FAILURE", "MATCH_DIRECTION_FAILURE",
           "EXPRESSION_FORMULA_FAILURE", "EXPORT_WIRING_FAILURE", "NO_FAILURE_REPRODUCED", "OTHER"}
assert root.root_cause.isin(allowed).all() and not root.production_policy_changed.any()
assert set(first["structural_turn_lineage_stage_summary"].stage_name) == set(STAGES)
for artifact in ("before_vs_after_stage_counts", "repaired_lineage_summary",
                 "repaired_expression_share", "repaired_match_audit",
                 "repaired_export_comparison"):
    assert artifact in first
assert first["repaired_export_comparison"].status.eq("match").all()
assert "config/" not in source.split("to_csv")[-1]
assert "automated_winner" not in source and '"Decision": "selected"' not in source

# Missing authoritative input fails before any output is written. Normal smoke
# never invokes either this authoritative build or the 66-scenario builder.
with tempfile.TemporaryDirectory() as tmp:
    out = Path(tmp) / "out"
    try:
        build_review(Path(tmp) / RUN_ID, out, Path.cwd())
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("lineage diagnostic must fail closed")
    assert not out.exists()

print("Structural turn lineage smoke test passed")
