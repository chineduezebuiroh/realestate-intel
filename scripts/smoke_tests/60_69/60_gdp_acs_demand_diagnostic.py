from regime.experiments.gdp_acs_demand_diagnostic import build_gdp_acs_demand_diagnostic


def main() -> int:
    result = build_gdp_acs_demand_diagnostic()
    for name in ("source_coverage", "time_series_structure", "feature_policy", "feature_behavior", "contribution_summary", "interaction_correlations"):
        if result[name].empty:
            raise AssertionError(f"{name} is empty")
    if set(result["source_coverage"]["canonical_metric_key"]) != {"gdp_annual", "median_household_income", "population"}:
        raise AssertionError("GDP/ACS source coverage is incomplete")
    if len(result["feature_policy"]) != 9:
        raise AssertionError("Expected Level, Short, and Long policy rows for all three metrics")
    if not bool(result["feature_behavior"][["lower_clip_rate", "upper_clip_rate", "near_zero_score_rate"]].stack().dropna().between(0, 1).all()):
        raise AssertionError("Feature behavior rates fall outside [0, 1]")
    if not result["contribution_summary"]["mean_absolute_demand_axis_contribution"].ge(0).all():
        raise AssertionError("Demand-axis contribution magnitude must be non-negative")
    print("[gdp_acs_demand_diagnostic] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
