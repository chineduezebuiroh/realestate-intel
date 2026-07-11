from __future__ import annotations
# scripts/smoke_tests/19_selected_policy_historical_review.py

from regime.historical_review import build_historical_review


def main() -> int:
    review = build_historical_review(
        run_id="macro_regime_v1",
    )

    print("[historical_review] period summary:")
    print(review["period_summary"].to_string(index=False))

    print("\n[historical_review] annual summary:")
    print(review["annual_summary"].to_string(index=False))

    print("\n[historical_review] major regime distribution:")
    print(review["major_distribution"].to_string(index=False))

    print("\n[historical_review] transition audit:")
    print(review["transition_audit"].to_string(index=False))

    if review["period_summary"].empty:
        raise AssertionError("Historical period summary is empty")

    if review["annual_summary"].empty:
        raise AssertionError("Annual historical summary is empty")

    print("\n[historical_review] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
