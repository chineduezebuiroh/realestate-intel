from __future__ import annotations
# scripts/smoke_tests/14_permit_pipeline_trace.py

from regime.validation import trace_permit_feature_pipeline


def main() -> int:
    for geo_id in [
        "district_of_columbia_dc__county",
        "alameda_county_ca__county",
    ]:
        trace = trace_permit_feature_pipeline(
            geo_id=geo_id,
            start_date="2025-09-01",
            end_date="2026-05-31",
        )

        print(f"\n[permit_pipeline_trace] geo: {geo_id}")

        print("\n[permit_pipeline_trace] metric scores:")
        print(
            trace["metric_scores"][
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "metric_score",
                    "previous_metric_score",
                    "delta_metric_score",
                    "feature_count",
                    "feature_weight_sum",
                    "min_feature_score",
                    "max_feature_score",
                ]
            ].to_string(index=False)
        )

        print("\n[permit_pipeline_trace] joined raw + normalized trace:")
        print(trace["joined_trace"].to_string(index=False))

    print("\n[permit_pipeline_trace] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
