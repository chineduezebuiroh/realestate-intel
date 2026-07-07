from __future__ import annotations
# scripts/smoke_test_feature_engine.py

from regime.feature_engine import build_feature_matrix


def main() -> int:
    features = build_feature_matrix()

    print("[feature_engine] rows:", len(features))
    print("[feature_engine] geos:", features["geo_id"].nunique())
    print("[feature_engine] feature_keys:", features["feature_key"].nunique())
    print("[feature_engine] date range:", features["date"].min(), "→", features["date"].max())

    print("[feature_engine] sample:")
    print(features.head(20).to_string(index=False))

    print("[feature_engine] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
