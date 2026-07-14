from __future__ import annotations
# scripts/smoke_tests/00_09/00_regime_config.py

from regime._00_config_loader import build_registry_resolution, load_regime_config


def main() -> int:
    config = load_regime_config(validate=True)
    resolved = build_registry_resolution(config)

    print("[regime:config] source_metrics:", len(config.source_metrics))
    print("[regime:config] features:", len(config.features))
    print("[regime:config] metric_dimensions:", len(config.metric_dimensions))
    print("[regime:config] axes:", len(config.axes))
    print("[regime:config] resolved rows:", len(resolved))

    print("[regime:config] resolved by axis/dimension:")
    print(
        resolved
        .groupby(["axis", "dimension"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["axis", "dimension"])
        .to_string(index=False)
    )

    print("[regime:config] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
