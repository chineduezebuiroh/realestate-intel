"""Experiment helpers exposed with lazy imports.

The package contains optional diagnostic modules that may require plotting
libraries. Keep package import lightweight so non-visual smoke tests can run in
minimal environments.
"""

__all__ = [
    "PriceFamilyStructuralCandidate",
    "PRICE_FAMILY_STRUCTURAL_CANDIDATES",
    "get_price_family_structural_candidate",
    "build_linked_price_family_features",
    "DEFAULT_SMOOTHING_EXPERIMENT_REGISTRY",
    "SmoothingExperiment",
    "SmoothingMetricPolicy",
    "build_smoothed_metric_features",
    "build_smoothed_metric_features_wide",
    "load_smoothing_experiments",
    "build_active_inventory_comparison",
    "build_metric_normalization_stability_audit",
    "apply_smoothing_experiment",
    "build_inventory_finalist_comparison",
    "build_inventory_chronological_review",
    "write_inventory_chronological_review",
]

_EXPORTS = {
    "build_linked_price_family_features": ("regime.experiments.linked_price_family_features", "build_linked_price_family_features"),
    "get_price_family_structural_candidate": ("regime.experiments.linked_price_family_features", "get_price_family_structural_candidate"),
    "PRICE_FAMILY_STRUCTURAL_CANDIDATES": ("regime.experiments.linked_price_family_features", "PRICE_FAMILY_STRUCTURAL_CANDIDATES"),
    "PriceFamilyStructuralCandidate": ("regime.experiments.linked_price_family_features", "PriceFamilyStructuralCandidate"),
    "build_smoothed_metric_features": ("regime.experiments.smoothing_features", "build_smoothed_metric_features"),
    "build_smoothed_metric_features_wide": ("regime.experiments.smoothing_features", "build_smoothed_metric_features_wide"),
    "DEFAULT_SMOOTHING_EXPERIMENT_REGISTRY": ("regime.experiments.smoothing_policy", "DEFAULT_SMOOTHING_EXPERIMENT_REGISTRY"),
    "SmoothingExperiment": ("regime.experiments.smoothing_policy", "SmoothingExperiment"),
    "SmoothingMetricPolicy": ("regime.experiments.smoothing_policy", "SmoothingMetricPolicy"),
    "load_smoothing_experiments": ("regime.experiments.smoothing_policy", "load_smoothing_experiments"),
    "build_active_inventory_comparison": ("regime.experiments.active_inventory_comparison", "build_active_inventory_comparison"),
    "build_metric_normalization_stability_audit": ("regime.experiments.metric_normalization_stability", "build_metric_normalization_stability_audit"),
    "apply_smoothing_experiment": ("regime.experiments.smoothing_run", "apply_smoothing_experiment"),
    "build_inventory_finalist_comparison": ("regime.experiments.inventory_finalist_comparison", "build_inventory_finalist_comparison"),
    "build_inventory_chronological_review": ("regime.experiments.inventory_chronological_review", "build_inventory_chronological_review"),
    "write_inventory_chronological_review": ("regime.experiments.inventory_chronological_review", "write_inventory_chronological_review"),
}


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = _EXPORTS[name]
    module = __import__(module_name, fromlist=[attr_name])
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
