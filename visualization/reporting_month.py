"""Reporting policy over an exact, independently accepted Source Set manifest."""

from datetime import date

from core.source_artifacts.source_set_v2 import validate_source_set_v2


def redfin_reporting_month(source_set: dict, *, accepted_source_set_id: str) -> str:
    """Resolve observation month; caller supplies the selected cycle's accepted ID.

    Acceptance is deliberately not inferred from the mutable current catalog or
    from a Macro run name. This helper does not select or execute a Macro run.
    """
    validate_source_set_v2(source_set)
    if source_set["source_set_id"] != accepted_source_set_id:
        raise ValueError("Source Set does not match the selected accepted Source Set")
    entries = [row for row in source_set["sources"] if row["source_id"] == "redfin"]
    if len(entries) != 1:
        raise ValueError("Reporting policy requires exactly one accepted Redfin entry")
    try:
        observation = date.fromisoformat(entries[0]["observation_max"])
    except (TypeError, ValueError) as exc:
        raise ValueError("Accepted Redfin observation_max must be an ISO observation date") from exc
    return observation.strftime("%Y-%m")
