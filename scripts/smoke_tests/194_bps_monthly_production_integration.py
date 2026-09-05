"""Offline acceptance for BPS use of the governed input-pin/candidate lifecycle."""
from __future__ import annotations

import json
import tempfile
import zipfile
from pathlib import Path

import pandas as pd

from jobs.monthly_refresh.bps_monthly import compiled_candidate, provisional_candidate, validate_compiled_coverage
from jobs.monthly_refresh.source_inputs import (add_pin, pinned_or_discover,
                                                provider_pin)
from sources.census_bps.artifact import load_registry
from sources.census_bps.artifact import ADAPTER_CONTRACT_VERSION
from sources.census_bps_provisional.ingest import PROVISIONAL_COLUMNS_BY_LEVEL


def _compiled(root: Path) -> Path:
    rows = []
    for item in load_registry():
        location, identifier = item["provider_location_type"], item["provider_identifier"]
        rows.append({"period": "Monthly", "year": "1988" if location == "Country" else "2026",
                     "month": "1", "location_type": location,
                     "state_fips": identifier if location == "State" else "",
                     "county_fips": identifier if location == "County" else "",
                     "cbsa_code": identifier if location == "Metro" else "", "total_units": "0"})
    rows.append(dict(rows[0]))  # deterministic identical duplicate
    csv = pd.DataFrame(rows).to_csv(index=False).encode()
    path = root / "BPS_Compiled_File_202604.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("New_Master_python_m2604.csv", csv)
    return path


def _provisional(root: Path) -> dict[str, Path]:
    by_level = {"state": [], "county": [], "cbsa_metro": []}
    for item in load_registry():
        if item["provider_location_type"] == "Country":
            continue
        level = {"State": "state", "County": "county", "Metro": "cbsa_metro"}[item["provider_location_type"]]
        row = {column: "0" for column in PROVISIONAL_COLUMNS_BY_LEVEL[level]}
        row.update({"survey_date": "202607", "geo_name": item["geo_id"]})
        if level == "state": row["state_fips"] = item["provider_identifier"]
        elif level == "county":
            row["state_fips"] = item["provider_identifier"][:2]
            row["county_fips_3"] = item["provider_identifier"][2:]
        else: row["cbsa_code"] = item["provider_identifier"]
        by_level[level].append(row)
    cbsa = {column: "0" for column in PROVISIONAL_COLUMNS_BY_LEVEL["cbsa_metro"]}
    cbsa.update({"survey_date": "202607", "cbsa_code": "99999", "geo_name": "outside"})
    by_level["cbsa_metro"].append(cbsa)
    paths = {}
    for level, rows in by_level.items():
        path = root / {"state":"st2607c.txt", "county":"co2607c.txt", "cbsa_metro":"cbsa2607c.txt"}[level]
        with path.open("w") as stream:
            stream.write("heading\nheading\nheading\n")
            pd.DataFrame(rows, columns=PROVISIONAL_COLUMNS_BY_LEVEL[level]).to_csv(stream, index=False, header=False)
        paths[level] = path
    return paths


def _pin(cycle: str, source: str, release: str, paths: dict[str, Path]) -> dict:
    import hashlib
    return provider_pin(cycle_id=cycle, source_id=source, provider_release_id=release,
        members={name: {"url": f"https://example.invalid/{path.name}", "retrieved_at": "2026-09-02T00:00:00Z",
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for name, path in paths.items()})


def main() -> None:
    with tempfile.TemporaryDirectory() as value:
        root = Path(value); cycle = "monthly_cycle__fixture"
        compiled_paths = {"compiled_zip": _compiled(root)}; provisional_paths = _provisional(root)
        compiled_pin = _pin(cycle, "census_bps", "202604", compiled_paths)
        provisional_pin = _pin(cycle, "census_bps_provisional", "2607", provisional_paths)
        assert compiled_pin["provider_release_id"] != provisional_pin["provider_release_id"]
        calls = []
        normal, discovered = pinned_or_discover(mode="normal", existing=None,
            discover_and_retrieve=lambda: calls.append("discover") or compiled_pin,
            cycle_id=cycle, source_id="census_bps", required_members={"compiled_zip"})
        assert discovered and calls == ["discover"]
        for mode in ("resume", "replay"):
            reused, discovered = pinned_or_discover(mode=mode, existing=normal,
                discover_and_retrieve=lambda: (_ for _ in ()).throw(AssertionError("rediscovered")),
                cycle_id=cycle, source_id="census_bps", required_members={"compiled_zip"})
            assert reused == normal and not discovered
        assert add_pin(normal, normal)[1] is False
        compiled = compiled_candidate(pin=compiled_pin, paths=compiled_paths,
            output=root/"compiled-artifact", cycle_id=cycle, repository_root=Path("."))
        provisional = provisional_candidate(pin=provisional_pin, paths=provisional_paths,
            output=root/"provisional-artifact", cycle_id=cycle, repository_root=Path("."))
        compiled_r2 = compiled_candidate(pin=compiled_pin, paths=compiled_paths,
            output=root/"compiled-artifact-r2", cycle_id=cycle, repository_root=Path("."),
            revision=2, prior_artifact_id="src__census_bps__2026-01__r1__parent",
            prior_artifact_sha256="a"*64, republication_id="source_republication__compiled__fixture",
            source_contract_version=ADAPTER_CONTRACT_VERSION)
        provisional_r2 = provisional_candidate(pin=provisional_pin, paths=provisional_paths,
            output=root/"provisional-artifact-r2", cycle_id=cycle, repository_root=Path("."),
            revision=2, prior_artifact_id="src__census_bps_provisional__2026-07__r1__parent",
            prior_artifact_sha256="b"*64, republication_id="source_republication__provisional__fixture",
            source_contract_version=ADAPTER_CONTRACT_VERSION)
        assert compiled["manifest"]["geography_count"] == 221
        assert compiled["manifest"]["observation_min"] == "1988-01-01"
        assert provisional["manifest"]["geography_count"] == 220
        assert provisional["manifest"]["row_count"] == 220
        for extension in ("source_contract_version", "republication_id", "supersedes_artifact_id"):
            assert extension not in compiled["manifest"] and extension not in provisional["manifest"]
        assert provisional["evidence"]["out_of_governance"]["classification"] == "OUT_OF_GOVERNANCE"
        assert compiled["manifest"]["artifact_id"] != provisional["manifest"]["artifact_id"]
        assert "__r2__" in compiled_r2["manifest"]["artifact_id"]
        assert "__r2__" in provisional_r2["manifest"]["artifact_id"]
        assert compiled_r2["manifest"]["supersedes_artifact_id"] == compiled_r2["manifest"]["prior_artifact_id"]
        assert provisional_r2["manifest"]["supersedes_artifact_id"] == provisional_r2["manifest"]["prior_artifact_id"]
        assert compiled_r2["manifest"]["provider_release_id"] == "bps-compiled:202604"
        assert provisional_r2["manifest"]["provider_release_id"] == "bps-provisional:2607"
        compiled_data = pd.read_parquet(root/"compiled-artifact-r2"/"data.parquet")
        provisional_data = pd.read_parquet(root/"provisional-artifact-r2"/"data.parquet")
        expected_cbsa = {item["geo_id"] for item in load_registry() if item["level"] == "cbsa_metro"}
        assert len(expected_cbsa) == 53
        assert set(compiled_data.loc[compiled_data.geo_id.isin(expected_cbsa), "geo_id"]) == expected_cbsa
        assert set(provisional_data.loc[provisional_data.geo_id.isin(expected_cbsa), "geo_id"]) == expected_cbsa
        assert "united_states__nation" not in set(provisional_data.geo_id)
        assert not any("09999" in value for value in compiled_data.geo_id)

        # The real promoted contract contains 53 exact-code CBSAs.  A compiled
        # historical snapshot may contain only a provider-observed subset; this
        # must remain explicit evidence rather than becoming synthetic history
        # or a false all-configured completeness failure.
        registry = load_registry()
        promoted = [item for item in registry if item["level"] == "cbsa_metro"]
        absent_promoted = {item["geo_id"] for item in promoted[:11]}
        variable_rows = []
        for item in registry:
            if item["geo_id"] in absent_promoted:
                continue
            location, identifier = item["provider_location_type"], item["provider_identifier"]
            variable_rows.append({"period": "Monthly", "year": "2026", "month": "4",
                "location_type": location,
                "state_fips": identifier if location == "State" else "",
                "county_fips": identifier if location == "County" else "",
                "cbsa_code": identifier if location == "Metro" else "", "total_units": "1"})
        from jobs.monthly_refresh.bps_bootstrap import verify
        _, variable_coverage, variable_diagnostics, _ = verify(
            pd.DataFrame(variable_rows), release_month="2026-04")
        validate_compiled_coverage(variable_coverage)
        assert variable_diagnostics["configured_geography_count"] == 221
        assert variable_diagnostics["present_geography_count"] == 210
        assert variable_diagnostics["missing_configured_geography_count"] == 11
        assert {item["geo_id"] for item in variable_diagnostics["missing_configured_geographies"]} == absent_promoted
        assert {item["provider_geography_type"] for item in
                variable_diagnostics["missing_configured_geographies"]} == {"Metro"}
        broken = variable_coverage.copy()
        broken.loc[broken.geo_id.eq("united_states__nation"), "present_in_release"] = False
        try: validate_compiled_coverage(broken)
        except ValueError as exc: assert "united_states__nation" in str(exc)
        else: raise AssertionError("missing compiled nation passed stable-level coverage")
        print(json.dumps({"compiled": compiled["manifest"]["artifact_id"],
                          "provisional": provisional["manifest"]["artifact_id"]}, sort_keys=True))


if __name__ == "__main__": main()
