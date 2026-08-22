"""Smoke 162: governed Redfin percentage domains and real header normalization."""
from __future__ import annotations

import copy
import json
import tempfile
from pathlib import Path

import pandas as pd

from sources.redfin.governance import GovernanceError, load_metric_domain_contract
from sources.redfin.validate import normalize_columns, validate_metric_domains


def rejects(function) -> None:
    try:
        function()
    except GovernanceError:
        return
    raise AssertionError("expected governed failure")


contract = load_metric_domain_contract()
assert normalize_columns([
    "MEDIAN SALE PRICE NSA ($)", "AVERAGE SALE TO LIST RATIO (%)",
    "SHARE SOLD ABOVE ORIGINAL LIST (%)", "MEDIAN SALE PRICE PER SQ.FT. ($)",
    "PERCENT OFF MARKET IN TWO WEEKS (%)",
]) == [
    "median_sale_price_nsa", "average_sale_to_list_ratio",
    "share_sold_above_original_list", "median_sale_price_per_sqft",
    "percent_off_market_in_two_weeks",
]

with tempfile.TemporaryDirectory() as directory:
    path = Path(directory) / "domains.json"
    for mutation in (
        lambda value: value["metrics"].pop("average_sale_to_list_ratio"),
        lambda value: value.update(contract_version=2),
        lambda value: value["metrics"]["share_sold_above_original_list"].update(unit="fraction"),
        lambda value: value["metrics"]["average_sale_to_list_ratio"].update(source_min=201, source_max=200),
        lambda value: value["metrics"]["percent_off_market_in_two_weeks"].update(expected_min=-9),
    ):
        invalid = copy.deepcopy(contract); mutation(invalid); path.write_text(json.dumps(invalid))
        rejects(lambda: load_metric_domain_contract(path))

accepted = pd.DataFrame({
    "share_sold_above_original_list": [50, 100, 100.45, 103.47, -1.73, 105, -2],
    "percent_off_market_in_two_weeks": [50, 0, -3.79, -7.57, -8, 100, 50],
    "average_sale_to_list_ratio": [100, 50, 150, 175, 200, 100, 100],
})
summary = validate_metric_domains(accepted, contract)
assert all(result["status"] in {"normal", "warning"} for result in summary.values())
assert summary["share_sold_above_original_list"]["status"] == "warning"
assert summary["percent_off_market_in_two_weeks"]["status"] == "warning"
assert summary["average_sale_to_list_ratio"]["status"] == "warning"

for metric, value in (
    ("share_sold_above_original_list", 105.01), ("share_sold_above_original_list", -2.01),
    ("percent_off_market_in_two_weeks", -8.01), ("percent_off_market_in_two_weeks", 100.01),
    ("average_sale_to_list_ratio", 49.99), ("average_sale_to_list_ratio", 200.01),
):
    rejects(lambda metric=metric, value=value: validate_metric_domains(pd.DataFrame({metric: [value]}), contract))

print("redfin metric domains smoke: ok")
