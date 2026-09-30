"""Smoke 176: pure governed CES request/acquisition/canonical adapter."""
from __future__ import annotations

import random
from copy import deepcopy
from pathlib import Path

import pandas as pd
import requests

from sources.bls_ces.artifact import (
    BLS_API_ENDPOINT,
    GOVERNED_METRICS,
    MAX_SERIES_PER_REQUEST,
    TransientCESAcquisitionError,
    UNIT,
    acquire,
    build_request_plan,
    canonicalize,
    load_series_spec,
)


def fixture_rows(count: int = 52):
    rows = []
    for index in range(count):
        rows.append({"geo_id": f"geo-{index:02d}", "series_id": f"SMS{index:017d}",
                     "metric_base": "ces_total_nonfarm", "seasonal": "S"})
    rows.extend([
        {"geo_id": "geo-00", "series_id": "SMS90000000000000001",
         "metric_base": "ces_total_private", "seasonal": "S"},
        {"geo_id": "geo-01", "series_id": "SMS90000000000000002",
         "metric_base": "ces_construction", "seasonal": "S"},
        # Explicit legacy exclusions.
        {"geo_id": "geo-00", "series_id": "SMU90000000000000003",
         "metric_base": "ces_total_nonfarm", "seasonal": "U"},
        {"geo_id": "geo-00", "series_id": "SMS90000000000000004",
         "metric_base": "ces_manufacturing", "seasonal": "S"},
    ])
    return rows


hashes = {"fixture": "a" * 64}
rows = fixture_rows()
plan = build_request_plan(rows, start_year=1960, end_year=2024,
                          acquisition_mode="deep_reconciliation", config_hashes=hashes)
shuffled = deepcopy(rows)
random.Random(7).shuffle(shuffled)
same = build_request_plan(shuffled, start_year=1960, end_year=2024,
                          acquisition_mode="deep_reconciliation", config_hashes=hashes)
assert plan == same
assert set(item["metric_id"] for item in plan["series"]) == set(GOVERNED_METRICS)
assert len(plan["series"]) == 54
assert all(len(request["series_ids"]) <= MAX_SERIES_PER_REQUEST for request in plan["requests"])
assert all(request["end_year"] - request["start_year"] + 1 <= 20 for request in plan["requests"])
assert plan["year_windows"] == [{"start_year": 1960, "end_year": 1979},
                                {"start_year": 1980, "end_year": 1999},
                                {"start_year": 2000, "end_year": 2019},
                                {"start_year": 2020, "end_year": 2024}]
assert plan["annualaverage"] is False and "secret" not in str(plan).lower()
try:
    build_request_plan(rows, start_year=2020, end_year=2024,
                       acquisition_mode="deep_reconciliation", config_hashes=hashes)
except ValueError as exc:
    assert "1960" in str(exc)
else:
    raise AssertionError("shallow CES deep reconciliation was accepted")

# The actual tracked registry freezes exactly three physical metrics, with total
# nonfarm mandatory across all configured CES geographies and asymmetric sparse
# optional availability.
actual = build_request_plan(load_series_spec(), start_year=2022, end_year=2024,
                            acquisition_mode="ordinary_overlap", config_hashes=hashes)
assert set(item["metric_id"] for item in actual["series"]) == set(GOVERNED_METRICS)
assert sum(item["mandatory_for_target"] for item in actual["series"]) == 50
assert len(actual["series"]) == 59


class Response:
    def __init__(self, body=None, status=200):
        self.body = body
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            error = requests.HTTPError(f"HTTP {self.status_code}")
            error.response = self
            raise error

    def json(self):
        return self.body


def body(series):
    return {"status": "REQUEST_SUCCEEDED", "message": [], "Results": {"series": series}}


def block(sid, observations=None):
    return {"seriesID": sid, "data": observations or [
        {"year": "2024", "period": "M01", "value": "100.5"},
        {"year": "2024", "period": "M02", "value": "101.5"},
        {"year": "2024", "period": "M13", "value": "999"},
    ]}


# Small plan proves transformation, asymmetric optional membership, ordering,
# M13 exclusion, target semantics, and exact unit without network access.
small_rows = [
    {"geo_id": "geo-a", "series_id": "SMS00000000000000001",
     "metric_base": "ces_total_nonfarm", "seasonal": "S"},
    {"geo_id": "geo-b", "series_id": "SMS00000000000000002",
     "metric_base": "ces_total_nonfarm", "seasonal": "S"},
    {"geo_id": "geo-a", "series_id": "SMS00000000000000003",
     "metric_base": "ces_total_private", "seasonal": "S"},
]
small = build_request_plan(small_rows, start_year=2022, end_year=2024,
                           acquisition_mode="ordinary_overlap", config_hashes=hashes)
request = small["requests"][0]
blocks = [
    block("SMS00000000000000003", [{"year": "2023", "period": "M12", "value": "80"}]),
    block("SMS00000000000000002", [{"year": "2024", "period": "M01", "value": "90"}]),
    block("SMS00000000000000001"),
]
acquired = [{"request": request, "response": body(blocks)}]
frame, diagnostics = canonicalize(small, acquired)
reverse_frame, reverse_diagnostics = canonicalize(
    small, [{"request": request, "response": body(list(reversed(blocks)))}])
pd.testing.assert_frame_equal(frame, reverse_frame)
assert diagnostics == reverse_diagnostics
assert list(frame.columns) == ["geo_id", "metric_id", "date", "property_type_id",
                               "value", "source_id", "property_type"]
assert set(frame["metric_id"]) == {"ces_total_nonfarm_sa", "ces_total_private_sa"}
assert set(frame["date"].map(str)) == {"2023-12-31", "2024-01-31", "2024-02-29"}
assert 999 not in set(frame["value"])
assert diagnostics["unit"] == UNIT == "thousands_of_jobs"
assert diagnostics["mandatory_series_common_observation_max"] == "2024-01-31"
assert diagnostics["target_month"] == "2024-01"
optional = diagnostics["optional_series_lag"][0]
assert optional["lag_months"] == 1
assert diagnostics["missing_mandatory_series"] == []

# A configured optional series may be absent without inventing Cartesian
# membership, while a mandatory omission is diagnosed.
omitted = [{"request": request, "response": body([blocks[0], blocks[1]])}]
_, omitted_diagnostics = canonicalize(small, omitted)
assert omitted_diagnostics["missing_mandatory_series"] == ["SMS00000000000000001"]
assert omitted_diagnostics["mandatory_series_common_observation_max"] is None
assert omitted_diagnostics["missing_requested_series"] == ["SMS00000000000000001"]


def rejected(acquired_value, message=None):
    try:
        canonicalize(small, acquired_value)
    except ValueError as exc:
        if message:
            assert message in str(exc)
    else:
        raise AssertionError("invalid CES provider truth was accepted")


rejected([{"request": request, "response": body(blocks + [block("SMS999")])}], "unexpected")
rejected([{"request": request, "response": body([blocks[0], blocks[1], blocks[1]])}], "duplicate provider")
for bad_observation in (
    {"year": "2024", "period": "Q01", "value": "1"},
    {"year": "2024", "period": "M01", "value": None},
    {"year": "2024", "period": "M01", "value": "nan"},
    {"year": "2024", "period": "M01", "value": "inf"},
):
    bad = [block("SMS00000000000000001", [bad_observation]), blocks[1], blocks[0]]
    rejected([{"request": request, "response": body(bad)}])
duplicate_observation = [{"year": "2024", "period": "M01", "value": "1"},
                         {"year": "2024", "period": "M01", "value": "2"}]
rejected([{"request": request, "response": body([
    block("SMS00000000000000001", duplicate_observation), blocks[1], blocks[0]])}], "duplicate CES")

# Retry behavior is per request and deterministic.  The API key reaches only the
# transport payload, never the semantic plan.
attempts = []
def http_then_success(url, *, json, timeout):
    assert url == BLS_API_ENDPOINT and json["registrationkey"] == "secret"
    attempts.append(1)
    return Response(body([]), 502) if len(attempts) == 1 else Response(body(blocks))

got = acquire(small, api_key="secret", transport=http_then_success,
              backoff_seconds=(0, 0), sleep=lambda _: None)
assert len(got) == 1 and len(attempts) == 2

attempts = []
def timeout_then_success(url, *, json, timeout):
    attempts.append(1)
    if len(attempts) == 1:
        raise requests.Timeout("fixture")
    return Response(body(blocks))
acquire(small, api_key="secret", transport=timeout_then_success,
        backoff_seconds=(0, 0), sleep=lambda _: None)
assert len(attempts) == 2

attempts = []
def exhausted(url, *, json, timeout):
    attempts.append(1)
    raise requests.ConnectionError("fixture")
try:
    acquire(small, api_key="secret", transport=exhausted,
            backoff_seconds=(0, 0), sleep=lambda _: None)
except TransientCESAcquisitionError:
    assert len(attempts) == 3
else:
    raise AssertionError("exhausted CES transport succeeded")

for response in (Response({}, 400), Response({"bad": "schema"}, 200)):
    attempts = []
    def deterministic(url, *, json, timeout, response=response):
        attempts.append(1)
        return response
    try:
        acquire(small, api_key="secret", transport=deterministic,
                backoff_seconds=(0, 0), sleep=lambda _: None)
    except (requests.HTTPError, ValueError):
        assert len(attempts) == 1
    else:
        raise AssertionError("deterministic CES failure was retried/succeeded")

# A later failed batch raises and returns no partial acquisition value.
multi = build_request_plan(fixture_rows(), start_year=2022, end_year=2024,
                           acquisition_mode="ordinary_overlap", config_hashes=hashes)
attempts = []
def second_batch_fails(url, *, json, timeout):
    attempts.append(tuple(json["seriesid"]))
    if len(attempts) >= 2:
        return Response({}, 400)
    return Response(body([block(sid) for sid in json["seriesid"]]))
try:
    acquire(multi, api_key="secret", transport=second_batch_fails,
            backoff_seconds=(0, 0), sleep=lambda _: None)
except requests.HTTPError:
    assert len(attempts) == 2
else:
    raise AssertionError("partial CES batch acquisition was returned")

print("[smoke] governed CES adapter passed")
