"""Strict, offline-testable historical provider observations and omission receipts.

Receipts attest to exact observed responses, not to eligibility thresholds,
whole-vintage gaps, crosswalks, or independently approved historical governance.
"""
from __future__ import annotations

import math
import json
import time
import signal
import threading
from email.utils import parsedate_to_datetime
from datetime import timezone
from typing import Any, Mapping

from core.source_artifacts.hashing import sha256_json
from jobs.monthly_refresh.acs_monthly import API, _safe_url
from sources.census_acs.artifact import (CONTRACT_VERSION, PRODUCTS, SENTINELS,
                                       VARIABLES, request_params)

OMISSION_VERSION = "acs_historical_omission_evidence_v1"


class VerificationRequired(ValueError):
    """Provider evidence is insufficient for historical execution."""



ACQUISITION_DEADLINE_SECONDS = 900
MAX_REQUEST_ATTEMPTS = 3
RETRYABLE_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504})
MAX_RETRY_DELAY_SECONDS = 10


class _AcquisitionDeadlineExpired(BaseException):
    """Interrupt even a slow/trickling in-flight body read on the hosted main thread."""


class HistoricalAcquisitionFailure(VerificationRequired):
    """Only trusted, fixed-vocabulary request context may cross log boundaries."""
    def __init__(self, *, source_id, vintage, request_ordinal, request_kind,
                 geography_id, category, http_status, attempts_consumed):
        self.diagnostic = {"source_id": source_id, "product": PRODUCTS[source_id],
            "vintage": vintage, "request_ordinal": request_ordinal,
            "request_kind": request_kind, "geography_id": geography_id,
            "category": category, "http_status": http_status,
            "attempts_consumed": attempts_consumed}
        super().__init__("historical acquisition failed: " + json.dumps(self.diagnostic, sort_keys=True))


def _retry_delay(attempt, retry_after, wall_clock):
    delay = float(2 ** (attempt - 1))
    if isinstance(retry_after, str) and len(retry_after) <= 128:
        try:
            seconds = float(retry_after)
            if not math.isfinite(seconds):
                raise ValueError()
        except ValueError:
            try:
                date = parsedate_to_datetime(retry_after)
                if date.tzinfo is None:
                    date = date.replace(tzinfo=timezone.utc)
                seconds = date.timestamp() - wall_clock()
            except (ValueError, TypeError, OverflowError):
                seconds = 0
        delay = max(delay, min(MAX_RETRY_DELAY_SECONDS, max(0, seconds)))
    return min(delay, MAX_RETRY_DELAY_SECONDS)


def exact_dataset(source_id: str, vintage: int, discovery: Mapping[str, Any]) -> dict:
    """Bind provider identifiers as opaque facts to an exact advertised dataset.

    An identifier need not equal its API endpoint. Opaque identifiers require
    independent exact accessURL evidence; no identifier substring or ID naming
    convention can establish ordinary product/vintage identity.
    """
    from urllib.parse import urlsplit
    product = PRODUCTS[source_id]
    matches = [d for d in discovery.get("dataset", [])
               if str(d.get("c_vintage")) == str(vintage)
               and d.get("c_dataset") == product.split("/")]
    if len(matches) != 1:
        raise VerificationRequired("exact ordinary product/vintage dataset must resolve once")
    dataset = matches[0]
    endpoints = {f"http://api.census.gov/data/{vintage}/{product}", f"{API}/{vintage}/{product}"}
    identifier = dataset.get("identifier", "")
    if not isinstance(identifier, str) or not identifier:
        raise VerificationRequired("exact provider dataset identifier unavailable")
    if identifier not in endpoints:
        uri = urlsplit(identifier)
        access = [d.get("accessURL") for d in dataset.get("distribution", [])
                  if d.get("accessURL") in endpoints]
        if (uri.scheme not in {"http", "https"} or uri.netloc != "api.census.gov"
                or not uri.path or uri.query or uri.fragment or len(access) != 1):
            raise VerificationRequired("opaque dataset identifier lacks exact official accessURL binding")
    for field, suffix in (("c_variablesLink", "/variables.json"), ("c_geographyLink", "/geography.json")):
        if field in dataset and dataset[field] not in {endpoint + suffix for endpoint in endpoints}:
            raise VerificationRequired("historical dataset metadata links contradict requested product/vintage")
    return dict(dataset)


def validate_metadata(metadata: Mapping[str, Any], *, source_id: str, vintage: int) -> dict:
    # Captured ordinary ACS1 2010/2011 metadata omits concept. Preserve that
    # absence; the exact dataset/year and variable key remain independently bound.
    selected = {}
    for variable in VARIABLES:
        item = metadata.get("variables", {}).get(variable, {})
        if not isinstance(item, Mapping):
            raise VerificationRequired(f"historical variable metadata unavailable: {variable}")
        group = variable.split("_", 1)[0]
        omitted_legacy_concept = (source_id == "census_acs1" and vintage in {2010, 2011}
                                  and "concept" not in item and item.get("group") == group)
        if (item.get("predicateType") not in {"int", "float"}
                or not isinstance(item.get("label"), str) or not item["label"].strip()
                or ("group" in item and item["group"] != group)
                or (not omitted_legacy_concept and
                    (not isinstance(item.get("concept"), str) or not item["concept"].strip()))):
            raise VerificationRequired(f"historical variable metadata unavailable: {variable}")
        selected[variable] = dict(item)
    return selected


def geography_identity(item: Mapping[str, str]) -> dict[str, str]:
    params = request_params(item)
    return dict(part.rsplit(":", 1) for part in
                [params["for"], *([params["in"]] if "in" in params else [])])


def value_classification(raw: Any) -> str:
    if isinstance(raw, bool):
        raise ValueError("invalid historical boolean value")
    if raw is None or raw == "null":
        return "provider_null"
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise ValueError("unrecognized historical provider value") from None
    if not math.isfinite(value):
        raise ValueError("nonfinite historical provider value")
    if value in SENTINELS:
        return f"provider_sentinel:{int(value)}"
    if value < 0:
        raise VerificationRequired("unrecognized historical negative/sentinel value")
    return "available"


def omission_receipts(*, source_id: str, vintage: int, snapshot: Mapping[str, Any]) -> list[dict]:
    """Describe observed omissions explicitly. Hashing does not confer approval.

    Only exact 204 member observations and 200 null/known sentinel cells are
    representable. No receipt can waive a missing header or a missing response.
    The caller must also validate the full snapshot and reviewed metadata.
    """
    receipts = []
    for member in snapshot["members"]:
        classified = []
        if member["status"] == "provider_ineligible_no_content":
            if member.get("payload") is not None:
                raise ValueError("no-content observation contains unexpected payload")
            classified = [("provider_no_content", sorted(VARIABLES), 204)]
        elif member["status"] == "available":
            payload = member.get("payload")
            if (not isinstance(payload, list) or len(payload) != 2
                    or not all(isinstance(row, list) for row in payload)):
                raise ValueError("invalid historical provider response shape")
            header, values = payload
            if (not all(isinstance(h, str) for h in header) or len(header) != len(values)
                    or len(set(header)) != len(header) or not set(VARIABLES) <= set(header)):
                raise ValueError("historical variable headers duplicate or missing")
            record = dict(zip(header, values))
            for variable in VARIABLES:
                classification = value_classification(record[variable])
                if classification != "available":
                    classified.append((classification, [variable], 200))
        else:
            raise VerificationRequired("undocumented historical member failure is not an omission")
        for classification, variables, status in classified:
            body = {"schema_version": OMISSION_VERSION, "source_id": source_id,
                "product": PRODUCTS[source_id], "vintage": vintage,
                "geography": {k: member[k] for k in ("geo_id", "level", "census_code")},
                "variables": variables, "request_url": member["request_url"],
                "http_status": status, "classification": classification,
                "response_sha256": sha256_json(member["payload"])}
            receipts.append({**body, "evidence_id": OMISSION_VERSION + "__" + sha256_json(body)})
    return sorted(receipts, key=lambda r: r["evidence_id"])


def validate_snapshot(*, source_id: str, vintage: int, snapshot: Mapping[str, Any],
                      plan: list[dict], excluded: list[dict], omissions: list[dict] | None = None) -> dict:
    if (snapshot.get("contract_version") != CONTRACT_VERSION
            or snapshot.get("source_id") != source_id or snapshot.get("product") != PRODUCTS[source_id]
            or type(snapshot.get("vintage")) is not int or snapshot.get("vintage") != vintage
            or snapshot.get("excluded_geographies") != excluded):
        raise ValueError("historical snapshot source/product/vintage/exclusions mismatch")
    members = snapshot.get("members", [])
    by_geo = {m["geo_id"]: m for m in members}
    if (len(by_geo) != len(members) or len({g["geo_id"] for g in plan}) != len(plan)
            or set(by_geo) != {g["geo_id"] for g in plan}):
        raise ValueError("historical member inventory duplicate, missing, or unexpected")
    expected_receipts = omission_receipts(source_id=source_id, vintage=vintage, snapshot=snapshot)
    supplied = omissions or []
    if (len({r.get("evidence_id") for r in supplied}) != len(supplied)
            or sorted(supplied, key=lambda r: r.get("evidence_id", "")) != expected_receipts):
        raise VerificationRequired("exact explicit evidence required for every historical omission")
    accounting = []
    for item in plan:
        member = by_geo[item["geo_id"]]
        if any(member.get(k) != v for k, v in item.items()):
            raise ValueError("historical requested geography identity mismatch")
        if member.get("request_url") != _safe_url(vintage, PRODUCTS[source_id], request_params(item)):
            raise ValueError("historical request identity mismatch")
        if member["status"] == "provider_ineligible_no_content":
            accounting.extend({"geo_id": item["geo_id"], "variable": v, "classification": "provider_no_content"}
                              for v in VARIABLES)
            continue
        header, values = member["payload"]
        expected_geo = geography_identity(item)
        if set(header) != {"NAME", *VARIABLES, *expected_geo}:
            raise ValueError("historical headers missing or unexpected")
        record = dict(zip(header, values))
        if any(record[k] != v for k, v in expected_geo.items()):
            raise ValueError("historical returned geography differs from request")
        if not isinstance(record["NAME"], str) or not record["NAME"]:
            raise ValueError("historical NAME missing")
        accounting.extend({"geo_id": item["geo_id"], "variable": v,
                           "classification": value_classification(record[v])} for v in VARIABLES)
    if not any(a["classification"] == "available" for a in accounting):
        raise VerificationRequired("no available historical metrics; whole-vintage gap requires separate governance")
    return {"accounting": accounting, "snapshot_sha256": sha256_json(snapshot)}


def checked_response(response: Any, *, allow_no_content: bool) -> Any:
    """204 must be observed with an empty body; errors never become absence."""
    status = response.status_code
    if status == 204 and allow_no_content:
        if getattr(response, "content", None) != b"":
            raise VerificationRequired("historical 204 body evidence absent or nonempty")
        return None
    if status != 200:
        raise VerificationRequired("historical provider request failed or returned unexpected status")
    response.raise_for_status()
    return response.json()


def preflight(*, source_id: str, vintage: int, transport: Any,
              plan: list[dict], excluded: list[dict]) -> dict:
    """Read exact metadata/responses using only the injected transport.

    Credentials remain transport state. Persisted receipts are provider
    observations reviewed as acquisition evidence, not production gap approval.
    """
    if source_id not in PRODUCTS or type(vintage) is not int or not 2005 <= vintage <= 2100:
        raise ValueError("invalid bounded physical product/vintage")
    def read(url, params=None, *, allow_no_content=False):
        try:
            response = transport.get(url, params=params, timeout=60)
            return checked_response(response, allow_no_content=allow_no_content), response.status_code
        except VerificationRequired:
            raise
        except Exception:
            raise VerificationRequired("official historical provider evidence inaccessible or invalid") from None
    dataset = exact_dataset(source_id, vintage, read(f"{API}.json")[0])
    endpoint = f"{API}/{vintage}/{PRODUCTS[source_id]}"
    metadata = validate_metadata(read(endpoint + "/variables.json")[0], source_id=source_id, vintage=vintage)
    members = []
    for item in plan:
        payload, status = read(endpoint, request_params(item), allow_no_content=True)
        members.append({**item, "request_url": _safe_url(vintage, PRODUCTS[source_id], request_params(item)),
                        "status": "provider_ineligible_no_content" if status == 204 else "available", "payload": payload})
    snapshot = {"contract_version": CONTRACT_VERSION, "source_id": source_id,
                "product": PRODUCTS[source_id], "vintage": vintage, "members": members, "excluded_geographies": excluded}
    omissions = omission_receipts(source_id=source_id, vintage=vintage, snapshot=snapshot)
    checked = validate_snapshot(source_id=source_id, vintage=vintage, snapshot=snapshot,
                                plan=plan, excluded=excluded, omissions=omissions)
    return {"schema_version": "acs_historical_provider_preflight_v1", "source_id": source_id,
            "vintage": vintage, "dataset": dataset, "variables": metadata,
            "snapshot": snapshot, "omission_evidence": omissions, **checked}


def validate_evidence(evidence: dict, *, source_id: str, vintage: int,
                      plan: list[dict], excluded: list[dict]) -> dict:
    if (evidence.get("schema_version") != "acs_historical_provider_preflight_v1"
            or evidence.get("source_id") != source_id or evidence.get("vintage") != vintage):
        raise ValueError("historical preflight evidence identity mismatch")
    exact_dataset(source_id, vintage, {"dataset": [evidence["dataset"]]})
    validate_metadata({"variables": evidence["variables"]}, source_id=source_id, vintage=vintage)
    checked = validate_snapshot(source_id=source_id, vintage=vintage, snapshot=evidence["snapshot"],
        plan=plan, excluded=excluded, omissions=evidence.get("omission_evidence", []))
    if any(evidence.get(k) != v for k, v in checked.items()):
        raise ValueError("historical preflight accounting identity mismatch")
    return checked


def acquire_historical_snapshot(*, source_id: str, vintage: int, output: Any,
                                session: Any = None, key: str | None = None,
                                clock=time.monotonic, sleep=time.sleep,
                                wall_clock=time.time) -> dict:
    """Retry transport failures only; retain ordinary acquisition interpretation.

    The vintage budget covers request time, decoding, backoff and Retry-After.
    No request or sleep starts beyond its remaining budget; late responses are
    rejected before snapshot validation/pinning. No reviewed-snapshot fallback.
    """
    import os
    import requests
    from jobs.monthly_refresh.acs_monthly import acquire_snapshot, geography_plan
    if source_id not in PRODUCTS or type(vintage) is not int or not 2005 <= vintage <= 2100:
        raise ValueError("invalid bounded physical product/vintage")
    deadline = clock() + ACQUISITION_DEADLINE_SECONDS
    plan, _ = geography_plan()
    context = {"source_id": source_id, "vintage": vintage, "request_ordinal": 0,
               "request_kind": "setup", "geography_id": None,
               "http_status": None, "attempts_consumed": 0}

    def fail(category):
        raise HistoricalAcquisitionFailure(**context, category=category) from None

    def remaining():
        left = deadline - clock()
        if left <= 0:
            fail("deadline_exhausted")
        return left

    credential = (key if key is not None else os.environ.get("CENSUS_API_KEY", "")).strip()
    if not credential:
        fail("credential_unavailable")
    client = session if session is not None else requests.Session()

    class CheckedTransport:
        def get(self, url, *, params=None, timeout=60):
            context.update(request_ordinal=context["request_ordinal"] + 1,
                           geography_id=None, http_status=None, attempts_consumed=0)
            endpoint = f"{API}/{vintage}/{PRODUCTS[source_id]}"
            if url == API + ".json" and params is None:
                context["request_kind"] = "dataset"
            elif url == endpoint + "/variables.json" and params is None:
                context["request_kind"] = "variables"
            elif url == endpoint:
                context["request_kind"] = "geography"
                public = {k: v for k, v in (params or {}).items() if k != "key"}
                matches = [g for g in plan if request_params(g) == public]
                if len(matches) != 1:
                    fail("request_identity")
                context["geography_id"] = matches[0]["geo_id"]
            else:
                context["request_kind"] = "unknown"
                fail("request_identity")
            for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
                left = remaining()
                context.update(attempts_consumed=attempt, http_status=None)
                response = None
                category = None
                try:
                    # Bound both connect/read inactivity timeouts; their sum fits
                    # the remaining budget. Requests' body reads are not a hard
                    # wall-clock timer; remaining() also rejects late completion.
                    response = client.get(url, params=params,
                        timeout=(min(30.0, left / 2), min(30.0, left / 2)), allow_redirects=False)
                except requests.exceptions.SSLError:
                    fail("tls_failure")
                except requests.exceptions.Timeout:
                    category = "timeout"
                except requests.exceptions.ConnectionError:
                    category = "connection"
                except Exception:
                    fail("transport_failure")
                if response is not None:
                    status = getattr(response, "status_code", None)
                    context["http_status"] = status if type(status) is int and 100 <= status <= 599 else None
                remaining()
                if response is None and category is None:
                    fail("invalid_response")
                retry_after = None
                if response is not None:
                    status = response.status_code
                    if context["http_status"] is None:
                        fail("invalid_response")
                    if status in RETRYABLE_HTTP_STATUSES:
                        category = "http_status"
                        retry_after = getattr(response, "headers", {}).get("Retry-After")
                    else:
                        if status not in {200, 204}:
                            fail("http_status")
                        try:
                            checked_response(response, allow_no_content=context["request_kind"] == "geography")
                        except ValueError:
                            fail("invalid_json" if status == 200 else "invalid_no_content")
                        except Exception:
                            fail("invalid_response")
                        remaining()
                        return response
                if response is not None:
                    close = getattr(response, "close", None)
                    if close is not None:
                        close()
                if attempt == MAX_REQUEST_ATTEMPTS:
                    fail(category)
                delay = _retry_delay(attempt, retry_after, wall_clock)
                if delay >= remaining():
                    fail("deadline_exhausted")
                sleep(delay)
    timer_installed = False
    previous_handler = None
    def expired(signum, frame):
        raise _AcquisitionDeadlineExpired() from None
    try:
        # Socket inactivity timeouts alone cannot stop a response that trickles
        # bytes indefinitely. Hosted acquisition runs on the POSIX main thread;
        # fail closed rather than silently dropping its wall-clock deadline.
        if (not hasattr(signal, "setitimer") or threading.current_thread() is not threading.main_thread()
                or signal.getitimer(signal.ITIMER_REAL)[0] != 0):
            fail("deadline_unavailable")
        previous_handler = signal.getsignal(signal.SIGALRM)
        signal.signal(signal.SIGALRM, expired)
        timer_installed = True
        signal.setitimer(signal.ITIMER_REAL, remaining())
        snapshot = acquire_snapshot(source_id=source_id, vintage=vintage, output=output,
                                    session=CheckedTransport(), key=credential)
        remaining()
        return snapshot
    except _AcquisitionDeadlineExpired:
        fail("deadline_exhausted")
    except HistoricalAcquisitionFailure:
        raise
    except VerificationRequired:
        fail("metadata_mismatch" if context["request_kind"] in {"dataset", "variables"} else "invalid_payload")
    except ValueError:
        fail("invalid_payload")
    except Exception:
        fail("acquisition_failure")
    finally:
        if timer_installed:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous_handler)
        if session is None:
            try:
                client.close()
            except Exception:
                pass  # Cleanup cannot replace the sanitized failure with transport details.
