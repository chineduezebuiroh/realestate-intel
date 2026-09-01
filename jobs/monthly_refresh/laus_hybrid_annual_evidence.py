"""Machine-readable LAUS annual-event evidence (C1b.2 stop boundary).

The Atom feed can establish that the January LAUS release occurred.  Neither
API v2 nor the time-series download contract exposes a reviewed annual
completion marker, so this module intentionally cannot produce processing-class
completion evidence or READY_FOR_ANNUAL_DEEP.
"""
from __future__ import annotations

import hashlib
import re
from datetime import date, datetime
from typing import Any
from urllib.parse import urlparse
from xml.etree import ElementTree


FEED_ID = "bls.gov:feed:laus"
FEED_TITLE = "State Employment and Unemployment (Monthly)"
EVIDENCE_SCHEMA = "laus_annual_processing_evidence_v1"
ATOM = "{http://www.w3.org/2005/Atom}"
ENTRY_ID = re.compile(
    r"^laus-(\d{4})_(\d{2})_(\d{2})__(\d{2})_(\d{2})_(\d{2})$"
)
ARCHIVE_PATH = re.compile(r"/news\.release/archives/laus_(\d{2})(\d{2})(\d{4})\.htm")
OFFICIAL_CAPTURE_SHA256 = "64ff95bd1a6c60860dd4d49b06ca1bac10d19c1121d1901443a9fe2fef295631"
OFFICIAL_CAPTURE_SIZE = 8056


class LausFeedParserError(ValueError):
    """Official feed bytes do not satisfy the frozen narrow Atom contract."""


class AnnualApplicabilityEvidenceUnavailable(RuntimeError):
    """A January reference release cannot itself establish annual applicability."""


def _text(node: ElementTree.Element, name: str) -> str:
    child = node.find(ATOM + name)
    value = "" if child is None or child.text is None else child.text.strip()
    if not value:
        raise LausFeedParserError(f"LAUS Atom {name} is missing")
    return value


def _element_text(node: ElementTree.Element, field: str) -> str:
    value = "" if node.text is None else node.text.strip()
    if not value:
        raise LausFeedParserError(f"LAUS Atom {field} is missing")
    return value


def _timestamp(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LausFeedParserError("LAUS Atom timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise LausFeedParserError("LAUS Atom timestamp lacks an offset")
    return value


def _archive_link(node: ElementTree.Element) -> str:
    links = [item.get("href", "").strip() for item in node.findall(ATOM + "link")
             if item.get("rel", "alternate") == "alternate"]
    if len(links) != 1:
        raise LausFeedParserError("LAUS Atom entry must have one alternate link")
    parsed = urlparse(links[0])
    if (parsed.scheme, parsed.hostname) != ("https", "www.bls.gov") or not ARCHIVE_PATH.fullmatch(parsed.path):
        raise LausFeedParserError("LAUS Atom entry link is not a BLS LAUS archive")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise LausFeedParserError("LAUS Atom entry link has forbidden components")
    return links[0]


def _publication_identity(entry_id: str, link: str, published: str, updated: str) -> date:
    """Validate mutually redundant provider publication identity fields."""
    id_match = ENTRY_ID.fullmatch(entry_id)
    archive_match = ARCHIVE_PATH.fullmatch(urlparse(link).path)
    if id_match is None or archive_match is None:
        raise LausFeedParserError("LAUS Atom publication identity is malformed")
    try:
        id_date = date(*(int(value) for value in id_match.groups()[:3]))
        archive_date = date(int(archive_match.group(3)), int(archive_match.group(1)),
                            int(archive_match.group(2)))
        published_value = datetime.fromisoformat(published.replace("Z", "+00:00"))
        updated_value = datetime.fromisoformat(updated.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LausFeedParserError("LAUS Atom publication identity date is invalid") from exc
    if id_date != archive_date or id_date != published_value.date() or id_date != updated_value.date():
        raise LausFeedParserError("LAUS Atom publication identity dates contradict")
    id_time = tuple(int(value) for value in id_match.groups()[3:])
    if id_time != (published_value.hour, published_value.minute, published_value.second):
        raise LausFeedParserError("LAUS Atom entry ID and published time contradict")
    return id_date


def parse_laus_atom(body: bytes) -> dict[str, Any]:
    """Parse provider facts only; XML/schema/identity drift is fatal."""
    try:
        root = ElementTree.fromstring(body)
    except ElementTree.ParseError as exc:
        raise LausFeedParserError("LAUS Atom XML is invalid") from exc
    if root.tag != ATOM + "feed":
        raise LausFeedParserError("LAUS feed root is not Atom feed")
    feed_id, title = _text(root, "id"), _text(root, "title")
    if feed_id != FEED_ID or title != FEED_TITLE:
        raise LausFeedParserError("LAUS Atom feed identity mismatch")
    entries: list[dict[str, Any]] = []
    seen: set[str] = set()
    for node in root.findall(ATOM + "entry"):
        entry_id = _text(node, "id")
        if entry_id in seen:
            raise LausFeedParserError("LAUS Atom entry ID is duplicated")
        seen.add(entry_id)
        categories = sorted({_element_text(item, "category") for item in node.findall(ATOM + "category")})
        if categories != ["News Release"]:
            raise LausFeedParserError("LAUS Atom entry category is not News Release")
        link = _archive_link(node)
        published, updated = _timestamp(_text(node, "published")), _timestamp(_text(node, "updated"))
        publication_date = _publication_identity(entry_id, link, published, updated)
        entries.append({"id": entry_id, "title": _text(node, "title"), "link": link,
                        "published": published, "updated": updated, "categories": categories,
                        "content": _text(node, "content"),
                        "publication_date": publication_date.isoformat()})
    if not entries:
        raise LausFeedParserError("LAUS Atom feed has no entries")
    return {"feed_id": feed_id, "feed_title": title, "feed_updated": _timestamp(_text(root, "updated")),
            "response_sha256": hashlib.sha256(body).hexdigest(), "entries": entries}


def january_reference_release(feed: dict[str, Any]) -> dict[str, Any] | None:
    """Classify a corroborated January reference-period release, not annual processing."""
    matches = []
    for entry in feed["entries"]:
        title_january = entry["title"].startswith("January ")
        content_january = entry["content"].startswith("In January, ")
        if title_january != content_january:
            raise LausFeedParserError("LAUS January reference-month fields contradict")
        if title_january:
            matches.append(entry)
    if len(matches) > 1:
        raise LausFeedParserError("contradictory duplicate January annual events")
    if not matches:
        return None
    entry = matches[0]
    # LAUS reference-month semantics: a January release's reference year is its
    # provider-validated publication year. This is event identity, not evidence
    # that annual revisions apply or that either processing class is complete.
    annual_reference_year = date.fromisoformat(entry["publication_date"]).year
    return {"annual_reference_year": annual_reference_year, "entry": entry,
            "surface": "https://www.bls.gov/feed/laus.rss",
            "provider_content_sha256": feed["response_sha256"]}


def annual_event_evidence(feed: dict[str, Any], *, applicability: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Combine RSS reference-release facts with separately governed applicability."""
    release = january_reference_release(feed)
    if release is None:
        return None
    if applicability is None:
        raise AnnualApplicabilityEvidenceUnavailable(
            "January RSS release does not establish annual-processing applicability")
    annual_reference_year = release["annual_reference_year"]
    expected = {"evidence_type", "authoritative_url", "annual_reference_year", "provider_release_id"}
    if set(applicability) != expected or applicability.get("evidence_type") != "official_bls_publication":
        raise LausFeedParserError("annual-processing applicability evidence is malformed")
    parsed = urlparse(str(applicability.get("authoritative_url", "")))
    if parsed.scheme != "https" or parsed.hostname not in {"bls.gov", "www.bls.gov"}:
        raise LausFeedParserError("annual-processing applicability evidence is not official BLS")
    if int(applicability.get("annual_reference_year", -1)) != annual_reference_year:
        raise LausFeedParserError("annual-processing applicability year contradicts RSS release")
    entry = release["entry"]
    base = {"evidence_type": "official_bls_publication", "authoritative_url": entry["link"],
            "annual_reference_year": annual_reference_year}
    return {"schema_version": EVIDENCE_SCHEMA, "annual_reference_year": annual_reference_year,
            "release_event": {**base, "expected": True, "provider_release_id": entry["id"]},
            "processing_classes": [],
            "machine_evidence": {"surface": release["surface"],
                                 "provider_content_sha256": release["provider_content_sha256"],
                                 "annual_processing_applicability": dict(applicability)}}


def verify_official_capture_fixture(body: bytes) -> dict[str, Any]:
    """Verify and parse the exact reviewed capture when its bytes become available."""
    if len(body) != OFFICIAL_CAPTURE_SIZE or hashlib.sha256(body).hexdigest() != OFFICIAL_CAPTURE_SHA256:
        raise LausFeedParserError("LAUS official capture fixture identity mismatch")
    return parse_laus_atom(body)
