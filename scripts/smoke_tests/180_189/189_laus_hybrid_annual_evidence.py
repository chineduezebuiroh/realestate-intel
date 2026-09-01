"""Smoke 189: Atom event discovery is deterministic and completion-fail-closed."""
from __future__ import annotations

import json
from pathlib import Path

from jobs.monthly_refresh.laus_annual_processing import AnnualState, evaluate
from jobs.monthly_refresh.laus_hybrid_annual_evidence import (
    AnnualApplicabilityEvidenceUnavailable, LausFeedParserError,
    annual_event_evidence, january_reference_release, parse_laus_atom,
    verify_official_capture_fixture,
)
from sources.bls_laus.artifact import _classify_observation


# Synthetic parser fixture mirroring the exact structures reported from the
# official capture. The complete 8,056 provider bytes are not in this repository.
ATOM = b'''<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
 <id>bls.gov:feed:laus</id><title>State Employment and Unemployment (Monthly)</title>
 <updated>2026-08-21T09:20:27.646-04:00</updated>
 <entry><id>laus-2026_04_08__10_00_00</id>
  <title>January jobless rate up in 1 state; payroll jobs up in 5 states, down in D.C.</title>
  <link href="https://www.bls.gov/news.release/archives/laus_04082026.htm"/>
  <published>2026-04-08T10:00:00-04:00</published><updated>2026-04-08T10:00:00-04:00</updated>
  <category>News Release</category><content>In January, unemployment rates were higher in 1 state and stable in 49 states and the District of Columbia. Nonfarm payroll employment increased in 5 states, decreased in the District, and was essentially unchanged in 45 states.</content>
 </entry>
</feed>'''


def expect(error, operation):
    try:
        operation()
    except error:
        return
    raise AssertionError(f"expected {error.__name__}")


def main():
    policy = json.loads(Path("config/laus_annual_processing_policy_v1.json").read_text())
    parsed = parse_laus_atom(ATOM)
    assert parsed == parse_laus_atom(ATOM)
    assert parsed["feed_id"] == "bls.gov:feed:laus"
    assert len({entry["id"] for entry in parsed["entries"]}) == len(parsed["entries"])
    assert parsed["entries"][0]["categories"] == ["News Release"]
    assert parsed["entries"][0]["publication_date"] == "2026-04-08"
    release = january_reference_release(parsed)
    assert release["annual_reference_year"] == 2026
    expect(AnnualApplicabilityEvidenceUnavailable, lambda: annual_event_evidence(parsed))
    # Synthetic C1 state-machine fixture: this is not claimed as captured BLS
    # annual-applicability evidence.
    applicability = {"evidence_type": "official_bls_publication",
                     "authoritative_url": "https://www.bls.gov/lau/notices/2026.htm",
                     "annual_reference_year": 2026,
                     "provider_release_id": "synthetic-applicability-2026"}
    event = annual_event_evidence(parsed, applicability=applicability)
    assert event["annual_reference_year"] == 2026
    assert event["processing_classes"] == []
    watching = evaluate(policy=policy, evidence=event)
    assert watching.state == AnnualState.WATCHING
    assert watching.evidence["missing_processing_classes"] == ["model_based_state", "substate_nonmodeled"]
    # Numeric change and no-change cannot bridge the absent provider completion marker.
    assert evaluate(policy=policy, evidence=event, numeric_changes=True).state == AnnualState.WATCHING
    assert evaluate(policy=policy, evidence=event, numeric_changes=False).state == AnnualState.WATCHING
    status, value, rendered, codes = _classify_observation(
        {"value": "-", "footnotes": [{"code": "X", "text": "Data not available"}]})
    assert (status, value, rendered, codes) == ("provider_unavailable", None, None, ("X",))
    assert evaluate(policy=policy, evidence=event, observation_footnote_codes=codes).state == AnnualState.WATCHING

    # The exact official bytes are unavailable; the verification boundary must
    # reject this synthetic structural fixture rather than mislabel it official.
    expect(LausFeedParserError, lambda: verify_official_capture_fixture(ATOM))
    february = ATOM.replace(b"January jobless", b"February jobless").replace(
        b"In January,", b"In February,")
    assert january_reference_release(parse_laus_atom(february)) is None
    expect(LausFeedParserError, lambda: parse_laus_atom(ATOM.replace(b"bls.gov:feed:laus", b"bad")))
    expect(LausFeedParserError, lambda: parse_laus_atom(ATOM.replace(b"</feed>", b"<entry></entry></feed>")))
    expect(LausFeedParserError, lambda: parse_laus_atom(ATOM.replace(
        b"</feed>", ATOM[ATOM.index(b"<entry>"):ATOM.index(b"</entry>") + 8] + b"</feed>")))
    expect(LausFeedParserError, lambda: parse_laus_atom(ATOM.replace(b"www.bls.gov", b"example.com")))
    expect(LausFeedParserError, lambda: parse_laus_atom(
        ATOM.replace(b"laus_04082026.htm", b"laus_04092026.htm")))
    expect(LausFeedParserError, lambda: parse_laus_atom(
        ATOM.replace(b"<published>2026-04-08", b"<published>2026-04-09")))
    expect(LausFeedParserError, lambda: parse_laus_atom(
        ATOM.replace(b"<updated>2026-04-08", b"<updated>2026-04-09")))
    expect(LausFeedParserError, lambda: parse_laus_atom(
        ATOM.replace(b"<category>News Release</category>",
                     b'<category term="News Release"/>')))
    expect(LausFeedParserError, lambda: january_reference_release(parse_laus_atom(
        ATOM.replace(b"In January,", b"In February,"))))
    print("Smoke 189 passed: RSS plus separate applicability starts WATCHING; RSS/numeric data cannot authorize READY.")


if __name__ == "__main__":
    main()
