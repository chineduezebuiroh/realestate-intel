"""Smoke 189: official LAUS RSS event discovery is deterministic and completion-fail-closed."""
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


OFFICIAL_FIXTURE = Path(
    "scripts/smoke_tests/fixtures/bls_laus/laus_rss_20260821.xml"
)

# Synthetic parser fixture retained only for deliberate drift / negative tests.
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

    # Exact reviewed official BLS capture is now the positive provider-contract path.
    official_bytes = OFFICIAL_FIXTURE.read_bytes()
    official = verify_official_capture_fixture(official_bytes)

    assert len(official_bytes) == 8056
    assert official["response_sha256"] == (
        "64ff95bd1a6c60860dd4d49b06ca1bac10d19c1121d1901443a9fe2fef295631"
    )
    assert official["feed_id"] == "bls.gov:feed:laus"
    assert official["feed_title"] == "State Employment and Unemployment (Monthly)"
    assert official["feed_updated"] == "2026-08-21T09:20:27.646-04:00"

    january = [
        entry for entry in official["entries"]
        if entry["id"] == "laus-2026_04_08__10_00_00"
    ]
    assert len(january) == 1
    january = january[0]
    assert january["title"] == (
        "January jobless rate up in 1 state; payroll jobs up in 5 states, down in D.C."
    )
    assert january["link"] == (
        "https://www.bls.gov/news.release/archives/laus_04082026.htm"
    )
    assert january["published"] == "2026-04-08T10:00:00-04:00"
    assert january["updated"] == "2026-04-08T10:00:00-04:00"
    assert january["categories"] == ["News Release"]
    assert january["content"].startswith("In January, ")
    assert january["publication_date"] == "2026-04-08"

    release = january_reference_release(official)
    assert release is not None
    assert release["annual_reference_year"] == 2026
    assert release["entry"]["id"] == "laus-2026_04_08__10_00_00"
    assert release["provider_content_sha256"] == official["response_sha256"]

    # RSS alone still cannot establish annual-processing applicability.
    expect(
        AnnualApplicabilityEvidenceUnavailable,
        lambda: annual_event_evidence(official),
    )

    # Separately governed applicability may begin WATCHING, but cannot establish
    # either processing class or authorize annual deep.
    applicability = {
        "evidence_type": "official_bls_publication",
        "authoritative_url": "https://www.bls.gov/lau/notices/2026.htm",
        "annual_reference_year": 2026,
        "provider_release_id": "synthetic-applicability-2026",
    }
    event = annual_event_evidence(official, applicability=applicability)
    assert event["annual_reference_year"] == 2026
    assert event["processing_classes"] == []

    watching = evaluate(policy=policy, evidence=event)
    assert watching.state == AnnualState.WATCHING
    assert watching.evidence["missing_processing_classes"] == [
        "model_based_state",
        "substate_nonmodeled",
    ]

    # Numeric change/no-change cannot bridge absent provider completion evidence.
    assert evaluate(
        policy=policy, evidence=event, numeric_changes=True
    ).state == AnnualState.WATCHING
    assert evaluate(
        policy=policy, evidence=event, numeric_changes=False
    ).state == AnnualState.WATCHING

    status, value, rendered, codes = _classify_observation(
        {"value": "-", "footnotes": [{"code": "X", "text": "Data not available"}]}
    )
    assert (status, value, rendered, codes) == (
        "provider_unavailable", None, None, ("X",)
    )
    assert evaluate(
        policy=policy,
        evidence=event,
        observation_footnote_codes=codes,
    ).state == AnnualState.WATCHING

    # Exact-capture identity is byte-sensitive.
    expect(
        LausFeedParserError,
        lambda: verify_official_capture_fixture(official_bytes + b"\n"),
    )

    # Synthetic fixtures remain negative / parser-drift tests only.
    synthetic = parse_laus_atom(ATOM)
    assert synthetic == parse_laus_atom(ATOM)
    expect(LausFeedParserError, lambda: verify_official_capture_fixture(ATOM))

    february = ATOM.replace(
        b"January jobless", b"February jobless"
    ).replace(
        b"In January,", b"In February,"
    )
    assert january_reference_release(parse_laus_atom(february)) is None

    expect(
        LausFeedParserError,
        lambda: parse_laus_atom(
            ATOM.replace(b"bls.gov:feed:laus", b"bad")
        ),
    )
    expect(
        LausFeedParserError,
        lambda: parse_laus_atom(
            ATOM.replace(b"</feed>", b"<entry></entry></feed>")
        ),
    )
    expect(
        LausFeedParserError,
        lambda: parse_laus_atom(
            ATOM.replace(
                b"</feed>",
                ATOM[ATOM.index(b"<entry>"):ATOM.index(b"</entry>") + 8]
                + b"</feed>",
            )
        ),
    )
    expect(
        LausFeedParserError,
        lambda: parse_laus_atom(
            ATOM.replace(b"www.bls.gov", b"example.com")
        ),
    )
    expect(
        LausFeedParserError,
        lambda: parse_laus_atom(
            ATOM.replace(
                b"laus_04082026.htm",
                b"laus_04092026.htm",
            )
        ),
    )
    expect(
        LausFeedParserError,
        lambda: parse_laus_atom(
            ATOM.replace(
                b"<published>2026-04-08",
                b"<published>2026-04-09",
            )
        ),
    )
    expect(
        LausFeedParserError,
        lambda: parse_laus_atom(
            ATOM.replace(
                b"<updated>2026-04-08",
                b"<updated>2026-04-09",
            )
        ),
    )
    expect(
        LausFeedParserError,
        lambda: parse_laus_atom(
            ATOM.replace(
                b"<category>News Release</category>",
                b'<category term="News Release"/>',
            )
        ),
    )
    expect(
        LausFeedParserError,
        lambda: january_reference_release(
            parse_laus_atom(
                ATOM.replace(b"In January,", b"In February,")
            )
        ),
    )

    print(
        "Smoke 189 passed: exact official RSS establishes January reference "
        "release only; applicability remains separate and completion remains WATCHING."
    )


if __name__ == "__main__":
    main()
