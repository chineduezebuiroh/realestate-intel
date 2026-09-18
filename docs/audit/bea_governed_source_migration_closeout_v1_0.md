# BEA governed-source migration closeout v1.0

## 1. Status

**BEA governed source migration COMPLETE THROUGH SHARED COHORT INTEGRATION /
LIVE-PROVEN. Accepted-state promotion intentionally deferred to the full
intended initial cohort.**

BEA-A through BEA-D are closed. This closeout records the frozen contracts and
evidence; it performs no provider call, artifact publication, or state mutation.

## 2. Governed source inventory

BEA is two independent required governed sources, not one source family.

| source | BEA Regional product | metric | frequency / canonical date | native unit | governed applicability |
|---|---|---|---|---|---:|
| `bea_gdp_qtr` | `SQGDP9`, line 1 | `bea_qgdp_real_total_chained2017_saar` | quarterly / quarter-end | millions of chained 2017 dollars, SAAR (`UNIT_MULT="6"`) | 6 |
| `bea_gdp_ann` | `CAGDP9`, line 1 | `bea_agdp_real_total_chained2017` | annual / December 31 | thousands of chained 2017 dollars (`UNIT_MULT="3"`) | 169 |

There is no logical `bea`, no BEA family resolver, and neither physical source
is lineage-only relative to the other. Annual ingestion preserves provider-native
thousands: it performs **no numeric rescaling**.

## 3. Frozen physical contracts

Both products use line 1, **All industry total**. Quarterly observations are
seasonally adjusted annual-rate levels. The live proof covers 2005Q1--2026Q1
for quarterly GDP and 2001--2024 for annual GDP. Provider credentials are not
identity or durable evidence.

## 4. Geography and applicability contract

Quarterly applicability is exactly one nation and five states. Annual
applicability is one nation, five states, and 163 counties/county-equivalents
(169 total). The provider directly returns 129 annual identities: one nation,
five states, and 123 counties. The remaining exactly 40 governed Virginia
identities have `observation_count=0`, are absent from legacy data, and remain
explicitly classified `PROVIDER_UNAVAILABLE`.

The contract permits no county aggregation to synthesize metros and no annual
Virginia synthesis, substitution, aggregation, or removal from applicability.

## 5. Production candidate and live-proof evidence

| evidence | `bea_gdp_qtr` | `bea_gdp_ann` |
|---|---|---|
| numeric observations | 510 (85 per geography) | 3,096 (24 per returned geography) |
| legacy comparison | 510 exact; no legacy-only, provider-only, or revised values | 3,096 exact; no legacy-only, provider-only, or revised values |
| candidate | `src__bea_gdp_qtr__2026-03__r1__6060ef25ef70eb62` | `src__bea_gdp_ann__2024-12__r1__384cf1e026bce84e` |
| pin | `source_input__bea_gdp_qtr__ad80383e793408a6d36b` | `source_input__bea_gdp_ann__f749f8bc494f943a576d` |
| provider/content release | `bea_gdp_qtr:8a152358056f94259f33ba3399e6b60c39460588281bcf09d3133166fc29a29f` | `bea_gdp_ann:536e861b11027c3906b88f3f8972d2ae957caa13d7bea38afc0b96a97f070ae3` |
| snapshot SHA-256 | `f79faa04760f2ed0826f9eced000af96f7b00bf7b9588e9cbcf915e35508fe71` | `4bbaad9ca316369b5df6199eb8b43b79fa395ecefe901698fb892c539b57f56b` |
| normalized-content SHA-256 | `8a152358056f94259f33ba3399e6b60c39460588281bcf09d3133166fc29a29f` | `536e861b11027c3906b88f3f8972d2ae957caa13d7bea38afc0b96a97f070ae3` |
| key-inventory SHA-256 | `94222960df0c85acf993456f8c8019fc9a311ca48767d4b4a76f8d48dee57bfd` | `1c331b3d023dc77e70e5d8f3318fd29fa9cf826b15fa3455c330a08a278cffd2` |

## 6. Pin and semantic identity contract

The existing `monthly_source_input_pin_v1` contract is sufficient; no common
pin-schema v2 is needed. Deterministic normalized governed snapshot bytes are
the exact immutable pinned input. `pin_id` is cycle-scoped selected-input
identity, not semantic candidate identity. Governed normalized content plus
contract identity determines semantic identity; raw-response hashes, retrieval
timestamps, and transport details are nonsemantic lineage evidence.

A same-period normalized-content change creates a new immutable revision. A raw
response mismatch alone does not when normalized governed content and key
inventory are unchanged. Normal execution acquires, normalizes, hashes, persists
and rereads the exact pin before execution and immutable candidate publication.
Resume/replay recovers that durable snapshot, verifies size and SHA, and executes
without BEA provider rediscovery.

## 7. Shared cohort integration

BEA-D found the existing shared cohort contract sufficient. Both sources are
separately registered required hosted members and participate independently in
hosted fan-out, durable result recovery, resume planning, the common barrier,
and Source Set construction. Both use ordinary direct Source Set entries;
neither belongs to `PHYSICAL_FAMILY_SOURCES`. Missing either member or presenting
duplicate/colliding identities fails closed. Main remains durable authority.

## 8. Promotion state

Future pointers are `accepted.source.bea_gdp_qtr` and
`accepted.source.bea_gdp_ann`; neither has been promoted. BEA-D moved no source
pointer and promoted no Source Set or canonical market. It consumed no Redfin
readiness, changed no serving state, and enabled no routine schedule.

## 9. Phase and commit lineage

* **BEA-A:** reconnaissance and conceptual contract.
* **BEA-B:** live provider verification and physical-contract freeze.
* **BEA-C:** production physical-source implementation plus local/live candidate proof.
* **BEA-D:** shared cohort integration, implementation commit `f9efd698`, merged
  by PR #245 at authoritative merge commit `1c6c377d`.

Post-merge local regression was `PYTHONPATH=. pytest -q tests` with 61 passing.

## 10. Remaining deferred work

Accepted-state promotion remains intentionally deferred to the full intended
initial cohort and fresh-Redfin production endgame. The next project work is
remaining source migration, not another BEA implementation phase.
