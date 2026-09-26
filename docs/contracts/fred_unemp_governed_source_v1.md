# Governed FRED unemployment source v1

## Status and bounded activation

`fred_unemp` remains a required, independent direct source.  This contract
closes its governed source lifecycle only.  It does **not** add the source to
the hosted master cohort, change promotion cardinality, move an accepted
pointer, correct the accepted August cohort, or change serving promotion.

The Phase 2 document that called eleven physical and nine logical/direct
sources the "final full cohort" closed an erroneous inventory before this
required vertical slice was migrated.  The intended future inventory is twelve
physical sources and ten logical/direct Source Set entries.  That correction is
explicitly deferred until the independently hosted source proof is reviewed.

## Applicability and ownership

The sole applicability authority is `config/geo_manifest.generated.csv`.  Its
exact enabled inventory is:

| Canonical geography | FRED series |
|---|---|
| `united_states__nation` | `UNRATE` |
| `california__state` | `CAUR` |
| `district_of_columbia__state` | `DCUR` |
| `maryland__state` | `MDUR` |
| `new_jersey__state` | `NJUR` |
| `virginia__state` | `VAUR` |

The source owns exactly `fred_unemployment_rate_sa`.  It is a seasonally
adjusted FRED measure and is not interchangeable with LAUS
`laus_unemployment_rate_nsa`.  Existing scoring semantics remain unchanged:
LAUS is preferred where available and FRED is the national/state fallback.

## Immutable provider input

Normal mode first reads durable authority on `main`.  When neither a successful
cycle result nor an input pin exists, it resolves the exact manifest inventory,
requires `FRED_API_KEY`, acquires all six complete histories, and fails closed
on missing, duplicate, unexpected, empty, failed, or non-finite series.  It
normalizes dates/values into deterministic canonical JSON and embeds those exact
bytes in a `monthly_source_input_pin_v1`.  The manifest hash, complete request
map, content hash, size, and request identity are pinned.  Candidate execution
starts only after the durable pin is written and read back.

The embedded immutable bytes—not a mutable FRED URL—are replay authority.
Resume reuses a valid durable successful result, or reconstructs only from the
existing pin when execution stopped after pinning.  Replay requires the exact
pin.  Neither mode performs provider discovery or acquisition after pinning.

## Candidate and reconciliation

The candidate contains exactly six geographies and one metric, uses canonical
month-end dates, finite values, and unique canonical keys, and records
per-series observation bounds and row counts.  It binds exact pin, request,
content, contract, and governed-config identities.  Ordinary FRED complete
history is current truth for returned keys; prior-only keys survive unless a
future governed retraction contract says otherwise.  Changed current truth
creates a new immutable artifact.  Identical reconciled truth reuses the exact
prior immutable artifact.

Publication and durable cycle-result recording are hosted operations against
`main`.  The migration-branch checkout supplies executable code only.  Source
execution may publish immutable input/candidate/result records, but it must not
change `accepted.source`, `accepted.source_set`, `accepted.canonical_market`,
`accepted.serving_market`, or Redfin readiness.

## Monthly cohort integration

Future governed cohorts execute exactly twelve physical barrier members:
`redfin`, `fred_macro`, `fred_unemp`, `ces`, `laus`, `census_bps`,
`census_bps_provisional`, `census_acs1`, `census_acs5`, `bea_gdp_qtr`,
`bea_gdp_ann`, and `census_nrc`. Their Source Set resolves to exactly ten
logical/direct entries: `fred_macro`, `fred_unemp`, `ces`, `laus`, `redfin`,
`bps`, `acs`, `bea_gdp_qtr`, `bea_gdp_ann`, and `census_nrc`.

`fred_unemp` is a direct source owning `fred_unemployment_rate_sa`; it is not a
`fred_macro` member and is not canonically coalesced with LAUS. Promotion record
v2 requires all ten entries. Persisted v1 nine-entry promotion records retain
v1 validation and transition order, so their immutable identities are not
reinterpreted.
