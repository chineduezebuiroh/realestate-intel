# BEA-C production physical-source integration v0.1

**Status:** fixture-backed local proof complete; credentialed high-fidelity verification pending.
No accepted pointer, Source Set, cohort, canonical/serving market, Redfin, or schedule was changed.

## Physical contracts

`bea_gdp_qtr` is Regional `SQGDP9`, line 1, `Year=ALL`, quarterly, metric
`bea_qgdp_real_total_chained2017_saar`, and six directly returned nation/state
geographies. Its frozen history is 2005Q1--2026Q1 (510 rows). Dates are calendar
quarter ends and values remain millions of chained 2017 dollars at SAAR without
rescaling.

`bea_gdp_ann` is Regional `CAGDP9`, line 1, `Year=ALL`, annual, metric
`bea_agdp_real_total_chained2017`, and 169 governed applicability identities.
The direct contract is 129 identities and 3,096 rows for 2001--2024. The other
40 identities are governed Virginia counties/county-equivalents classified
`PROVIDER_UNAVAILABLE`. They are retained in coverage evidence and never
synthesized, substituted, combined, or emitted as observations. Annual dates
are December 31 and values remain millions of chained 2017 dollars.

The frozen geography/availability inventory is
`config/bea_governed_geographies_v1.csv`, joined to canonical `geo_id` values
established by the geography registry. Geography membership, complete history,
line/unit metadata, duplicates, and blank/unknown nonnumeric sentinels all fail
closed.

BEA-B establishes `TableName`, `LineCode=1`, and the line description `All
industry total` through the governed request and table/parameter metadata.
Regional GetData observation rows do not repeat `LineCode` or
`LineDescription`; they do carry `CL_UNIT=Millions of chained 2017 dollars` and
`UNIT_MULT=6`. The adapter therefore keeps table/line/description as strict
snapshot source-contract metadata while validating the two fields actually
returned on every observation row. This applies consistently to the SQGDP9 and
CAGDP9 Regional row contracts and does not weaken request identity validation.

## Common lifecycle and normalized snapshot pin

The implementation uses `monthly_source_input_pin_v1` unchanged. Normal mode
makes one credential-safe POST for the selected physical source, validates the
complete frozen provider contract, sorts and serializes governed source truth as
canonical compact JSON, and hashes those bytes as member `normalized_snapshot`.
The member embeds the exact snapshot bytes as base64 and addresses them with a
`pin-embedded://` URI. This uses the existing durable GitHub Contents pin as the
immutable storage object; it creates no BEA registry. Fixture snapshot sizes are
about 60 KB quarterly / 392 KB annual before base64 and remain below the GitHub
Contents single-file limit.

Embedding is intentional: after the pin is durably persisted and reread,
resume/replay decodes and SHA-256 verifies the snapshot without contacting the
mutable BEA endpoint. Normal re-entry also resolves the existing pin before its
discovery callback, so it neither reacquires BEA nor regenerates `retrieved_at`.

The snapshot contains the adapter/parser versions, sanitized request plan,
Regional/table/line/frequency and stable unit metadata, full applicability and
availability inventories, normalized observations, governed-content hash, and
canonical-key-inventory hash. It excludes credentials, retrieval time, raw
response hashes, transport diagnostics, and unstable envelope metadata.

The member `retrieved_at` describes creation of that cycle's immutable snapshot.
Raw-response hash and transport status are acquisition lineage passed to the
candidate manifest only; neither participates in the semantic snapshot hash,
content-addressed provider release ID, canonical Parquet hash, or artifact ID.
A raw-envelope-only change therefore preserves snapshot and candidate identity.
A normalized value change changes both the provider semantic release and the
candidate artifact identity.

## Candidate and publication semantics

Both adapters emit only the standard seven canonical columns with
`property_type_id=all` and `property_type=all`. They use the common
`create_artifact` identity and existing publication abstraction. The physical
sources remain independent; there is no logical BEA family resolver.

Fixture tests prove exact request construction, credential exclusion, 6/129
direct geography output, 169 annual applicability, 40 unavailable identities,
510/3,096 row histories, dates and dimensions, deterministic output, raw-only
stability, same-period mutation identity, strict geography/sentinel failures,
durable snapshot recovery, normal persistence ordering, and acquisition-free
resume/replay.

## Credentialed local verification

No live credential was available in the hosted implementation environment, so
no live success is claimed. Run each source independently:

```bash
BEA_API_KEY='<runtime-only>' PYTHONPATH=. python scripts/bea_c_verify.py \
  --source-id bea_gdp_qtr --cycle-id local-bea-c-qtr \
  --workspace /tmp/bea-c-qtr --legacy-db data/market_serving.duckdb

BEA_API_KEY='<runtime-only>' PYTHONPATH=. python scripts/bea_c_verify.py \
  --source-id bea_gdp_ann --cycle-id local-bea-c-ann \
  --workspace /tmp/bea-c-ann --legacy-db data/market_serving.duckdb
```

Expected legacy parity is quarterly 510 exact and annual 3,096 exact, with zero
revised, provider-only, or legacy-only keys. The verifier writes only under the
chosen workspace and does not publish, promote, or mutate repository governance.

## Remaining gate before shared cohort integration

Run both credentialed commands and review their request, coverage, identity, and
legacy-parity results. Shared cohort registration, Source Set membership,
accepted-pointer promotion, and routine scheduling remain separate governed
work after that live proof.
