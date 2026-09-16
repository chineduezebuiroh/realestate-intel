# BEA-C production physical-source integration v0.1

**Status:** locally proven with deterministic full-history fixtures; credentialed live verification remains operator-run.

## Implemented contracts

BEA-C implements two independent physical sources, not a logical family: `bea_gdp_qtr` is Regional `SQGDP9`, line 1, quarterly real GDP SAAR for the nation and five governed states; `bea_gdp_ann` is Regional `CAGDP9`, line 1, annual real GDP for 169 applicable identities. The annual frozen availability registry classifies 129 identities `AVAILABLE_DIRECT` and exactly 40 Virginia county/county-equivalents `PROVIDER_UNAVAILABLE`. No unavailable identity is synthesized, aggregated, substituted, or removed from applicability.

The adapter builds one deterministic `Year=ALL` request per physical source. It maps through `config/geo_manifest.generated.csv`, parses only plain or comma-formatted finite numeric values, maps quarters to quarter ends and years to December 31, preserves BEA's millions-of-chained-2017-dollar values without rescaling, and fails closed on changed geography membership, mapping failure, duplicate keys, unknown periods, line changes, and blank/nonnumeric values.

## Common lifecycle and identities

Normal execution performs provider acquisition, validates and normalizes governed content, resolves a content-addressed semantic identity, writes the standard immutable source-input pin, and constructs/publishes the standard immutable candidate only from that pin. Resume and replay materialize the normalized snapshot embedded in the existing pin and make no BEA call. The two physical sources use the shared pin store, candidate artifact publication, and durable execution-result path. Source execution does not mutate accepted pointers.

The semantic identity binds adapter/parser versions, physical source, sanitized endpoint/request plan, table/line/frequency/unit metadata, exact applicability and availability classifications, normalized governed-content hash, and canonical key-inventory hash. Runtime credentials are transport-only. Raw envelope bytes and retrieval diagnostics do not enter normalized snapshot, semantic hash, request identity, candidate identity, or durable evidence. Consequently raw-only envelope changes retain the same semantic/candidate identity, while a same-period normalized value mutation produces a different identity.

## Local proof

`tests/test_bea_c_physical_integration.py` constructs the complete frozen histories (510 quarterly and 3,096 annual rows) and proves requests, canonical dates/schema, 6/129 direct geography counts, 169 applicability, 40 explicit unavailable identities, no synthesis, deterministic content addressing, credential exclusion, raw-envelope independence, revision sensitivity, fail-closed membership/sentinel behavior, and normal/resume/replay lifecycle ordering. Legacy parity is performed by the credentialed verifier whenever `data/market_serving.duckdb` is available.

Run the credentialed, read-only local proof with:

```bash
BEA_API_KEY='...' PYTHONPATH=. python scripts/bea_c_verify.py \
  --cycle-id monthly_cycle__YYYY-MM__LOCAL_BEA_C \
  --output-dir artifacts/bea_c_local_verification \
  --legacy-db data/market_serving.duckdb
```

The output records source, semantic pin, candidate identities, coverage, and parity counts without recording or printing the credential. The command does not publish, promote, mutate accepted pointers, or enter the shared cohort.

## Remaining work before shared cohort integration

1. Run the credentialed verifier and review the frozen 6/129 direct membership, 40 unavailable classifications, histories, and exact 510/3,096 legacy parity.
2. Separately govern hosted dispatch/registration and routine-automation policy.
3. Add each physical source to a future shared cohort/Source Set change only after review; define no BEA family resolver.
4. Exercise cohort-controlled promotion in a later scope. Accepted-pointer, canonical-market, serving-market, schedule, Redfin, and synthesis changes remain excluded from BEA-C.
