# ACS-B live provider verification and physical-contract freeze v0.1

**Status:** `PENDING_LOCAL_PROVIDER_VERIFICATION` (2026-09-08)

**Decision:** **NO-GO for physical-contract freeze or ACS-C.** The task network
proxy rejected the authoritative Census API before TLS connection (`CONNECT`
HTTP 403), and the web facility returned HTTP 401. The ACS-B stop condition
therefore applies. This artifact records what was tested, what remains unproven,
and exact safe local capture commands. It does not turn ACS-A assumptions or
checked-in legacy observations into provider facts.

## A. Executive conclusion

No authoritative ACS1 or ACS5 release was verified. Required variables, current
membership (especially geography level 314), sentinel semantics, release
metadata, and current content could not be captured. The two proposed physical
boundaries remain plausible but **unfrozen**. No adapter, registry, snapshot,
candidate, pointer, Source Set, canonical/serving write, or workflow was added or
executed.

## B. Official provider evidence

The primary machine-readable root attempted was
`https://api.census.gov/data.json`. Both `curl -I --max-time 20` and Python
`requests.get(..., timeout=20)` failed at the configured proxy. Because failure
preceded a Census response, it proves neither availability nor absence.

Local capture must include these official resources for each dynamically selected
year: `data.json`; each product's dataset `.json`, `geography.json`, and
`variables.json`; group metadata for `B01003` and `B19013`; and official release
schedule, API key/quota, estimate guidance, corrections/data notes, and
predicate/special-value documentation. No official bytes or hashes were captured.

## C. Authentication and credential contract

**Unverified:** anonymous availability and current anonymous/keyed quotas. The
legacy module requires `CENSUS_API_KEY`, but repository behavior is not provider
evidence. Eventual hosted runtime should inject `CENSUS_API_KEY` from a GitHub
Actions secret solely at transport time. It must never enter semantic identity,
URLs, logs, manifests, catalog records, diagnostics, fixtures, or hashes. No
secret was read, created, printed, or modified.

## D. Verified ACS1 release contract

**NOT VERIFIED.** Proposed source `census_acs1` and path `acs/acs1` remain the
accepted ACS-A starting hypothesis. Latest eligible year, title, publication
metadata, schema hashes, and content identity remain pending.

## E. Verified ACS5 release contract

**NOT VERIFIED.** Proposed source `census_acs5` and path `acs/acs5` remain the
accepted ACS-A starting hypothesis. Latest eligible year, title, publication
metadata, schema hashes, and content identity remain pending.

## F. Exact metric/variable registry

| source | metric ID | proposed variable | status |
|---|---|---|---|
| `census_acs1` | `census_acs1_pop_total` | `B01003_001E` | pending live metadata |
| `census_acs1` | `census_acs1_median_household_income` | `B19013_001E` | pending live metadata |
| `census_acs5` | `census_acs5_pop_total` | `B01003_001E` | pending live metadata |
| `census_acs5` | `census_acs5_median_household_income` | `B19013_001E` | pending live metadata |

No fifth variable, MOE, alias, or derived metric was admitted. Labels, concepts,
predicate/types, annotations, units, and cross-product equivalence are not frozen
until the four official group responses are captured.

## G. Exact ACS1 geography membership

**NOT ESTABLISHED.** The manifest has 233 enabled identities and legacy serving
has a 157-identity ACS1 union. These are repository observations, not a current
provider registry. Counts by nation/state/county/MSA/micro/division and every
identity classification remain pending live queries.

## H. Exact ACS5 geography membership

**NOT ESTABLISHED.** The manifest has 233 enabled identities and legacy serving
has a 223-identity ACS5 union. These are repository observations, not a current
provider registry. No completeness inference is made.

## I. Metropolitan Division findings

Repository identity and corrected concept are known; direct provider support is
not:

| code | canonical identity/name | concept | ACS1 | ACS5 |
|---|---|---|---|---|
| 11244 | `anaheim_ca_metro_area__cbsa_metro` / Anaheim, CA metro area | Metropolitan Division (314) | pending | pending |
| 15804 | `camden_nj_metro_area__cbsa_metro` / Camden, NJ metro area | Metropolitan Division (314) | pending | pending |
| 23224 | `frederick_md_metro_area__cbsa_metro` / Frederick, MD metro area | Metropolitan Division (314) | pending | pending |
| 31084 | `los_angeles_ca_metro_area__cbsa_metro` / Los Angeles, CA metro area | Metropolitan Division (314) | pending | pending |
| 35084 | `newark_nj_metro_area__cbsa_metro` / Newark, NJ metro area | Metropolitan Division (314) | pending | pending |
| 35154 | `new_brunswick_nj_metro_area__cbsa_metro` / New Brunswick, NJ metro area | Metropolitan Division (314) | pending | pending |
| 36084 | `oakland_ca_metro_area__cbsa_metro` / Oakland, CA metro area | Metropolitan Division (314) | pending | pending |
| 41884 | `san_francisco_ca_metro_area__cbsa_metro` / San Francisco, CA metro area | Metropolitan Division (314) | pending | pending |
| 42034 | `san_rafael_ca_metro_area__cbsa_metro` / San Rafael, CA metro area | Metropolitan Division (314) | pending | pending |
| 47894 | `washington_dc_metro_area__cbsa_metro` / Washington, DC metro area | Metropolitan Division (314) | pending | pending |

ACS-A is corrected concurrently: legacy absence resulted from using a CBSA query
clause for division codes and cannot establish provider non-support. Each product
must use and verify `for=metropolitan division:<code>`. No synthesis or silent
exclusion is permitted.

## J. Code 13720 finding

The manifest maps `13720` to
`big_stone_gap_va_metro_area__cbsa_metro` / Big Stone Gap, VA metro area. Legacy
ACS5 has it and ACS1 does not. Its selected-vintage concept/status and whether it
is historical or noncurrent remain **pending** official geography metadata and a
direct query; BPS history is not provider evidence.

## K. Release, vintage, and date semantics

The proposed `{survey_year}-12-31` period-end label remains unchanged but is not
newly provider-verified. It is distinct from publication date and cohort cycle.
ACS5 remains one five-year period estimate, never five annual facts. Final freeze
requires official estimate guidance and selected dataset metadata.

## L. Dynamic discovery proof

**NOT PROVEN LIVE.** Intended logic is to parse `data.json`, filter exact product
paths, apply cohort cutoff using verified publication metadata, select maximum
eligible survey year, and verify dataset/variables/groups/geography schema.
Resume/replay consume the pin and never rediscover. No discovery code was added
because no authoritative response could validate it.

## M. Proposed immutable pin schemas

Per-product semantic identity remains: source ID, product path, survey year,
credential-free API root, dataset/group/variable/geography hashes, exact canonical
membership and request plan, governed config hashes, member response hashes, and
aggregate governed-content hash. Product/year identifies the logical vintage;
hashes detect same-vintage revisions. Discovery/retrieval timestamps and runner
identity are non-semantic provenance. API keys are absent entirely. The schema is
not frozen until response shapes are verified separately for ACS1 and ACS5.

## N. Sentinel and missing-value contract

**NOT SAFE TO FREEZE.** A sentinel must never become an observation or zero, and
an unknown token must fail closed. Exact codes, annotations, nulls, and
`VALID_NUMERIC`/`OMIT_WITH_DIAGNOSTIC`/`FAIL_CANDIDATE` classifications require
official documentation plus observed responses. ACS-A's illustrative codes were
not promoted into code or registry.

## O. Verification acquisition diagnostics

No snapshot was acquired. Both products' vintage, requests, hashes, raw/canonical
counts, geography counts, date ranges, missing/sentinel counts, unsupported
membership, and schema anomalies are **pending**. No provider evidence was
written into the repository.

## P. Isolated canonicalization diagnostics

No live payload existed, so no physical contract was frozen. The seven columns
and proposed `property_type_id=all`, `property_type=all` remain pending. Runtime
code and DuckDB were untouched.

## Q. Legacy equivalence results

Live equivalence could not run. ACS-A baselines remain 12,888 serving rows (ACS1
5,800; ACS5 7,088) and 570 public compatibility rows. There are no defensible live
counts for exact/revision/provider-only/legacy-only/identity/taxonomy/coverage
classifications. ACS1 2020, historical eligibility, divisions, 13720, aliases,
and any post-2024 release remain explicit comparison requirements.

## R. Same-vintage revision proof

Not implemented because the canonical response/hash boundary is unverified. The
future fixture must hold product/year/plan constant, alter one governed response
value, and assert distinct aggregate hash, provider-release ID, and revision
identity. Shared revision policy was not modified.

## S. Physical boundary and downstream implications

The working proposal remains exactly two independent physical sources,
`census_acs1` and `census_acs5`, each owning discovery, pin, acquisition,
canonicalization, identity, and later publication. It is not formally frozen.
Existing ACS1-preferred/ACS5-fallback semantics mean a later phase should
investigate logical family resolution, but ACS-B neither designs nor implements
it.

## T. Open blockers and decisions

All live-dependent items remain blocked: releases/publication metadata,
authentication/quota, variable schemas, membership, level 314, code 13720,
sentinels, acquisition, canonicalization, equivalence, and revision fixture
inputs. Unexplained identity conflict or ambiguous division behavior remains a
NO-GO. No shared-lifecycle incompatibility has been observed.

## U. Recommended next scope

Do **not** begin ACS-C. First rerun ACS-B with Census access, capture official
bytes/hashes outside production state, implement/test pure provider logic,
acquire separate snapshots, and complete equivalence. Only after review may ACS-C
implement two physical adapters; publication, acceptance, Source Set, serving,
and logical-family work remain later authorizations.

## Exact local commands required before merge

Run from a clean checkout with internet access. This captures primary metadata and
the eleven pressure-test queries outside the repository; it does not mutate a DB:

```bash
set -euo pipefail
cd /path/to/realestate-intel
OUT="$(mktemp -d)/acs-b-official"; mkdir -p "$OUT"
curl --fail --silent --show-error --location \
  https://api.census.gov/data.json -o "$OUT/data.json"
sha256sum "$OUT/data.json" | tee "$OUT/SHA256SUMS"
python - "$OUT/data.json" >"$OUT/vintages.txt" <<'PY'
import json, sys
d = json.load(open(sys.argv[1], encoding="utf-8"))["dataset"]
for product, parts in (("acs/acs1", ["acs", "acs1"]),
                       ("acs/acs5", ["acs", "acs5"])):
    years = sorted({int(x["c_vintage"]) for x in d
                    if x.get("c_dataset") == parts and str(x.get("c_vintage", "")).isdigit()})
    if not years: raise SystemExit(f"no catalog vintages for {product}")
    print(product, years[-1])
PY
while read -r product year; do
  slug="${product//\//_}"
  base="https://api.census.gov/data/${year}/${product}"
  curl --fail --silent --show-error --location "${base}.json" \
    -o "$OUT/${year}_${slug}_dataset.json"
  sha256sum "$OUT/${year}_${slug}_dataset.json" | tee -a "$OUT/SHA256SUMS"
  for suffix in geography.json variables.json groups/B01003.json groups/B19013.json; do
    file="$OUT/${year}_${slug}_${suffix//\//_}"
    curl --fail --silent --show-error --location "${base}/${suffix}" -o "$file"
    sha256sum "$file" | tee -a "$OUT/SHA256SUMS"
  done
  for code in 11244 15804 23224 31084 35084 35154 36084 41884 42034 47894; do
    file="$OUT/${year}_${slug}_division_${code}.json"
    curl --fail --silent --show-error --get \
      "https://api.census.gov/data/${year}/${product}" \
      --data-urlencode 'get=NAME,B01003_001E,B19013_001E' \
      --data-urlencode "for=metropolitan division:${code}" -o "$file"
    sha256sum "$file" | tee -a "$OUT/SHA256SUMS"
  done
  file="$OUT/${year}_${slug}_metro_micro_13720.json"
  curl --fail --silent --show-error --get \
    "https://api.census.gov/data/${year}/${product}" \
    --data-urlencode 'get=NAME,B01003_001E,B19013_001E' \
    --data-urlencode 'for=metropolitan statistical area/micropolitan statistical area:13720' \
    -o "$file"
  sha256sum "$file" | tee -a "$OUT/SHA256SUMS"
done <"$OUT/vintages.txt"
sha256sum --check "$OUT/SHA256SUMS"
printf 'Official evidence captured outside repository: %s\n' "$OUT"
```

Provide the printed directory as review input and rerun ACS-B. Do not move it
under `data/`, `artifacts/`, or `config/`, and do not run legacy refresh jobs,
which mutate DuckDB. If anonymous calls receive a documented quota response,
inject `CENSUS_API_KEY` at runtime without printing it or appending it to saved
URLs/hash inputs.

## Commands executed in this run

```text
git status --short --branch
git log -3 --oneline
env | rg -i 'proxy|census|api.key'
curl -I --max-time 20 https://api.census.gov/data.json
python requests.get('https://api.census.gov/data.json', timeout=20)
```

The environment output diagnosed the proxy; no Census key was present. Failed
network probes did not contact workflows or mutate repository/production state.
