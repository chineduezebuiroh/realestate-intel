# Source Refresh / Revision Audit v0.1

**Status:** reconnaissance evidence; candidate for Source Refresh / Revision Contract v0.2.
**Runtime authority:** none. This document is not consumed by production.
**Evidence boundary:** repository code/configuration at commit `12e2d7f`; no provider web research, source refresh, or database access was performed.

## 1. Executive findings

1. `config/source_metric_registry.csv` is the authoritative current Macro Regime input inventory: 11 external source IDs plus three `derived` metrics. `census_bps_provisional` is not a direct registry source but is an operational governed support source because serving creates `fact_timeseries_bps` with final-over-provisional precedence. The audit universe is therefore 12 external source IDs.
2. Retrieval and truth semantics differ materially. CES/LAUS have bounded three-year "incremental" retrieval modes; FRED, BEA, NRC/FRED and BPS generally retrieve provider-current full histories/snapshots; ACS retrieves ten years in incremental mode or full configured history. None uses a provider-vintage ledger or ALFRED.
3. Current overlap behavior is generally newer-response-wins for staged canonical keys. Current absence behavior is inconsistent: key-upsert sources preserve omitted old keys; CES/LAUS incremental delete every old observation in the staged series/date envelope; CES full, BPS final, and BPS provisional delete broader source slices, making omission deletion.
4. Only Redfin v2 is candidate-first and explicitly transactional with rollback. Other sources perform separate `DELETE` and `INSERT` statements without explicit transactions and validate after mutation or not at all: **UNSAFE** for automated canonical publication.
5. Online acquisition is technically cloud-fetchable for most sources, but a clean Actions checkout cannot safely update the locally authoritative ignored `data/market.duckdb`. Several checked-in workflows are additionally nonfunctional: they reference absent `jobs.run_refresh_*` modules, while the incremental jobs mutate the tracked serving DB rather than the local full DB.
6. Small deterministic governance inputs (`geo_manifest.generated.csv`, BLS generated specs, registries and inline series maps) are tracked. ACS's query plan is generated into untracked `data/census/` and can be generated in CI. Provider payload CSV/ZIP files are mutable local intermediates, not governed vintages.
7. **Redfin latest-vintage preservation is PARTIALLY IMPLEMENTED.** A candidate combines only the immutable July baseline and the one target drop. It does not fold prior promoted drops or a normalized latest-vintage ledger. Once a retained raw drop is deleted, historical values unique to that vintage cannot be recovered from remaining governed raw inputs; the canonical DB may temporarily preserve them, but the next candidate rebuild can lose them. Coverage validation protects geo/metric presence, not every old canonical key.
8. No managed `data/redfin/raw/incoming/` behavior exists. Registration expects seven files already placed in `drops/<YYYY-MM>/`; it does not derive the month from endpoints, move/hash an inbox, or resolve duplicate/conflicting inbox vintages.
9. Publication-lag thresholds are absent for all sources: **GOVERNANCE REQUIRED**. Monthly availability checking is practical, but source-specific check mechanisms and blocking rules must be ratified.
10. Recommended architecture is **Option C: cloud-generated immutable normalized source artifacts plus local canonical assembly**, introduced incrementally. It best preserves Redfin's local boundary, avoids cloud mutation of an unavailable DB, supports revision evidence and rollback, and limits operator work. Option A is the safe interim.

## 2. Authoritative governed source inventory

Direct scope and metrics below come from `config/source_metric_registry.csv`; provider-to-geo selections come from `config/geo_manifest.generated.csv`. `property_type_id='all'` is used for all non-Redfin sources. The normalized fact key is `(geo_id, metric_id, date, property_type_id)`; `source_id` is lineage but is not part of the table primary key.

| Source ID | Family | Macro role | Governed geography | Governed metrics |
|---|---|---:|---|---|
| `redfin` | Redfin monthly market data | Direct | nation, state, metro, county, ZIP | 11 governed sale, listing, inventory, velocity and ratio metrics |
| `ces` | BLS Current Employment Statistics | Direct | state, metro | total nonfarm, total private, construction SA employment |
| `laus` | BLS Local Area Unemployment Statistics | Direct | nation, state, metro, county | labor force, employment, unemployment, unemployment rate NSA |
| `census_bps` | Census Building Permits Survey final compiled | Direct | state, county, place | total units/buildings/value, 1-unit, 5+-unit permits |
| `census_bps_provisional` | Census BPS current provisional | Supporting | configured provisional state/county/CBSA mappings | provisional counterparts used only where final keys are absent |
| `census_acs1` | Census ACS 1-year | Direct | nation, state, county, metro where API supports it | population, median household income |
| `census_acs5` | Census ACS 5-year | Direct | nation, state, county, metro | population, median household income |
| `fred_macro` | ordinary FRED observations | Direct | nation | mortgage rates, Fed funds, Treasury yields, spreads, CPI |
| `fred_unemp` | ordinary FRED observations | Direct | nation, state | unemployment rate SA |
| `bea_gdp_qtr` | BEA Regional quarterly GDP | Direct | nation, state | real GDP, chained 2017 dollars SAAR |
| `bea_gdp_ann` | BEA Regional annual GDP | Direct | nation, state, county | real GDP, chained 2017 dollars |
| `census_nrc_fred` | Census New Residential Construction distributed by FRED | Direct | nation, four Census regions | starts and completions SAAR |

`derived` registry rows (`price_to_income`, `payment_burden`, `permit_intensity`) are internal computations, not external refresh sources, and are outside this external-source contract.

## 3. Source Refresh / Revision Contract candidate matrix

All sources have proposed `check_cadence=monthly`; provider cadence remains distinct. "Unsafe" means production can be left partially mutated because there is no explicit transaction/candidate promotion around delete+insert+validation.

| Source | Provider cadence | Current acquisition | Current refresh depth | Revision risk | Current DB write / safety | Absence today | Recommended absence | Cloud fetch? | Clean Actions ready? | Main blocker | Recommended refresh model |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Redfin | monthly | seven manual full-history files | baseline + one target full snapshot | high | candidate + explicit transaction/rollback; SAFE | target omission falls back only to July baseline; post-July prior vintage not consulted | preserve latest governed vintage per key | No | NO | manual ignored raw files and local DB | local managed inbox; normalized latest-vintage ledger; new-release reconciliation |
| CES | monthly | BLS v2 timeseries API from generated spec | full ≈60 years; incremental 3 years | medium/high, external confirmation needed | full deletes all CES; incremental deletes staged series/window; UNSAFE | deletion inside selected slice/window | preserve prior unless explicit retraction | Yes | YES WITH CHANGES | canonical DB absent; workflow modules stale | monthly overlap refresh + annual/deep full reconciliation |
| LAUS | monthly | BLS API; bulk-file fallback | full 1976-current; incremental 3 years | medium/high | deletes all history or window for staged series; UNSAFE | deletion for staged series/window; wholly absent series preserved | preserve prior unless explicit retraction | Yes | YES WITH CHANGES | canonical DB absent; fallback/network and stale workflow | monthly overlap refresh + annual deep reconciliation |
| BPS final | monthly compiled release | discovers/downloads latest compiled ZIP | full compiled snapshot | medium | deletes entire `census_bps` slice; UNSAFE | omitted final keys disappear | preserve prior unless provider retraction; final supersedes provisional on overlap | Yes | YES WITH CHANGES | canonical DB absent; mutable raw intermediate | new-release full reconciliation via immutable normalized snapshot |
| BPS provisional | monthly current files | directory discovery, multiple text files | current provisional payload | medium/unknown suppression semantics | deletes entire provisional slice; UNSAFE | omitted provisional keys disappear | preserve prior provisional until final replacement or explicit retraction | Yes | UNKNOWN / WITH CHANGES | discovery is marked provisional; canonical DB absent | monthly immutable provisional snapshot/delta; final precedence |
| ACS 1-year | annual vintage | Census API per planned year/geo | incremental last 10 years; full configured history | medium; vintage semantics need verification | staged-key delete/insert; UNSAFE | unstaged keys preserved | preserve prior canonical year; archive release identity | Yes | YES WITH CHANGES | generated untracked query plan, secret, canonical DB absent | annual/new-release reconciliation, monthly metadata check |
| ACS 5-year | annual vintage | Census API per planned year/geo | incremental last 10 years; full since 2009 | medium | staged-key delete/insert; UNSAFE | unstaged keys preserved | preserve prior canonical year; archive release identity | Yes | YES WITH CHANGES | same as ACS1 | annual/new-release reconciliation, monthly metadata check |
| FRED macro | mixed daily/weekly/monthly | `fredapi.get_series`, then monthly aggregation | full available ordinary series | medium/high | staged-key delete/insert; UNSAFE | unstaged keys preserved | preserve prior; newest observation response wins | Yes | YES WITH CHANGES | FRED secret; no vintage identity; canonical DB absent | monthly full ordinary-series reconciliation + normalized vintage evidence |
| FRED unemployment | monthly | `fredapi.get_series` | full available ordinary series | medium/high | staged-key delete/insert; UNSAFE | unstaged keys preserved | preserve prior | Yes | YES WITH CHANGES | FRED secret; no vintage identity; canonical DB absent | monthly full ordinary-series reconciliation |
| BEA quarterly GDP | quarterly | Regional API `SQGDP9`, `Year=ALL` | all provider-returned years | high conceptually; provider evidence required | staged-key delete/insert; UNSAFE | unstaged keys preserved | preserve prior; newer BEA response wins | Yes | YES WITH CHANGES | BEA secret; canonical DB absent | new-release full reconciliation; archive normalized release |
| BEA annual GDP | annual | Regional API `CAGDP9`, `Year=ALL` | all provider-returned years | high conceptually; provider evidence required | staged-key delete/insert; UNSAFE | unstaged keys preserved | preserve prior | Yes | YES WITH CHANGES | BEA secret; canonical DB absent | new-release full reconciliation; annual deep audit |
| Census NRC/FRED | monthly | unauthenticated `fredgraph.csv` for ten series | full available ordinary series | medium | staged-key delete/insert; UNSAFE | unstaged keys preserved | preserve prior | Yes | YES WITH CHANGES | canonical DB absent; no vintage identity/validator | monthly full series reconciliation + normalized release evidence |

**Confidence:** HIGH for current repository mechanics; MEDIUM for CES/LAUS/FRED/BEA/NRC recommended revision periods; LOW for provisional BPS omission/suppression meaning until provider documentation is reviewed.

## 4. Cloud dependency matrix

| Source | API/network accessible from GitHub | Secret required | Required tracked config | Required untracked/local file | Required generated file | Can generate in CI? | Should commit anything? | Clean-run status |
|---|---|---|---|---|---|---|---|---|
| Redfin | No governed downloader | none | baseline/domain manifests, geo manifest | seven raw files + immutable baseline under ignored raw root; local full DB | candidate/report | only after operator handoff, not provider fetch | No raw data; keep local by design | NO |
| CES | Yes | `BLS_API_KEY` optional but configured | geo manifest + tracked `ces_series.generated.csv` | none for ingest; `config/bls/*` only for regeneration | generated spec | Yes, downloads BLS metadata | Existing tracked spec is appropriate | fetch YES; canonical update NO |
| LAUS | Yes | `BLS_API_KEY` optional | geo manifest + tracked LAUS specs | `config/bls/*` downloaded on regeneration/fallback | generated spec and BLS lookup/data cache | Yes | Keep small generated spec tracked; generate cache | fetch YES; canonical update NO |
| BPS final | Yes | none | geo manifest | downloaded ZIP/CSV in `data/census` | normalized timeseries CSV | Yes | No raw payload commit | fetch YES; canonical update NO |
| BPS provisional | Yes | none | geo manifest + inline level schema | downloaded text/CSV in `data/census` | normalized timeseries CSV | Yes if discovery remains valid | No raw payload commit | UNKNOWN/WITH CHANGES |
| ACS1/5 | Yes | `CENSUS_API_KEY` required by code | geo manifest + inline variables | none before generation | query plan and raw CSV in `data/census` | Yes | Do not commit date-derived plan; generate and archive manifest with release | fetch YES; canonical update NO |
| FRED macro | Yes | `FRED_API_KEY`; missing key silently skips | geo manifest + inline series registry | none | none | N/A | Inline series map should eventually become tracked declarative config | fetch YES; canonical update NO |
| FRED unemployment | Yes | `FRED_API_KEY`; missing key silently skips | geo manifest series IDs | none | none | N/A | No additional data file | fetch YES; canonical update NO |
| BEA quarterly/annual | Yes | `BEA_API_KEY` or `BEA_API_USER_ID` | geo manifest + inline table/line codes | generated raw CSV | normalized raw CSV | Yes | Inline BEA query definition should eventually be declarative tracked config | fetch YES; canonical update NO |
| Census NRC/FRED | Yes | none | inline ten-series list | generated raw CSV | raw long CSV | Yes | Series list is small/tracked in code; declarative config desirable | fetch YES; canonical update NO |

A clean runner can fetch and normalize most online sources. It cannot safely update the canonical architecture because `data/market.duckdb` is ignored/unavailable, no governed prior source state is acquired, and workflows do not publish source artifacts or a canonical DB. The tracked `data/market_serving.duckdb` is a derived snapshot and is not an acceptable mutation target for canonical refresh.

## 5. Cross-source architectural findings

### 5.1 Retrieval is not truth policy

* CES/LAUS incremental retrieval intentionally overlaps three years, but delete-by-window means retrieval omission becomes canonical deletion. That coupling violates provisional `absence_semantics=preserve_prior`.
* Full provider retrieval in FRED, BEA and NRC/FRED is followed by key-only upsert. Therefore a missing provider key is preserved accidentally, not through explicit retraction governance.
* BPS full-snapshot retrieval deletes the source slice, so snapshot omission is treated as deletion without proof.
* ACS annual API years are observations labeled by year; the raw `vintage` column records the plan's selected latest year but no provider release ID/vintage ledger is stored in facts.

### 5.2 Canonical key and collision boundary

All transforms normalize to `(geo_id, metric_id, date, property_type_id)`. The table PK omits `source_id`. Several key-based deletes explicitly delete regardless of source; this is safe only if metric IDs have exclusive source ownership. V0.2 should state that registry ownership is mandatory and should validate it before mutation.

### 5.3 Availability checks and publication lag

| Source | Current availability detection | Cheapest practical monthly check | Lag governance |
|---|---|---|---|
| Redfin | existence of registered target metadata only | future managed-inbox endpoint inspection | GOVERNANCE REQUIRED |
| CES/LAUS | none; refresh request itself | latest-period BLS query or small overlap fetch; provider metadata needs verification | GOVERNANCE REQUIRED |
| BPS final | HTML directory discovery identifies latest `YYYYMM` ZIP | existing discovery without download | GOVERNANCE REQUIRED |
| BPS provisional | HTML discovery of latest per-level `YYMM` files | existing discovery, but reliability unknown | GOVERNANCE REQUIRED |
| ACS1/5 | plan uses `today.year - 2`; no API release probe | dataset metadata/latest available year | GOVERNANCE REQUIRED |
| FRED macro/unemployment | none; full `get_series` | last-observation query if library/API supports bounded request; otherwise refresh is check | GOVERNANCE REQUIRED |
| BEA quarterly/annual | none | small latest-year query or release metadata if supported | GOVERNANCE REQUIRED |
| NRC/FRED | none | download ten ordinary CSVs or bounded last observation | GOVERNANCE REQUIRED |

Monthly checks are suitable for every source. `no_new_release_expected` is correct for ACS/BEA between releases; it must depend on governed release metadata/lag policy, not comparison to a universal latest month.

### 5.4 Raw and provider-vintage evidence

* **Redfin:** raw baseline and three recent drops plus metadata hashes are retained locally; metadata history survives longer. Candidate truth is not reconstructible across arbitrary post-baseline vintages after raw retention.
* **BLS:** API JSON is not archived. CES/LAUS generated specs are tracked, while downloaded BLS metadata/cache under `config/bls` is ephemeral. Canonical history cannot be reconstructed by provider vintage.
* **BPS:** latest ZIP/raw CSV/normalized CSV paths are overwritten; there is no immutable release archive.
* **ACS:** raw CSV is overwritten; `vintage` is a plan field, not a retained API response/release identity.
* **FRED:** no raw archive and no realtime/vintage fields. Ordinary current observations are all that survives in the DB.
* **BEA:** current raw annual/quarterly CSVs are overwritten; table/line metadata is retained in rows but release identity is not.
* **NRC/FRED:** one raw long CSV is overwritten; no FRED/Census vintage identity.

Recommended evidence is source-appropriate: immutable normalized snapshots plus request/release metadata and hashes for BLS/FRED/NRC; normalized release snapshots for BPS/BEA/ACS; Redfin needs a compact latest-vintage-per-key ledger or durable normalized deltas in addition to existing raw retention. Full wire-payload retention is not universally necessary.

### 5.5 Transaction and rollback classification

* **SAFE:** Redfin only—validated Parquet candidate, explicit `BEGIN`, source delete/insert/postchecks, `COMMIT`, and exception `ROLLBACK` in `sources/redfin/transform.py::apply_candidate`.
* **UNSAFE:** CES, LAUS, BPS final/provisional, ACS1/5, FRED macro/unemployment, BEA annual/quarterly, NRC/FRED. Each uses separate delete/insert statements without explicit transaction/candidate promotion. API fetch generally precedes connection/mutation, which limits network-failure exposure, but insertion, schema, disk, or validation failure can leave committed deletion/partial state. Validation typically occurs after mutation; BPS/BEA/NRC have no equivalent comprehensive post-refresh validator.
* No current online-source job offers rollback to an immutable prior source artifact.

### 5.6 Current incremental paths and workflow reality

`jobs/incremental_refresh/` exists. CES and LAUS truly use a bounded three-year API overlap. ACS defaults to ten years. FRED/BEA/NRC jobs are named incremental but fetch full provider histories and key-upsert them. These jobs target `SERVING_DB_PATH`, not the authoritative full DB. The CES/LAUS workflows call these real modules; other scheduled workflows call absent modules (`jobs.run_refresh_bls`, `jobs.run_refresh_census`, `jobs.run_refresh_macro`, `jobs.run_refresh_all_hosted` dependencies), so clean Actions readiness is not established. This is audit evidence only; workflow changes are outside this task.

## 6. Detailed source audits: current behavior and recommended governed policy

### 6.1 Redfin (`redfin`) — confidence HIGH for repository mechanics

**Current repository behavior.** Provider acquisition is manual. `register_drop` requires exactly seven already-placed raw files under `data/redfin/raw/drops/<drop_id>`, infers one family per filename, hashes them and rejects conflicting re-registration. `build_candidate` reads the immutable July baseline plus only the requested drop, maps provider IDs through the tracked generated geo manifest, normalizes exactly 11 metrics and selects target-drop values over baseline on canonical-key overlap. Candidate validation enforces exact metric/geography/latest-month/floor contracts and rejects loss of a previously present **geo/metric pair**, but reports rather than rejects old individual keys. Transactional apply replaces the complete Redfin source slice with the candidate.

**Revision/absence/archive conclusion.** New target values win over baseline. Missing target keys fall back to baseline only. Prior August value is not considered when building September. Retention deletes raw drops beyond the newest three after archiving metadata, not normalized values. Therefore latest-vintage-per-key survival after raw deletion is **PARTIALLY IMPLEMENTED**, not complete.

**Recommended policy.** Monthly manual availability check; new-release full reconciliation; newest governed vintage containing a key wins; absence preserves the latest governed value; explicit retraction alone deletes. Add a durable normalized per-key vintage ledger/delta archive before reducing raw retention. Redfin remains publication-blocking/waiting when the target is absent or invalid. Provider/archive/inbox are local by design. No `incoming/` automation exists: **managed inbox gap NOT IMPLEMENTED**.

**Open questions.** Storage size and compaction policy for normalized vintage deltas; authoritative way to infer source month from all seven exports. External verification required: export window behavior and legal/operational download constraints.

### 6.2 BLS CES (`ces`) — confidence HIGH current / MEDIUM provider revision policy

**Current.** `expand_spec.py` downloads `sm.series` and AllData, selects active governed state/metro series and writes tracked `config/ces_series.generated.csv`. Ingest requires that spec, posts up to 50 series to BLS v2, uses optional `BLS_API_KEY`, fetches ≈60 years in full mode or current year minus three in incremental mode, drops M13 when monthly rows exist, maps series to governed geo/metric, then mutates DuckDB. Full mode deletes all `source_id='ces'`; incremental deletes every CES row for staged geo/metric/property keys between global staged min/max; insertion follows without explicit transaction. Validator checks expected generated-series coverage and continuity after mutation.

**Overlap/absence.** Overlap response wins. Full omission deletes history; incremental omission inside a staged series/window deletes it; a wholly unstaged key survives only in incremental mode. No raw API/vintage archive exists.

**Recommended.** Monthly latest/overlap check, preserve prior absent keys, block publication if a detected required release cannot be ingested/validated, warning on transient availability-check failure within governed lag, annual deep full-history reconciliation, immutable normalized response snapshot plus request/release metadata. A BLS key should be a GitHub Secret if used. Spec is small/nonsecret/deterministic and correctly tracked; regeneration inputs may be fetched in CI.

### 6.3 BLS LAUS (`laus`) — confidence HIGH current / MEDIUM provider revision policy

**Current.** Generated tracked spec derives from BLS `la.*` lookup files and the geo manifest. API retrieval uses full 1976-current or three-year incremental windows. `fetch_series_any` may fall back to BLS bulk files for missing/short series; state fallback is intentionally constrained after a windowed API call. Full mutation deletes all dates only for series keys present in staging (not the complete LAUS source); incremental deletes their staged date envelope. No explicit transaction; validator runs afterward.

**Overlap/absence.** Response wins for overlap. Missing observations within a staged series range are deleted; completely missing series are preserved. No provider-vintage archive.

**Recommended.** Preserve absent canonical keys; monthly overlap check; block publication on detected-release ingest/validation failure; annual deep reconciliation to expose revisions and series remaps; archive normalized response and selected-series identity. Generated spec remains tracked; ephemeral bulk cache should be generated, not committed.

### 6.4 Census BPS final (`census_bps`) — confidence HIGH current / MEDIUM revision policy

**Current.** HTML discovery selects latest `BPS Compiled_YYYYMM.zip`; ingest downloads it, reads the first CSV, normalizes aliases/period/geography, chooses latest `survey_date` for duplicate observational keys, maps the geo manifest and overwrites raw and normalized CSVs in `data/census`. Transform deduplicates, then deletes the entire final source slice and inserts the compiled snapshot without explicit transaction. `fact_timeseries_bps` ranks final source over provisional for identical canonical keys.

**Overlap/absence.** Full snapshot values replace overlap; omitted final keys disappear. In the merged view a remaining provisional row can fill a missing final key, but final-source deletion is still not proof of provider retraction.

**Recommended.** Existing cheap directory availability check; on new compiled release, immutable normalized full snapshot, preserve prior absent final keys unless retracted, then final-over-provisional precedence. Block publication on detected final-release failure; allow `no_new_release_expected` within governed lag. Reconcile on each new compiled release. Verify provider suppression/deletion semantics externally.

### 6.5 Census BPS provisional (`census_bps_provisional`) — confidence HIGH current / LOW provider semantics

**Current.** Conservative HTML discovery searches separate current state/county/CBSA text files, explicitly says discovery/schema still need confirmation, downloads and concatenates them, and overwrites raw/normalized CSV. Transform deletes the entire provisional slice then inserts. Serving view uses provisional only when no final canonical key exists.

**Recommended.** Preserve missing provisional values until replaced by final or explicitly retracted; archive small normalized monthly snapshots/deltas with discovered URLs/hashes; monthly check; a provisional failure should generally `continue_with_warning` if final coverage remains within governed freshness, but block publication when a required recent BPS signal has neither acceptable final nor provisional evidence. Provider file/suppression semantics are priority external verification.

### 6.6 Census ACS 1-year (`census_acs1`) — confidence HIGH current / MEDIUM vintage policy

**Current.** `expand_spec.py` derives an untracked query plan from the tracked geo manifest using `today.year - 2` as "stable enough" vintage and includes both ACS datasets. Code requires `CENSUS_API_KEY`. Full mode requests every configured year; default incremental requests the latest ten years through plan vintage. API values are normalized to year-end observations; raw CSV includes plan `vintage` but transform persists only canonical fact values. Key-based delete/insert replaces returned years; absent years/keys remain. No explicit transaction; validator checks dates, duplicates and nonempty source counts after mutation.

**Recommended.** Treat observation year and provider release/vintage identity separately. Monthly metadata check; refresh only on new annual release; preserve absent prior years; archive normalized release plus query plan/request identity; new-release full governed-year reconciliation (not blind monthly rerun). New-release ingest failure blocks publication once governed lag is exceeded. Generate plan in CI/local execution; do not commit date-derived plan.

### 6.7 Census ACS 5-year (`census_acs5`) — confidence HIGH current / MEDIUM vintage policy

Current mechanics match ACS1 except full history begins in 2009 and 5-year estimates cover broader governed geographies. Recommended policy matches ACS1, with separate release identity and coverage rules; do not replace an older observation year merely because a newer annual vintage exists unless the provider actually republishes that observation key. External Census vintage/revision documentation is required.

### 6.8 FRED macro (`fred_macro`) — confidence HIGH current / MEDIUM provider semantics

**Current.** Uses `fredapi.Fred.get_series(series_id)` with no `observation_start`, `realtime_start`, `realtime_end`, release endpoint, or ALFRED call. It retrieves ordinary current full series, converts/aggregates to month-end, derives physical spreads, maps nation through the geo manifest, and key-upserts. Missing `FRED_API_KEY` logs a skip and returns success—unsafe as a production readiness signal. No raw/vintage archive or source validator specific to this job.

**Overlap/absence.** Current FRED observations replace overlap; omitted keys remain. Historical revisions exposed by the ordinary current response will overwrite old values, but publication-vintage chronology cannot be reconstructed.

**Recommended.** Monthly full current-series reconciliation is technically modest; preserve absent prior keys; store normalized response, request timestamp, series IDs and hashes. Block publication if a required new release is detected but ingest fails; missing secret must be failure, not success, in a future governed job. Quarterly/annual deeper vintage comparison may be needed, pending official FRED/ALFRED semantics. FRED key belongs in GitHub Secrets.

### 6.9 FRED unemployment (`fred_unemp`) — confidence HIGH current / MEDIUM provider semantics

Current behavior mirrors FRED macro: ordinary full `get_series`, geo-specific series IDs from tracked manifest, key-upsert, no vintage fields/archive, and silent success when key is missing. Monthly full reconciliation and preserve-prior are recommended; new-release failure should block publication for required labor inputs. External confirmation is needed for series-specific revision/seasonal-adjustment behavior.

### 6.10 BEA quarterly GDP (`bea_gdp_qtr`) — confidence HIGH current / MEDIUM revision policy

**Current.** Authenticated BEA Regional API query uses table `SQGDP9`, line code 1, `Year=ALL`, governed geo FIPS from the manifest, and writes an overwritten normalized raw CSV. Transform key-upserts without transaction. No availability/release check, vintage ID, retained response, or dedicated validator. The "incremental" job actually fetches all years and targets the serving DB.

**Recommended.** Monthly metadata/latest-period check; refresh and reconcile all returned history on each quarterly release; newer response wins on overlap, absence preserves prior unless retracted; archive compact normalized release with request/table/line/release identity. Detected-release failure blocks publication after governed lag. BEA key belongs in GitHub Secrets. Benchmark revision mechanics require official verification.

### 6.11 BEA annual GDP (`bea_gdp_ann`) — confidence HIGH current / MEDIUM revision policy

Same operational path as quarterly using `CAGDP9`, line code 1 and `Year=ALL`, with annual/county scope. Recommend monthly availability check, new-release full reconciliation, annual deep audit, preserve-prior absence and normalized release archive. Unchanged annual data is `no_new_release_expected`, not stale, until a governed lag is exceeded.

### 6.12 Census NRC via FRED (`census_nrc_fred`) — confidence HIGH current / MEDIUM provider semantics

**Current.** Operationally this is ten ordinary FRED `fredgraph.csv` series—five starts and five completions—whose original subject/provider is Census. Endpoint requires no key and returns full current histories. Series/geographies are inline tracked code. Ingest overwrites `data/census/nrc_fred_raw_long.csv`; transform key-upserts without transaction or dedicated validation.

**Recommended.** Keep distinct source identity and record both original subject and distribution channel. Monthly full reconciliation is cheap; preserve absent keys; archive normalized series snapshot/request timestamp/hash. Detected-release ingest failure should block publication when NRC is required and stale beyond governed lag. Verify whether FRED observations are revised with Census releases and whether release metadata can cheaply identify change.

## 7. Monthly blocking candidate

| Condition | Manual Redfin | Required monthly/quarterly inputs | Annual/slower inputs | BPS provisional |
|---|---|---|---|---|
| Availability check fails transiently | `waiting` if readiness unknown | `continue_with_warning` only within governed lag | `continue_with_warning` | `continue_with_warning` if final coverage acceptable |
| Provider unavailable, no release expected | waiting until operator state known | `no_new_release_expected` only if supported by release calendar | `no_new_release_expected` | continue with final |
| New release detected; ingest/validation fails | `block_analytics` | `block_publication` (and analytics if metric contract requires new target) | `block_publication` once release is required | warning or block depending on final coverage |
| Source stale beyond governed lag | `block_analytics` | `block_publication` | `block_publication` | block if combined BPS view is stale |
| Valid release unchanged | continue | continue | continue | continue |

Exact required/stale lags are **GOVERNANCE REQUIRED** for every source before v0.2 becomes runtime policy.

## 8. Central `market.duckdb` architecture options

| Option | Reproducibility | Complexity/storage | Redfin integration | Revision/failure safety | Operator burden | Assessment |
|---|---|---|---|---|---|---|
| A. Local-authoritative DB; all fetches local | moderate; depends on local evidence retention | lowest near-term | native | weak until online sources are candidate-hardened | highest recurring | safe interim, not ideal end-state |
| B. GitHub-built canonical DB | high if full inputs are artifacted | high; DB artifact transfer/limits | requires uploading governed Redfin inputs, contradicting current boundary | potentially strong | lower after handoff, but handoff is substantial | not recommended now |
| C. GitHub source artifacts + local assembly | high per-source identities; modular reruns | medium; small Parquet artifacts | Redfin remains local | strong candidate validation and rollback boundary possible | moderate, automatable download | **recommended target** |
| D. Local source-artifact builds + canonical assembly | high if same contracts used | medium | native | strong, no cloud dependency | moderate/high | useful fallback and development parity for C |

**Recommendation.** Retain Option A until source contracts and candidate writers exist, then move cloud-capable acquisition to Option C. GitHub should publish immutable, checksummed, normalized source snapshots/deltas and release metadata—not mutate or publish the whole canonical DuckDB. The local orchestrator should verify artifacts, combine them with the governed Redfin candidate, build a complete candidate `market.duckdb`, validate cross-source contracts, and atomically promote. This minimizes cloud storage, isolates source failures, preserves revision evidence, and respects manual Redfin. GitHub artifact retention limits favor governed Releases or equivalent durable immutable storage over ephemeral Actions artifacts.

## 9. File/state governance recommendations

### TRACKED AND CORRECT

* `config/source_metric_registry.csv` and analytical registries: small, nonsecret governance authority.
* `config/geo_manifest.generated.csv`: ~41 KB, tracked, deterministic governed mapping used by every major source.
* `config/ces_series.generated.csv` (~69 KB), `config/laus_series.generated.csv` (~105 KB), and `config/laus_series.csv` (~7 KB): small, nonsecret selected series specifications. Keep tracked, but record generator/input hashes and review diffs on regeneration.
* `config/redfin_baseline_manifest.json` and `config/redfin_metric_domain_contract.json`: small immutable governance metadata; raw baseline remains local.
* Inline FRED, BEA and NRC series/table maps are tracked and reproducible, though declarative config would improve reviewability in a later implementation task.

### SHOULD BE GENERATED IN CI / CONTROLLED LOCAL RUN

* `data/census/census_acs5_query_plan.generated.csv`: deterministic from tracked geo manifest plus resolved target vintage; generation must record the resolved vintage and code SHA. Do not commit a date-derived plan silently.
* `config/bls/*` provider lookup/cache files: generated from BLS downloads; cache is not governance authority. Generate during explicit spec refresh and compare resulting tracked specs.
* Mutable `data/census/*`, `data/bea/*` and NRC raw/normalized intermediates: generate in isolated candidate workspaces, not as implicit prerequisites.

### SHOULD BECOME GOVERNED RELEASE ARTIFACT

* Immutable normalized source release snapshots/deltas for cloud-capable BLS, Census, FRED, BEA and NRC sources, with schema, provider/request/release identity, source URLs, retrieval time, row count, canonical-key inventory and SHA-256.
* For Redfin, normalized per-key vintage deltas/ledger if accepted after contract design; this supplements rather than replaces current local raw governance.

### SHOULD REMAIN LOCAL

* `data/redfin/raw/baseline/`, `drops/`, quarantine and future operator inbox: manual/large/provider-dependent by design.
* `data/market.duckdb` and candidate/live serving DBs under the current local-authoritative architecture.
* Credentials in `.env` locally or GitHub Secrets in cloud; never tracked.

### OBSOLETE / CONTRADICTORY

* Legacy tracked `data/raw/redfin/*` files and `schedule.yml` Redfin/Zillow Make targets represent pre-v2 behavior and are not current Redfin authority. Removal/deprecation requires a separate governed cleanup task.
* Workflows calling absent `jobs.run_refresh_*` modules are nonfunctional current infrastructure, not evidence that clean hosted refresh works.

### UNKNOWN

* Whether BPS provisional file schemas/discovery have stabilized enough to turn inline provisional mappings into governed configuration.
* Whether provider licensing/terms permit durable raw-wire archives; normalized evidence is the conservative candidate pending verification.

No newly discovered small required governance file needs to be committed in this audit. The important generated BLS and geo specs are already tracked. Future declarative series/release configs should be proposed only with a reviewed update mechanism.

## 10. External verification queue

| Priority | Source | Question | Why repository is insufficient | Evidence needed | Contract effect |
|---:|---|---|---|---|---|
| P0 | BPS provisional | Does omission mean suppression, correction, or retraction; are directory/file schemas stable? | code labels discovery/schema provisional | official file layout/data dictionary/revision policy | absence and blocking policy |
| P0 | Redfin | Do exports use rolling historical windows and how is common latest month encoded across seven families? | code only observes downloaded files | provider export documentation/sample vintages | inbox derivation and latest-vintage ledger |
| P1 | BLS CES | Official revision windows, benchmark revisions and latest-release metadata endpoint | ordinary API response has no governed release ID | BLS revision/release/API docs | overlap depth and deep reconciliation |
| P1 | BLS LAUS | Annual revision/re-estimation behavior and series replacement/retraction semantics | fallback/remap code is heuristic | BLS LAUS revision/series docs | reconciliation and absence rules |
| P1 | FRED/ALFRED | What revisions ordinary `get_series`/fredgraph expose; bounded/latest and vintage endpoints | code does not use realtime fields | official FRED/ALFRED API docs | archive and availability design |
| P1 | BEA | Regional SQGDP9/CAGDP9 release/benchmark revision guarantees and metadata APIs | `Year=ALL` proves retrieval, not release semantics | BEA Regional API/release docs | reconciliation depth and availability |
| P1 | ACS1/5 | Whether historical API-year responses can change after release and how releases/vintages are identified | plan `vintage` is locally derived | Census ACS API vintage/revision docs | immutable-year vs replace-on-overlap rule |
| P2 | Census NRC | Revision cadence and relationship between FRED series and Census release vintages | FRED transport hides original release identity | Census/FRED release docs | archive and availability policy |
| P2 | All | Acceptable publication-lag thresholds by source/metric | no repository policy | owner-approved calendars and operational SLA | stale/blocking decisions |
| P2 | All | Raw payload retention permissions and duration | repository contains no licensing review | provider terms and storage policy | raw vs normalized archive choice |

No web research was performed, per task instruction.

## 11. Recommended v0.2 decisions

1. Ratify `check_cadence=monthly` separately from provider cadence.
2. Ratify canonical overlap as newest **governed** release wins and default `absence_semantics=preserve_prior`; require explicit source-specific retraction evidence for deletion.
3. Make provider release identity/request identity mandatory, even when only retrieval time + content hash is available.
4. Define registry-exclusive canonical metric ownership because `source_id` is absent from the fact PK.
5. Require candidate-first mutation, validation and atomic/transactional promotion before any source enters automated canonical assembly.
6. Govern per-source availability method, expected lag, required-vs-advisory status, and block policy.
7. Treat missing credentials as failure for required sources; never silent-success.
8. Adopt final-over-provisional BPS precedence while preserving absent prior values independently within each source lineage.
9. Add Redfin latest-vintage-per-key persistence before raw retention can remove the only supplying drop; retain accepted v2 lifecycle otherwise unchanged.
10. Select Option C as target and Option A as interim; define immutable normalized source artifact schema, retention and checksum contract.
11. Do not mutate the tracked serving DB from incremental source jobs; serving remains a derived product of canonical assembly.
12. Separate availability probes from refresh, but permit "refresh is cheapest check" where payloads are small and safe candidate acquisition is idempotent.

## 12. Implementation priorities after v0.2 approval (not implemented here)

1. Approve provider verification queue and publication-lag matrix.
2. Specify normalized source artifact and release-manifest schemas.
3. Close the Redfin per-key vintage gap and separately design the managed inbox.
4. Build read-only availability probes.
5. Candidate-harden source transforms one family at a time, beginning with fatal monthly Macro inputs.
6. Establish cloud source-artifact publishing and local verified acquisition.
7. Build candidate canonical DB assembly and cross-source validation.
8. Deprecate contradictory workflows/legacy paths only after replacements pass governed acceptance.

## 13. Audit method and evidence index

Non-destructive reconnaissance inspected:

* `config/source_metric_registry.csv`, `config/geo_manifest.generated.csv`, BLS specs, Redfin contracts and `.gitignore`;
* every current module under `sources/redfin`, `sources/bls_ces`, `sources/bls_laus`, `sources/census_bps`, `sources/census_bps_provisional`, `sources/census_acs`, `sources/fred_macro`, `sources/fred_unemp`, `sources/bea`, and `sources/census_nrc_fred`;
* `jobs/full_refresh`, `jobs/incremental_refresh`, `jobs/provisional_refresh`, aggregate job references and `Makefile`;
* all refresh/schedule workflows, serving source precedence, regime config loading, and source-validation surfaces;
* global searches for delete/transaction, incremental/watermark, local paths, credentials, generated/raw files and provider endpoints.

No source code, workflow, registry, runtime contract, database, raw file, or generated artifact was modified. These two documentation/governance-candidate files are the sole repository changes.
