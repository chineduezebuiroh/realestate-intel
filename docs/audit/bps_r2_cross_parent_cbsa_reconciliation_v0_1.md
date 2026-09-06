# Immutable r2 BPS cross-parent CBSA reconciliation

**Decision: PASS — family-resolution design may begin, but this audit does not implement or authorize resolution, publication, or acceptance.**

## Authority and bounded method

This read-only audit uses the two already-published immutable physical candidates, not provider discovery or a current release:

* compiled: `src__census_bps__2026-04__r2__993afaddb934ce4f`; full artifact-content SHA-256 `993afaddb934ce4f8ea40e14a8e29ce63ddb6c1c743ba1e976b796b185dced4e`; package SHA-256 `2c64d65d784dd0447cd10273631b9f3f7c1cfa52d159031eab5cbdd8a4e41620`; `data.parquet` SHA-256 `a2f643923b73c6707e4cce71c5b1016e76e592bbde1c52bf532672b0ea4f4c1e`.
* provisional: `src__census_bps_provisional__2026-07__r2__61c56540953237cb`; full artifact-content SHA-256 `61c56540953237cb72cc2fec062e9aeb092de411153cd78a250994254004f7ab`; package SHA-256 `7376bc3fb41ec7a8e20a976ca5e235de285e3725ad63d98084af5cd42b3bfb88`; `data.parquet` SHA-256 `6a60284f00e927e27a1d2ec836707b4c6d8bc04254abcef4b41dd8821819e942`.

The cataloged packages were resolved by their exact immutable identities and validated against their catalog/package/member hashes. For each validated `data.parquet`, physical presence was computed as distinct canonical `geo_id` values ending in `__cbsa_metro`; no registry flag or earlier diagnostic supplied physical membership. The comparison universe is the 53 rows classified `compatible` in `config/bps_cbsa_canonical_concepts_v1.csv`, SHA-256 `76007778d36e44e00a7ac83310761ac99175c5ef7d97da7a29cff9c75d88b03c`. The corresponding governed-geography contract hash is `077770e32a1a26583c269d6bc4cc31890fe2994cc17c54053a388cb9b70a8b45`.

## Exact result

| Set | Count |
|---|---:|
| Compiled physical | 42 |
| Provisional physical | 50 |
| Shared | 41 |
| Compiled only | 1 |
| Provisional only | 9 |
| Absent from both | 2 |
| Union | 51 |
| Compiled missing from governed 53 | 11 |
| Provisional missing from governed 53 | 3 |

The required arithmetic passes explicitly: `41 + 1 = 42`; `41 + 9 = 50`; `41 + 1 + 9 = 51`; and `51 + 2 = 53`.

## Exact inventories

### Shared / intersection (41)

* `12100` — `atlantic_city_nj_metro_area__cbsa_metro` (Atlantic City, NJ metro area)
* `12540` — `bakersfield_ca_metro_area__cbsa_metro` (Bakersfield, CA metro area)
* `12580` — `baltimore_md_metro_area__cbsa_metro` (Baltimore, MD metro area)
* `13980` — `blacksburg_va_metro_area__cbsa_metro` (Blacksburg, VA metro area)
* `16820` — `charlottesville_va_metro_area__cbsa_metro` (Charlottesville, VA metro area)
* `17020` — `chico_ca_metro_area__cbsa_metro` (Chico, CA metro area)
* `19060` — `cumberland_md_metro_area__cbsa_metro` (Cumberland, MD metro area)
* `19260` — `danville_va_metro_area__cbsa_metro` (Danville, VA metro area)
* `20940` — `el_centro_ca_metro_area__cbsa_metro` (El Centro, CA metro area)
* `21700` — `eureka_ca_metro_area__cbsa_metro` (Eureka, CA metro area)
* `23420` — `fresno_ca_metro_area__cbsa_metro` (Fresno, CA metro area)
* `25180` — `hagerstown_md_metro_area__cbsa_metro` (Hagerstown, MD metro area)
* `25260` — `hanford_ca_metro_area__cbsa_metro` (Hanford, CA metro area)
* `25500` — `harrisonburg_va_metro_area__cbsa_metro` (Harrisonburg, VA metro area)
* `31340` — `lynchburg_va_metro_area__cbsa_metro` (Lynchburg, VA metro area)
* `32300` — `martinsville_va_metro_area__cbsa_metro` (Martinsville, VA metro area)
* `32900` — `merced_ca_metro_area__cbsa_metro` (Merced, CA metro area)
* `33700` — `modesto_ca_metro_area__cbsa_metro` (Modesto, CA metro area)
* `34900` — `napa_ca_metro_area__cbsa_metro` (Napa, CA metro area)
* `37100` — `oxnard_ca_metro_area__cbsa_metro` (Oxnard, CA metro area)
* `39820` — `redding_ca_metro_area__cbsa_metro` (Redding, CA metro area)
* `40060` — `richmond_va_metro_area__cbsa_metro` (Richmond, VA metro area)
* `40140` — `riverside_ca_metro_area__cbsa_metro` (Riverside, CA metro area)
* `40220` — `roanoke_va_metro_area__cbsa_metro` (Roanoke, VA metro area)
* `40900` — `sacramento_ca_metro_area__cbsa_metro` (Sacramento, CA metro area)
* `41500` — `salinas_ca_metro_area__cbsa_metro` (Salinas, CA metro area)
* `41740` — `san_diego_ca_metro_area__cbsa_metro` (San Diego, CA metro area)
* `41940` — `san_jose_ca_metro_area__cbsa_metro` (San Jose, CA metro area)
* `42020` — `san_luis_obispo_ca_metro_area__cbsa_metro` (San Luis Obispo, CA metro area)
* `42100` — `santa_cruz_ca_metro_area__cbsa_metro` (Santa Cruz, CA metro area)
* `42200` — `santa_maria_ca_metro_area__cbsa_metro` (Santa Maria, CA metro area)
* `42220` — `santa_rosa_ca_metro_area__cbsa_metro` (Santa Rosa, CA metro area)
* `44420` — `staunton_va_metro_area__cbsa_metro` (Staunton, VA metro area)
* `44700` — `stockton_ca_metro_area__cbsa_metro` (Stockton, CA metro area)
* `45940` — `trenton_nj_metro_area__cbsa_metro` (Trenton, NJ metro area)
* `46700` — `vallejo_ca_metro_area__cbsa_metro` (Vallejo, CA metro area)
* `47220` — `vineland_nj_metro_area__cbsa_metro` (Vineland, NJ metro area)
* `47260` — `virginia_beach_va_metro_area__cbsa_metro` (Virginia Beach, VA metro area)
* `47300` — `visalia_ca_metro_area__cbsa_metro` (Visalia, CA metro area)
* `49020` — `winchester_va_metro_area__cbsa_metro` (Winchester, VA metro area)
* `49700` — `yuba_city_ca_metro_area__cbsa_metro` (Yuba City, CA metro area)

### Compiled only (1)

* `36140` — `ocean_city_nj_metro_area__cbsa_metro` (Ocean City, NJ metro area)

### Provisional only (9)

* `15700` — `cambridge_md_metro_area__cbsa_metro` (Cambridge, MD metro area)
* `17340` — `clearlake_ca_metro_area__cbsa_metro` (Clearlake, CA metro area)
* `18860` — `crescent_city_ca_metro_area__cbsa_metro` (Crescent City, CA metro area)
* `20660` — `easton_md_metro_area__cbsa_metro` (Easton, MD metro area)
* `39780` — `red_bluff_ca_metro_area__cbsa_metro` (Red Bluff, CA metro area)
* `43760` — `sonora_ca_metro_area__cbsa_metro` (Sonora, CA metro area)
* `45000` — `susanville_ca_metro_area__cbsa_metro` (Susanville, CA metro area)
* `46020` — `truckee_ca_metro_area__cbsa_metro` (Truckee, CA metro area)
* `46380` — `ukiah_ca_metro_area__cbsa_metro` (Ukiah, CA metro area)

### Absent from both (2)

* `15680` — `california_md_metro_area__cbsa_metro` (California, MD metro area)
* `31460` — `madera_ca_metro_area__cbsa_metro` (Madera, CA metro area)

### Compiled missing from governed 53 (11)

* `15680` — `california_md_metro_area__cbsa_metro` (California, MD metro area)
* `15700` — `cambridge_md_metro_area__cbsa_metro` (Cambridge, MD metro area)
* `17340` — `clearlake_ca_metro_area__cbsa_metro` (Clearlake, CA metro area)
* `18860` — `crescent_city_ca_metro_area__cbsa_metro` (Crescent City, CA metro area)
* `20660` — `easton_md_metro_area__cbsa_metro` (Easton, MD metro area)
* `31460` — `madera_ca_metro_area__cbsa_metro` (Madera, CA metro area)
* `39780` — `red_bluff_ca_metro_area__cbsa_metro` (Red Bluff, CA metro area)
* `43760` — `sonora_ca_metro_area__cbsa_metro` (Sonora, CA metro area)
* `45000` — `susanville_ca_metro_area__cbsa_metro` (Susanville, CA metro area)
* `46020` — `truckee_ca_metro_area__cbsa_metro` (Truckee, CA metro area)
* `46380` — `ukiah_ca_metro_area__cbsa_metro` (Ukiah, CA metro area)

### Provisional missing from governed 53 (3)

* `15680` — `california_md_metro_area__cbsa_metro` (California, MD metro area)
* `31460` — `madera_ca_metro_area__cbsa_metro` (Madera, CA metro area)
* `36140` — `ocean_city_nj_metro_area__cbsa_metro` (Ocean City, NJ metro area)

### Union (51)

* `12100` — `atlantic_city_nj_metro_area__cbsa_metro` (Atlantic City, NJ metro area)
* `12540` — `bakersfield_ca_metro_area__cbsa_metro` (Bakersfield, CA metro area)
* `12580` — `baltimore_md_metro_area__cbsa_metro` (Baltimore, MD metro area)
* `13980` — `blacksburg_va_metro_area__cbsa_metro` (Blacksburg, VA metro area)
* `15700` — `cambridge_md_metro_area__cbsa_metro` (Cambridge, MD metro area)
* `16820` — `charlottesville_va_metro_area__cbsa_metro` (Charlottesville, VA metro area)
* `17020` — `chico_ca_metro_area__cbsa_metro` (Chico, CA metro area)
* `17340` — `clearlake_ca_metro_area__cbsa_metro` (Clearlake, CA metro area)
* `18860` — `crescent_city_ca_metro_area__cbsa_metro` (Crescent City, CA metro area)
* `19060` — `cumberland_md_metro_area__cbsa_metro` (Cumberland, MD metro area)
* `19260` — `danville_va_metro_area__cbsa_metro` (Danville, VA metro area)
* `20660` — `easton_md_metro_area__cbsa_metro` (Easton, MD metro area)
* `20940` — `el_centro_ca_metro_area__cbsa_metro` (El Centro, CA metro area)
* `21700` — `eureka_ca_metro_area__cbsa_metro` (Eureka, CA metro area)
* `23420` — `fresno_ca_metro_area__cbsa_metro` (Fresno, CA metro area)
* `25180` — `hagerstown_md_metro_area__cbsa_metro` (Hagerstown, MD metro area)
* `25260` — `hanford_ca_metro_area__cbsa_metro` (Hanford, CA metro area)
* `25500` — `harrisonburg_va_metro_area__cbsa_metro` (Harrisonburg, VA metro area)
* `31340` — `lynchburg_va_metro_area__cbsa_metro` (Lynchburg, VA metro area)
* `32300` — `martinsville_va_metro_area__cbsa_metro` (Martinsville, VA metro area)
* `32900` — `merced_ca_metro_area__cbsa_metro` (Merced, CA metro area)
* `33700` — `modesto_ca_metro_area__cbsa_metro` (Modesto, CA metro area)
* `34900` — `napa_ca_metro_area__cbsa_metro` (Napa, CA metro area)
* `36140` — `ocean_city_nj_metro_area__cbsa_metro` (Ocean City, NJ metro area)
* `37100` — `oxnard_ca_metro_area__cbsa_metro` (Oxnard, CA metro area)
* `39780` — `red_bluff_ca_metro_area__cbsa_metro` (Red Bluff, CA metro area)
* `39820` — `redding_ca_metro_area__cbsa_metro` (Redding, CA metro area)
* `40060` — `richmond_va_metro_area__cbsa_metro` (Richmond, VA metro area)
* `40140` — `riverside_ca_metro_area__cbsa_metro` (Riverside, CA metro area)
* `40220` — `roanoke_va_metro_area__cbsa_metro` (Roanoke, VA metro area)
* `40900` — `sacramento_ca_metro_area__cbsa_metro` (Sacramento, CA metro area)
* `41500` — `salinas_ca_metro_area__cbsa_metro` (Salinas, CA metro area)
* `41740` — `san_diego_ca_metro_area__cbsa_metro` (San Diego, CA metro area)
* `41940` — `san_jose_ca_metro_area__cbsa_metro` (San Jose, CA metro area)
* `42020` — `san_luis_obispo_ca_metro_area__cbsa_metro` (San Luis Obispo, CA metro area)
* `42100` — `santa_cruz_ca_metro_area__cbsa_metro` (Santa Cruz, CA metro area)
* `42200` — `santa_maria_ca_metro_area__cbsa_metro` (Santa Maria, CA metro area)
* `42220` — `santa_rosa_ca_metro_area__cbsa_metro` (Santa Rosa, CA metro area)
* `43760` — `sonora_ca_metro_area__cbsa_metro` (Sonora, CA metro area)
* `44420` — `staunton_va_metro_area__cbsa_metro` (Staunton, VA metro area)
* `44700` — `stockton_ca_metro_area__cbsa_metro` (Stockton, CA metro area)
* `45000` — `susanville_ca_metro_area__cbsa_metro` (Susanville, CA metro area)
* `45940` — `trenton_nj_metro_area__cbsa_metro` (Trenton, NJ metro area)
* `46020` — `truckee_ca_metro_area__cbsa_metro` (Truckee, CA metro area)
* `46380` — `ukiah_ca_metro_area__cbsa_metro` (Ukiah, CA metro area)
* `46700` — `vallejo_ca_metro_area__cbsa_metro` (Vallejo, CA metro area)
* `47220` — `vineland_nj_metro_area__cbsa_metro` (Vineland, NJ metro area)
* `47260` — `virginia_beach_va_metro_area__cbsa_metro` (Virginia Beach, VA metro area)
* `47300` — `visalia_ca_metro_area__cbsa_metro` (Visalia, CA metro area)
* `49020` — `winchester_va_metro_area__cbsa_metro` (Winchester, VA metro area)
* `49700` — `yuba_city_ca_metro_area__cbsa_metro` (Yuba City, CA metro area)

## Prior-discrepancy reconciliation

Outcome **A** applies: the earlier `39 shared / 3 compiled-only / 11 provisional-only` counts were internally correct for the earlier diagnostic's two *reported inventories*, but that diagnostic's provisional identity inventory was wrong. It reported `32300` (Martinsville) and `42020` (San Luis Obispo) absent while reporting `15680` (California, MD) and `31460` (Madera) present. The exact provisional r2 physical data has the opposite membership for those two pairs: `32300` and `42020` are present; `15680` and `31460` are absent. `36140` (Ocean City) is absent in both versions of the provisional evidence.

This was a stale diagnostic/fixture inventory, not a canonical-code mapping rule or a changed universe. The correction commit changed documentation and fixture expectations, not the exact-code parser/canonicalizer. Replacing the two false provisional presences with the two false absences preserves the provisional count of 50 but moves Martinsville and San Luis Obispo from compiled-only to shared, and moves California and Madera from provisional-only to absent-from-both. Thus `39/3/11` becomes `41/1/9`, with union 51 rather than the stale inferred 53. The Martinsville contradiction appeared specifically because narrative and fixture assertions treated the earlier diagnostic inventory as physical truth; direct `data.parquet` membership proves canonical `32300` is shared.

The governed 53 did **not** change: its frozen contract hash is unchanged. No fuzzy mapping, synthesis, parent-metro substitution, or Metropolitan Division derivation is involved.

## Negative controls and scope

Neither physical artifact contains canonical `09999`, any identity classified `metropolitan_division`, or any synthesized/fuzzy-mapped CBSA. The provisional artifact contains no `united_states__nation` row. These follow from direct canonical-row inventories, with the concept registry used only to classify the physically observed canonical IDs after extraction.

This reconciliation created no source candidate or family artifact, moved no accepted pointer, created no Source Set, mutated no DuckDB database, consumed no Redfin readiness, performed no provider discovery, and changed no schedule. Smoke 200 freezes the exact identities, arithmetic, contract hash, artifact full hashes, Metropolitan Division exclusion, placeholder exclusion, and Martinsville shared status so future resolver work cannot silently revive the stale assumptions.

## Recommendation

**PASS** for beginning a separately governed BPS family-resolution implementation. The resolver must start from the exact `42/50/41/1/9/51/2` physical-set facts above, must preserve the two governed identities absent from both parents as absent, and must not synthesize them. This is readiness evidence only; it is not authorization to publish or accept a family artifact.
