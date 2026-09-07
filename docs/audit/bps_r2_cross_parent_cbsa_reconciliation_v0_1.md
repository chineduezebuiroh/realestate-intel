# Immutable r2 BPS cross-parent CBSA reconciliation

**Decision: PASS — both published r2 physical parents are valid under the governed BPS source contract, and family resolution may use their exact release-variable coverage.**

## Authority and correction

The authoritative inputs are the exact cataloged and remotely verified compiled
`src__census_bps__2026-04__r2__993afaddb934ce4f` and provisional
`src__census_bps_provisional__2026-07__r2__61c56540953237cb` packages. The hosted
family resolver retrieved those packages through the production GitHub Release
resolver, validated their frozen package, artifact-content, and `data.parquet`
hashes, and computed physical canonical identities from the extracted payloads.

This supersedes the prior `41 shared / 1 compiled-only / 9 provisional-only /
51 union / 2 absent` version of this audit and its fixture. That version was bad
audit provenance: it did not describe the bytes published under the frozen r2
identities. It caused no artifact mutation and is not evidence of an
immutability violation.

## Governed semantic verdict

**r2 is VALID. No r3 physical-parent revision is required.**

The governed source contract permits CBSA coverage to vary by release. It
requires exact five-digit mapping for compatible concepts, rejects unsupported
concepts and the `09999` placeholder, prohibits synthesis, and retains physical
absence. Provisional releases must preserve complete governed state and county
coverage; they are not required to contain every compatible CBSA. The compiled
master is a historical snapshot whose CBSA coverage likewise reflects the
selected provider release. No contract requires the two parents to have equal
CBSA inventories.

Accordingly, compiled `202604` legitimately includes California, MD (`15680`)
and Madera, CA (`31460`) and legitimately lacks Eureka, CA (`21700`) and
Martinsville, VA (`32300`). Provisional `2607` legitimately lacks `15680` and
`31460` and includes `21700` and `32300`. All are exact compatible-code
observations or physical absences; none violates a transformation rule.

## Exact compatible inventories

| Set | Count | Codes |
|---|---:|---|
| Governed compatible | 53 | Frozen in `config/bps_cbsa_canonical_concepts_v1.csv` |
| Compiled | 42 | `12100, 12540, 12580, 13980, 15680, 16820, 17020, 19060, 19260, 20940, 23420, 25180, 25260, 25500, 31340, 31460, 32900, 33700, 34900, 36140, 37100, 39820, 40060, 40140, 40220, 40900, 41500, 41740, 41940, 42020, 42100, 42200, 42220, 44420, 44700, 45940, 46700, 47220, 47260, 47300, 49020, 49700` |
| Provisional | 50 | `12100, 12540, 12580, 13980, 15700, 16820, 17020, 17340, 18860, 19060, 19260, 20660, 20940, 21700, 23420, 25180, 25260, 25500, 31340, 32300, 32900, 33700, 34900, 37100, 39780, 39820, 40060, 40140, 40220, 40900, 41500, 41740, 41940, 42020, 42100, 42200, 42220, 43760, 44420, 44700, 45000, 45940, 46020, 46380, 46700, 47220, 47260, 47300, 49020, 49700` |
| Shared | 39 | `12100, 12540, 12580, 13980, 16820, 17020, 19060, 19260, 20940, 23420, 25180, 25260, 25500, 31340, 32900, 33700, 34900, 37100, 39820, 40060, 40140, 40220, 40900, 41500, 41740, 41940, 42020, 42100, 42200, 42220, 44420, 44700, 45940, 46700, 47220, 47260, 47300, 49020, 49700` |
| Compiled only | 3 | `15680, 31460, 36140` |
| Provisional only | 11 | `15700, 17340, 18860, 20660, 21700, 32300, 39780, 43760, 45000, 46020, 46380` |
| Union | 53 | Every governed compatible code |
| Absent from both | 0 | None |

Arithmetic is exact: `39 + 3 = 42`, `39 + 11 = 50`, `39 + 3 + 11 = 53`, and
`53 + 0 = 53`. Neither parent contains an unsupported canonical CBSA concept.

## Family-resolution consequence

Family resolution operates on the full canonical key `(geo_id, metric_id, date,
property_type_id)`, not on an assumed common geography set. It retains
compiled-only and provisional-only keys, chooses compiled only for an identical
key collision, and allows different release months for the same geography to
coexist. It synthesizes neither gap months nor missing geographies. Therefore
the `39/3/11` relationship is a valid physical-set fact, not a family-policy or
parent-transformation defect.

The immutable parents, their catalog records, their republication records, the
53-code compatibility universe, and parent hash validation remain unchanged.
The exact `39/3/11` counts and identity sets are scoped exclusively to this
frozen r2 pair and its regression fixture. They are not a permanent production
gate: later parent releases must derive their physical overlap, differences,
union, and absence from their own validated payloads within the unchanged
compatible universe.
