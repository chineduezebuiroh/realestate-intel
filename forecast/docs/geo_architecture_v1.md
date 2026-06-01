# Geo Architecture v1

## Purpose

This document defines the v1 geography architecture for the real estate market intelligence platform.

The platform must support two related but distinct workflows:

1. Macro market regime analysis across larger geographies.
2. Local opportunity analysis across ZIP codes and neighborhoods.

The goal is to avoid forcing every geography to use the same indicator set. Different geo levels have different data availability and should be scored accordingly.

## Core Principle

The system should not pretend that all indicators exist at all geo levels.

For example:

* unemployment may exist at county, metro, state, or selected city level;
* GDP may exist at state or national level;
* mortgage rates are national;
* Redfin housing metrics may exist at neighborhood, ZIP, city, county, metro, state, and national levels.

Therefore, the system needs geography-aware scoring.

## Supported Geo Levels

V1 supported levels:

* nation
* region
* state
* metro
* county
* city
* ZIP
* neighborhood

These levels are not always a perfect tree. ZIP codes and neighborhoods may overlap or relate imperfectly to cities and counties. V1 should use a practical hierarchy, while preserving review flags and override capability.

## Two Mapping Types

The system needs two related but distinct geography mappings.

## 1. Macro Stand-Alone Hierarchy

Purpose:

> Support macro market analysis even when no local ZIP/neighborhood analysis is needed.

Example use cases:

* Compare DC MSA vs New York MSA.
* Compare Prince George's County vs Montgomery County.
* Compare Maryland vs Virginia.
* Rank all selected MSAs.

This mapping should describe larger geography relationships:

```text
nation
  region
    state
      metro
        county
          city
```

This mapping supports the Macro Regime Engine.

## 2. Local-to-Macro Context Mapping

Purpose:

> Connect local geographies to their parent macro context.

Example use cases:

* Compare Georgetown vs Deanwood.
* Compare ZIP 20019 vs ZIP 20011.
* Determine whether a local opportunity is aligned with or fighting against broader macro context.

Example:

```text
deanwood_neighborhood
  parent_city = dc_city
  parent_metro = dc_msa
  parent_state = dc_state
  parent_nation = us_nation
```

This mapping supports the Local Opportunity Engine.

## Important Distinction

The Local Opportunity Engine does not need to directly assign every macro indicator to every local geography in V1.

It primarily needs:

```text
local signal + parent macro context
```

not:

```text
neighborhood-level GDP
neighborhood-level payroll growth
neighborhood-level unemployment
```

## Engine Responsibilities

## Macro Regime Engine

Allowed geo levels:

* nation
* region
* state
* metro
* county
* city

Primary indicators:

* employment growth
* unemployment trend
* payroll momentum
* inventory growth
* permits / starts
* affordability
* liquidity
* capital market context

Primary output:

* macro regime
* market-cycle phase
* macro score
* supply-demand balance
* conviction level

## Local Opportunity Engine

Allowed geo levels:

* ZIP
* neighborhood

Primary indicators:

* PPSF momentum
* sale price momentum
* inventory trend
* DOM trend
* sale-to-list ratio
* transaction volume
* months of supply
* relative affordability

Primary output:

* local opportunity score
* local risk score
* local momentum score
* supply tightness score
* parent macro alignment flag

## Combined Interpretation

The final intelligence layer should combine macro and local signals.

Example:

| Macro Context       | Local Signal        | Interpretation                             |
| ------------------- | ------------------- | ------------------------------------------ |
| Strong macro regime | Strong local signal | High-conviction opportunity                |
| Strong macro regime | Weak local signal   | Selective / investigate local weakness     |
| Weak macro regime   | Strong local signal | Possible outlier / higher-risk opportunity |
| Weak macro regime   | Weak local signal   | Avoid / low priority                       |

## Proposed Generated Hierarchy Schema

A first-pass generated hierarchy file may look like:

```text
child_geo_id
child_geo_level
child_region_name
child_state
parent_city_geo_id
parent_county_geo_id
parent_metro_geo_id
parent_state_geo_id
parent_region_geo_id
parent_nation_geo_id
mapping_method
mapping_confidence
needs_review
notes
```

## Proposed Files

Design documents:

```text
forecast/docs/geo_architecture_v1.md
forecast/docs/indicator_matrix_v1.md
```

Generated config files:

```text
config/geo_hierarchy.generated.csv
```

Manual override files:

```text
config/geo_hierarchy_overrides.csv
```

Potential future DuckDB tables:

```text
dim_geo
dim_geo_hierarchy
dim_geo_context
```

## Redfin Metadata Notes

Redfin raw files appear to contain useful hierarchy clues:

* neighborhood files contain a `Parent region` column that maps to a city;
* ZIP files contain a `Parent region` column that maps to a city;
* city files contain a `Parent region` column, but this field may not reliably represent a true hierarchy;
* county and metro files also contain `Parent region`, but the semantic meaning needs validation;
* state files may use broad region labels such as `West` or `Midwest`.

Therefore:

1. neighborhood-to-city and ZIP-to-city mappings may be high-confidence;
2. city parent mappings require review;
3. county/metro/state relationships should be inferred carefully;
4. generated mappings should include confidence and review flags;
5. manual overrides should be allowed.

## V1 Recommendation

Do not hand-map every geography.

Instead:

1. infer mappings from Redfin metadata where reliable;
2. join against existing geo manifest identifiers;
3. assign mapping confidence;
4. flag ambiguous mappings for review;
5. apply manual overrides only for edge cases;
6. materialize a generated hierarchy file.

## Deferred Work

A full geo-to-macro feature mapping is deferred.

That would look like:

```text
target_geo
labor_geo
gdp_geo
construction_geo
capital_geo
income_geo
```

This may be needed later if macro variables are fed directly into local models.

For V1, the cleaner architecture is:

```text
local geography → parent macro context
```

not:

```text
local geography → every macro feature family
```
