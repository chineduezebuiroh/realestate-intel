# Geo Hierarchy Algorithm v1

## Purpose

Psuedo-code to help explain the algorithm for mappging the various Redfin geography levels.

## Pseudo-Code Definitions

### Macro Regime Engine

with reference_table as (
    select  states
    from    repo_refernce_table
    )
select  z."PARENT_METRO_REGION", z."PARENT_METRO_REGION_METRO_CODE", z."TABLE_ID" as COUNTY_ID, z.COUNTY,
        y."TABLE_ID" as METRO_ID, y.METRO,
        x."TABLE_ID" as STATE_ID, x.STATE, x."PARENT_METRO_REGION" as CENSUS_REGION
from (  select  distinct a."PARENT_METRO_REGION", a."PARENT_METRO_REGION_METRO_CODE", a."TABLE_ID", a."REGION" as COUNTY,	a."STATE", a."STATE_CODE"
        from    county_market_tracker.tsv000 a
        join    reference_table ref
          on    a."STATE_CODE" in ref.states
        where   a."PARENT_METRO_REGION_METRO_CODE" <> "NA"
        ) z
join (  select  distinct b."PARENT_METRO_REGION", b."PARENT_METRO_REGION_METRO_CODE", b."TABLE_ID", b."REGION" as METRO, b."STATE_CODE"
        from    redfin_metro_market_tracker.tsv000 b
        join    reference_table ref
          on    b."STATE_CODE" in ref.states
        where   b."PARENT_METRO_REGION_METRO_CODE" <> "NA"
        ) y
  on  z."PARENT_METRO_REGION_METRO_CODE" = y."PARENT_METRO_REGION_METRO_CODE"
join (  select  distinct c."PARENT_METRO_REGION", c."TABLE_ID", c."REGION" as STATE, c."STATE_CODE"
        from    state_market_tracker.tsv000 c
        join    reference_table ref
          on    c."STATE_CODE" in ref.states
        ) x
  on  z."STATE_CODE" = x."STATE_CODE"




### Local Opportunity Engine

