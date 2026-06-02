# Geo Hierarchy Algorithm v1

## Purpose

Psuedo-code to help explain the algorithm for mappging the various Redfin geography levels.

## Pseudo-Code Definitions

### Macro Regime Engine

_'github_repo_reference_table' will be a table in  my repo with the state codes I want to iclude in the macro analysis_

select  z."PARENT_METRO_REGION", z."PARENT_METRO_REGION_METRO_CODE", z."TABLE_ID" as COUNTY_ID, z.COUNTY,
        y."TABLE_ID" as METRO_ID, y.METRO,
        x."TABLE_ID" as STATE_ID, x.STATE, x."PARENT_METRO_REGION" as CENSUS_REGION
from (  select  distinct a."PARENT_METRO_REGION", a."PARENT_METRO_REGION_METRO_CODE", a."TABLE_ID", a."REGION" as COUNTY,	a."STATE", a."STATE_CODE"
        from    county_market_tracker.tsv000 a
        join    github_repo_reference_table ref
          on    a."STATE_CODE" in ref.state_codes
        where   a."PARENT_METRO_REGION_METRO_CODE" <> "NA"
        ) z
join (  select  distinct b."PARENT_METRO_REGION", b."PARENT_METRO_REGION_METRO_CODE", b."TABLE_ID", b."REGION" as METRO, b."STATE_CODE"
        from    redfin_metro_market_tracker.tsv000 b
        join    github_repo_reference_table ref
          on    b."STATE_CODE" in ref.state_codes
        where   b."PARENT_METRO_REGION_METRO_CODE" <> "NA"
        ) y
  on  z."PARENT_METRO_REGION_METRO_CODE" = y."PARENT_METRO_REGION_METRO_CODE"
join (  select  distinct c."PARENT_METRO_REGION", c."TABLE_ID", c."REGION" as STATE, c."STATE_CODE"
        from    state_market_tracker.tsv000 c
        join    github_repo_reference_table ref
          on    c."STATE_CODE" in ref.state_codes
        ) x
  on  z."STATE_CODE" = x."STATE_CODE"




### Local Opportunity Engine

_'github_repo_master_zip_county_xref_table' will be a master xref table in my repo with all zipcodes matched their respectie Counties_
_'github_repo_county_reference_table' will be a reference table in my repo with all the counties for which I want to analyze zipcodes (and maybe eventually neighborhoods)_

select  z."PARENT_METRO_REGION", z."PARENT_METRO_REGION_METRO_CODE", z."TABLE_ID" as ZIP_CODE_ID, "zip_code" as GEO_LEVEL, z.ZIP,
        y."TABLE_ID" as COUNTY_ID, y.COUNTY, y."STATE", y."STATE_CODE"
from (  select  distinct a."PARENT_METRO_REGION", a."PARENT_METRO_REGION_METRO_CODE", a."TABLE_ID", a."REGION" as ZIP,	a."STATE", a."STATE_CODE"
        from    zip_code_market_tracker.tsv000 a
        where   a."PARENT_METRO_REGION_METRO_CODE" <> "NA"
        ) z
join (  select  master_xref.*
        from    github_repo_master_zip_county_xref_table master_xref
        join    github_repo_county_reference_table c
          on    master_xref.County in c.County
        ) ref
  on    right_substring(z.ZIP, 5) in ref.ZIP
join (  select  distinct b."PARENT_METRO_REGION", b."PARENT_METRO_REGION_METRO_CODE", b."TABLE_ID", b."REGION" as COUNTY,	b."STATE", b."STATE_CODE"
        from    county_market_tracker.tsv000 b
        where   b."PARENT_METRO_REGION_METRO_CODE" <> "NA"
        ) y
  on    z."PARENT_METRO_REGION_METRO_CODE" = y."PARENT_METRO_REGION_METRO_CODE"
 and    z."STATE_CODE" = y."STATE_CODE"
 and    y.COUNTY in ref.County
