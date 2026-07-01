# Job Execution Doc v1

## Purpose

Itemize local job commands that will require manual kickoff, since they won't have a scheduled / automated workflow file conencted to them.

---

## Local Execution Commands

### Build Serving Snapshot Job
- PYTHONPATH=. python -m jobs.run_build_serving_snapshot



### BLS (Bureau of Labor Statistics) Full Refresh

Generate BLS Spec
- PYTHONPATH=. python -m jobs.full_refresh.run_generate_bls_specs.py

CES (Current Employment Statistics) Full Refresh
- PYTHONPATH=. python -m jobs.full_refresh.run_refresh_bls_ces

LAUS (Local Area Unemployment Statistics) Full Refresh
- PYTHONPATH=. python -m jobs.full_refresh.run_refresh_bls_laus



### Census
ACS (American Community Survey) Full Refresh
- PYTHONPATH=. python -m jobs.full_refresh.run_refresh_census_acs

BPS (Building Permits Survey) Full Refresh
- PYTHONPATH=. python -m jobs.full_refresh.run_refresh_census_bps


---

