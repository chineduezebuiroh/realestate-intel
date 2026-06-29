# Job Execution Doc v1

## Purpose

Itemize local job commands that will require manual kickoff, since they won't have a scheduled / automated workflow file conencted to them.

---

## Build Serving Snapshot Job
- PYTHONPATH=. python -m jobs.run_build_serving_snapshot


## BLS CES (Bureau of Labor Statistics Current Employment Statistics) Full Refresh
- PYTHONPATH=. python -m jobs.full_refresh.run_refresh_bls_ces

---

