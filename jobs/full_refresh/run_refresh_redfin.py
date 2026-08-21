from __future__ import annotations
# jobs/full_refresh/run_refresh_redfin.py

from sources.redfin.ingest import monthly_gate


def main() -> int:
    # Downloads are intentionally manual. Future monthly orchestration passes its
    # target month here, then invokes the explicit validate/build/apply/publish
    # commands only when this gate says registered.
    from datetime import date
    target = date.today().strftime("%Y-%m")
    state = monthly_gate(target)
    print(state)
    if state != "registered":
        return 0
    print("[job] registered Redfin drop requires explicit governed local apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
    
