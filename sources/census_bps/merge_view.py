from __future__ import annotations
# sources/census_bps/merge_view.py

from core.db import connect

VIEW_NAME = "fact_timeseries_bps"

COMPILED = "census_bps"              # your compiled source_id
PROVISIONAL = "census_bps_provisional"  # your provisional source_id


def main() -> None:
    con = connect()

    # View policy:
    # - If compiled exists for the exact fact PK -> use compiled
    # - Else use provisional
    #
    # Implemented by union + window rank picking compiled first.
    con.execute(f"""
    CREATE OR REPLACE VIEW {VIEW_NAME} AS
    WITH base AS (
      SELECT
        geo_id, metric_id, date, property_type_id,
        value, source_id, property_type
      FROM fact_timeseries
      WHERE source_id IN ('{COMPILED}', '{PROVISIONAL}')
    ),
    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY geo_id, metric_id, date, property_type_id
          ORDER BY
            CASE source_id
              WHEN '{COMPILED}' THEN 1
              WHEN '{PROVISIONAL}' THEN 2
              ELSE 99
            END
        ) AS rn
      FROM base
    )
    SELECT
      geo_id, metric_id, date, property_type_id,
      value, source_id, property_type
    FROM ranked
    WHERE rn = 1;
    """)

    # Hard invariant: if both sources exist for a key and values differ, scream.
    # (You can relax this later, but don’t start lax.)
    conflicts = con.execute(f"""
      WITH source_overlap AS (
        SELECT
          geo_id, metric_id, date, property_type_id,
          COUNT(DISTINCT source_id) AS n_sources,
          COUNT(DISTINCT value) AS n_values
        FROM fact_timeseries
        WHERE source_id IN ('{COMPILED}', '{PROVISIONAL}')
        GROUP BY 1,2,3,4
      )
      SELECT COUNT(*) AS n_conflicts
      FROM source_overlap
      WHERE n_sources > 1 AND n_values > 1;
    """).fetchone()[0]

    print(f"[bps:merge_view] created view {VIEW_NAME}")
    print(f"[bps:merge_view] conflicts(compiled vs provisional different values) = {conflicts}")

    if conflicts:
        raise SystemExit("[bps:merge_view] abort: compiled/provisional overlap has conflicting values")

    con.close()


if __name__ == "__main__":
    main()
