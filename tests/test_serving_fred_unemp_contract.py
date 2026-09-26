import duckdb
import pytest
import scripts.validate_serving_snapshot as serving


def test_serving_candidate_missing_fred_unemp_fails_closed(tmp_path):
    path=tmp_path/"candidate.duckdb"
    con=duckdb.connect(str(path))
    con.execute("""CREATE TABLE fact_timeseries (
        geo_id VARCHAR, metric_id VARCHAR, date DATE,
        property_type_id VARCHAR, source_id VARCHAR, value DOUBLE)""")
    for source in sorted(serving.EXPECTED_SOURCES - {"fred_unemp"}):
        con.execute("INSERT INTO fact_timeseries VALUES (?, ?, DATE '2026-08-31', 'all', ?, 1)",
                    ["fixture__nation", "fixture_metric", source])
    con.close()
    with pytest.raises(SystemExit, match="missing expected sources: \\['fred_unemp'\\]"):
        serving.validate_snapshot_path(path)
