import pyarrow as pa
import pytest

from nycynapse_lake.contracts import ContractError, parse
from nycynapse_lake.quality import failed, validate


def table(**over):
    t = {
        "name": "reading",
        "description": "sensor readings",
        "grain": "one row per sensor per hour",
        "primary_key": ["sensor_id", "hour"],
        "write": "merge",
        "version_column": "updated_at",
        "max_reject_rate": 0.5,
        "columns": [
            {"name": "sensor_id", "type": "integer", "nullable": False, "description": "s"},
            {"name": "hour", "type": "timestamp", "nullable": False, "description": "h"},
            {
                "name": "value",
                "type": "double",
                "min": 0,
                "max": 100,
                "description": "v",
                "max_null_rate": 0.1,
            },
            {"name": "status", "type": "varchar", "accepted": ["ok", "stale"], "description": "s"},
            {"name": "updated_at", "type": "timestamp", "description": "u"},
        ],
    }
    t.update(over)
    return parse(
        {
            "source": "demo",
            "version": 1,
            "domain": "t",
            "description": "d",
            "cadence": "hourly",
            "freshness_sla": "2 hours",
            "tables": [t],
        }
    ).tables[0]


def stage(con, rows):
    data = pa.table(
        {
            "sensor_id": [r[0] for r in rows],
            "hour": [r[1] for r in rows],
            "value": [r[2] for r in rows],
            "status": [r[3] for r in rows],
            "updated_at": [r[4] for r in rows],
        }
    )
    con.register("staged", data)
    return "staged"


def test_bad_rows_are_set_aside_with_reasons(con):
    rows = [
        ("1", "2024-01-01 00:00", "10", "ok", "2024-01-01 01:00"),
        (None, "2024-01-01 00:00", "10", "ok", None),
        ("2", "not a time", "10", "ok", None),
        ("3", "2024-01-01 00:00", "250", "ok", None),
        ("4", "2024-01-01 00:00", "5", "broken", None),
    ]
    v = validate(con, table(max_reject_rate=1.0), stage(con, rows))
    assert v.rows_in == 5
    assert v.rows_rejected == 4
    reasons = sorted(r for r, _ in v.rejected_sample)
    assert reasons == [
        "hour is not a valid timestamp",
        "sensor_id is missing",
        "status has an unexpected value",
        "value above 100",
    ]
    original = dict(v.rejected_sample)["value above 100"]
    assert original["value"] == "250"
    assert con.execute(f"SELECT count(*) FROM {v.clean}").fetchone()[0] == 1


def test_duplicate_keys_keep_latest_version(con):
    rows = [
        ("1", "2024-01-01 00:00", "10", "ok", "2024-01-01 01:00"),
        ("1", "2024-01-01 00:00", "20", "ok", "2024-01-01 02:00"),
        ("1", "2024-01-01 00:00", "30", "ok", "2024-01-01 00:30"),
    ]
    v = validate(con, table(), stage(con, rows))
    assert v.duplicates == 2
    assert con.execute(f"SELECT value FROM {v.clean}").fetchall() == [(20.0,)]


def test_reject_rate_over_threshold_fails_the_batch(con):
    rows = [("1", "2024-01-01", "500", "ok", None), ("2", "2024-01-01", "5", "ok", None)]
    v = validate(con, table(max_reject_rate=0.1), stage(con, rows))
    assert [c.check for c in failed(v.checks)] == ["reject_rate"]


def test_too_few_rows_fails_the_batch(con):
    v = validate(con, table(min_rows=10), stage(con, [("1", "2024-01-01", "5", "ok", None)]))
    assert [c.check for c in failed(v.checks)] == ["min_rows"]


def test_null_rate_is_a_warning(con):
    rows = [("1", "2024-01-01", None, "ok", None), ("2", "2024-01-01", "5", "ok", None)]
    v = validate(con, table(), stage(con, rows))
    warn = [c for c in v.checks if c.check == "null_rate:value"][0]
    assert not warn.passed and warn.severity == "warn"
    assert not failed(v.checks)


def test_missing_column_stops_the_load(con):
    con.execute("CREATE TEMP TABLE partial AS SELECT 1 AS sensor_id")
    with pytest.raises(ContractError):
        validate(con, table(), "partial")


def test_new_upstream_column_is_reported(con):
    con.execute(
        "CREATE TEMP TABLE wide AS SELECT 1 AS sensor_id, TIMESTAMP '2024-01-01' AS hour, "
        "1.0 AS value, 'ok' AS status, NULL::TIMESTAMP AS updated_at, 'x' AS surprise"
    )
    v = validate(con, table(), "wide")
    drift = [c for c in v.checks if c.check == "schema_drift"][0]
    assert not drift.passed
    assert drift.detail["unexpected_columns"] == ["surprise"]
