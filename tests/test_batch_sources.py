from datetime import date

import duckdb

from nycynapse_lake.sources import mta_gtfs, tlc_trips


def test_gtfs_times_past_midnight_stay_on_the_service_day():
    con = duckdb.connect()
    got = con.execute(
        f"SELECT {mta_gtfs._seconds('t')} "
        "FROM (VALUES ('00:06:00'), ('24:00:30'), ('25:10:00')) v(t)"
    ).fetchall()
    assert [r[0] for r in got] == [360, 86430, 90600]


def test_realtime_trip_id_is_the_tail_of_the_schedule_id():
    con = duckdb.connect()
    got = con.execute(
        "SELECT NULLIF(regexp_extract(t, '_([0-9]{6}_.+)$', 1), '') FROM (VALUES "
        "('ASP26GEN-1038-Sunday-00_000600_1..S03R'), ('odd')) v(t)"
    ).fetchall()
    assert [r[0] for r in got] == ["000600_1..S03R", None]


class FakeCtx:
    def __init__(self, loaded):
        self.loaded = loaded

    def checkpoint(self, key, default=None):
        return self.loaded if key == "loaded_months" else default


def test_tlc_rechecks_recent_months_and_skips_settled_ones(monkeypatch):
    class Today(date):
        @classmethod
        def today(cls):
            return date(2024, 8, 15)

    monkeypatch.setattr(tlc_trips, "date", Today)
    loaded = [date(2024, m, 1).isoformat() for m in range(1, 8)]
    months = tlc_trips._months(FakeCtx(loaded))
    assert [m.month for m in months] == [4, 5, 6, 7]
    months = tlc_trips._months(FakeCtx([]))
    assert [m.month for m in months] == [1, 2, 3, 4, 5, 6, 7]


def test_fhvhv_sql_fills_columns_missing_from_older_files():
    sql = tlc_trips.fhvhv_sql("/x.parquet", date(2024, 1, 1), {"airport_fee"})
    assert "NULL AS cbd_congestion_fee_usd" in sql
    assert "airport_fee AS airport_fee_usd" in sql
