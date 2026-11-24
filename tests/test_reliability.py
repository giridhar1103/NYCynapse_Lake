"""What happens when a source misbehaves, and whether anyone would notice."""

import dataclasses
import json
import os
import shutil
import subprocess
import uuid
import zipfile
from datetime import UTC, datetime, timedelta

import httpx
import pyarrow as pa
import pytest

from nycynapse_lake import backup, health, maintain, offsite
from nycynapse_lake.config import Settings
from nycynapse_lake.contracts import ContractError, parse
from nycynapse_lake.http import Http
from nycynapse_lake.quality import QualityFailure
from nycynapse_lake.retry import Retryable, RetryPolicy
from nycynapse_lake.runs import DiskLow, PartialFailure, run_source
from nycynapse_lake.socrata import Dataset, Socrata
from nycynapse_lake.sources.files import open_zip
from nycynapse_lake.sources.weather_alerts import live_rows


def http(handler, attempts=2):
    return Http(
        policy=RetryPolicy(attempts=attempts, base=0, cap=0),
        transport=httpx.MockTransport(handler),
        sleep=lambda s: None,
    )


def table(write="replace_all", **extra):
    return {
        "name": "obs",
        "description": "d",
        "grain": "g",
        "primary_key": ["id"],
        "write": write,
        "event_time": "at",
        "columns": [
            {"name": "id", "type": "INTEGER", "nullable": False, "description": "id"},
            {"name": "at", "type": "TIMESTAMPTZ", "description": "t"},
        ],
        **extra,
    }


def contract(*tables, sla="2 hours"):
    return parse(
        {
            "source": "demo",
            "version": 1,
            "domain": "test",
            "description": "d",
            "cadence": "hourly",
            "freshness_sla": sla,
            "tables": list(tables) or [table()],
        }
    )


def rows(n, at=None):
    at = at or datetime.now(UTC)
    return pa.table({"id": list(range(n)), "at": [at] * n})


# answers that are not what they claim to be


def test_an_error_page_behind_a_200_is_retried_and_then_named():
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(200, text="<html>busy</html>", headers={"Content-Type": "text/html"})

    with pytest.raises(Retryable, match="not JSON"):
        http(handler).get_json("https://x.test/a")
    assert len(calls) == 2


def test_a_server_error_is_never_taken_for_not_published():
    status = {"v": 404}
    client = http(lambda r: httpx.Response(status["v"]))
    assert client.exists("https://x.test/f") is False
    status["v"] = 403  # what S3 and CloudFront say for a missing file
    assert client.exists("https://x.test/f") is False
    status["v"] = 200
    assert client.exists("https://x.test/f") is True
    status["v"] = 503
    with pytest.raises(Retryable):
        client.exists("https://x.test/f")


def test_an_empty_download_is_an_error(tmp_path):
    with pytest.raises(Retryable, match="empty body"):
        http(lambda r: httpx.Response(200, content=b"")).download("https://x.test/f", tmp_path)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("body", ["<html>oops</html>\n", '"a",":id"\n"1","r1"\n"2"\n'])
def test_a_cut_off_or_wrong_socrata_page_is_an_error(tmp_path, body):
    soda = Socrata(http(lambda r: httpx.Response(200, text=body)))
    with pytest.raises(Retryable):
        list(soda.pages(Dataset("x.test", "abcd"), tmp_path, columns="a", where="1=1"))
    assert list(tmp_path.iterdir()) == []


def test_a_damaged_zip_counts_as_a_bad_download(tmp_path):
    bad = tmp_path / "a.zip"
    bad.write_bytes(b"not a zip")
    with pytest.raises(Retryable), open_zip(bad):
        pass
    good = tmp_path / "b.zip"
    with zipfile.ZipFile(good, "w") as z:
        z.writestr("a.csv", "x" * 5000)
    data = bytearray(good.read_bytes())
    data[60] ^= 0xFF
    good.write_bytes(bytes(data))
    with pytest.raises(Retryable), open_zip(good):
        pass


def test_weather_alerts_survive_a_feature_without_its_fields():
    assert live_rows({"features": [{}, {"properties": {"status": "Actual"}}]}) == []


# loads


def test_a_replacing_load_refuses_to_shrink_a_table_unless_forced(settings, ctl, con):
    c = contract()
    with run_source(c, settings, ctl=ctl, con=con) as ctx:
        ctx.load("obs", rows(1000))
    with pytest.raises(QualityFailure, match="would shrink"):
        with run_source(c, settings, ctl=ctl, con=con) as ctx:
            ctx.load("obs", rows(300))
    assert con.execute("SELECT count(*) FROM lake.silver.obs").fetchone()[0] == 1000
    with run_source(c, settings, ctl=ctl, con=con) as ctx:
        ctx.load("obs", rows(900))  # a small change is fine
    with run_source(c, settings, ctl=ctl, con=con, force=True) as ctx:
        ctx.load("obs", rows(10))
    assert con.execute("SELECT count(*) FROM lake.silver.obs").fetchone()[0] == 10


def test_one_failed_part_does_not_stop_the_others_but_fails_the_run(settings, ctl, con):
    c = contract(table("merge"))
    with pytest.raises(Retryable, match="down"):
        with run_source(c, settings, ctl=ctl, con=con) as ctx:
            with ctx.part("a"):
                raise Retryable("down")
            with ctx.part("b"):
                ctx.load("obs", rows(5))
    assert con.execute("SELECT count(*) FROM lake.silver.obs").fetchone()[0] == 5
    last = ctl.conn.execute("SELECT * FROM ops.runs ORDER BY started_at DESC LIMIT 1").fetchone()
    assert last["status"] == "failed" and "a" in last["detail"]["failed_parts"]
    with pytest.raises(PartialFailure):
        with run_source(c, settings, ctl=ctl, con=con) as ctx:
            for name in "ab":
                with ctx.part(name):
                    raise Retryable(name)
    failures = ctl.conn.execute("SELECT failures FROM ops.breakers").fetchone()["failures"]
    assert failures == 2  # both runs count against the source


def test_a_run_a_killed_process_left_open_is_closed(settings, ctl, con):
    c = contract()
    ctl.register_source(c)
    dead = uuid.uuid4()
    ctl.start_run(dead, "demo")
    with run_source(c, settings, ctl=ctl, con=con) as ctx:
        ctx.load("obs", rows(1))
    row = ctl.conn.execute("SELECT status, error FROM ops.runs WHERE run_id = %s", (dead,))
    assert row.fetchone()["status"] == "failed"


def test_nothing_is_written_to_a_nearly_full_disk(settings, ctl, con, monkeypatch):
    monkeypatch.setattr("nycynapse_lake.runs.MIN_FREE_BYTES", 10**18)
    with pytest.raises(DiskLow):
        with run_source(contract(), settings, ctl=ctl, con=con) as ctx:
            ctx.load("obs", rows(1))


def test_a_future_date_does_not_hide_that_a_feed_stopped(settings, ctl, con):
    c = contract(table("merge", data_sla="1 hour"))
    old = datetime.now(UTC) - timedelta(hours=5)
    with run_source(c, settings, ctl=ctl, con=con) as ctx:
        ctx.load("obs", pa.table({"id": [1, 2], "at": [old, old + timedelta(days=4000)]}))
    problems = health.data(ctl)
    assert [p.kind for p in problems] == ["source_late"]


def test_a_limit_applies_to_a_source_that_brings_nothing(settings, ctl, con):
    with run_source(contract(), settings, ctl=ctl, con=con) as ctx:
        ctx.load("obs", rows(10, datetime.now(UTC) - timedelta(days=3)))
    assert health.data(ctl) == []
    with run_source(contract(table(data_sla="2 days")), settings, ctl=ctl, con=con):
        pass
    assert [p.kind for p in health.data(ctl)] == ["source_late"]
    with run_source(contract(), settings, ctl=ctl, con=con):
        pass
    assert health.data(ctl) == []


def test_a_late_limit_needs_an_event_time():
    with pytest.raises(ContractError, match="event_time"):
        contract({**table(), "event_time": None, "data_sla": "1 day"})


# health


def test_incidents_open_once_and_resolve(settings, ctl):
    p = health.Problem("k", "x", "warn", "m1")
    assert [o.key for o in health.record(ctl, [p])[0]] == ["k"]
    opened, gone = health.record(ctl, [dataclasses.replace(p, message="m2")])
    assert opened == [] and gone == []
    assert health.record(ctl, []) == ([], ["k"])
    assert [o.key for o in health.record(ctl, [p])[0]] == ["k"]  # it came back: a new incident
    n = ctl.conn.execute("SELECT count(*) AS n FROM ops.incidents").fetchone()["n"]
    assert n == 2


def test_late_failing_and_thin_loads_are_found(settings, ctl):
    c = contract()
    ctl.register_source(c)
    assert [p.kind for p in health.loads(ctl)] == ["load_late"]  # never ran

    def done(status, rows_in, hours):
        rid = uuid.uuid4()
        ctl.start_run(rid, "demo")
        ctl.finish_run(rid, status, stats={"rows_in": rows_in}, error="boom")
        ctl.conn.execute(
            "UPDATE ops.runs SET started_at = now() - make_interval(mins => %s), "
            "finished_at = now() - make_interval(mins => %s) WHERE run_id = %s",
            (hours * 60 + 1, hours * 60, rid),
        )

    for day in range(9, 0, -1):
        done("succeeded", 1000, day * 24)
        done("succeeded", 0, day * 24 - 6)  # the second look of the day finds nothing new
    assert health.volume(ctl) == []
    done("succeeded", 20, 0)
    assert health.loads(ctl) == []
    assert [p.kind for p in health.volume(ctl)] == ["volume_drop"]
    done("failed", 0, 0)
    ctl.conn.execute("UPDATE ops.runs SET started_at = now() WHERE status = 'failed'")
    assert health.loads(ctl) == []  # one failure is not a pattern
    done("failed", 0, 0)
    ctl.conn.execute("UPDATE ops.runs SET started_at = now() WHERE status = 'failed'")
    assert [p.kind for p in health.loads(ctl)] == ["load_failing"]


def test_jobs_that_stop_running_are_found(settings, ctl):
    kinds = {p.key: p.kind for p in health.jobs(ctl)}
    assert kinds["job:backup"] == "job_late"
    ctl.record_job("backup", True, {})
    assert "job:backup" not in {p.key for p in health.jobs(ctl)}
    ctl.record_job("backup", False, {"error": "no space"})
    assert {p.key: p.kind for p in health.jobs(ctl)}["job:backup"] == "job_failed"


def test_one_broken_check_does_not_hide_the_others(settings, ctl, monkeypatch):
    monkeypatch.setattr(health, "data", lambda ctl: 1 / 0)
    settings.data_path.mkdir(parents=True, exist_ok=True)
    keys = {p.key for p in health.run(settings, ctl)}
    assert "check:data" in keys and "job:backup" in keys


# files


def test_a_missing_data_file_is_found(settings, ctl, con):
    with run_source(contract(), settings, ctl=ctl, con=con) as ctx:
        ctx.load("obs", rows(5000))
    report = backup.verify(con)
    assert report["files"] >= 1 and report["missing"] == 0
    path = con.execute(
        "SELECT data_file FROM ducklake_list_files('lake', 'obs', schema => 'silver')"
    ).fetchone()[0]
    os.truncate(path, 10)
    with pytest.raises(RuntimeError, match="missing data"):
        backup.verify(con)
    os.unlink(path)
    with pytest.raises(RuntimeError, match="missing data"):
        backup.verify(con)


def test_replaced_files_outlive_a_week_of_backups(settings, ctl, con):
    c = contract()
    for n in (5000, 4999):
        with run_source(c, settings, ctl=ctl, con=con) as ctx:
            ctx.load("obs", rows(n))
    before = len(list(settings.data_path.rglob("*.parquet")))
    report = maintain.run(con, ctl, keep_days=0)
    assert report["deleted_files"] == 0
    assert len(list(settings.data_path.rglob("*.parquet"))) >= before - 0


def mostly_dead(con, files, rows=200):
    """A table of small files where most rows of each were deleted again."""
    con.execute("CREATE TABLE lake.gold.churn (f INTEGER, a INTEGER)")
    con.execute("CALL lake.set_option('data_inlining_row_limit', 0)")
    for f in range(files):
        con.execute(f"INSERT INTO lake.gold.churn SELECT {f}, range FROM range({rows})")
        con.execute(f"DELETE FROM lake.gold.churn WHERE f = {f} AND a >= 4")
    return con.execute("SELECT count(*), sum(f), sum(a) FROM lake.gold.churn").fetchone()


def test_files_of_deleted_rows_are_rewritten(con):
    before = mostly_dead(con, 25)
    assert maintain._dead_files(con, "gold", "churn", 0.95)[0] == 25
    done, errors = maintain.rewrite(con)
    assert done == {"churn": 25} and not errors
    assert maintain._dead_files(con, "gold", "churn", 0.05) == (0, 0)
    assert con.execute("SELECT count(*), sum(f), sum(a) FROM lake.gold.churn").fetchone() == before
    assert maintain.rewrite(con) == ({}, [])


def test_a_rewrite_too_big_for_memory_is_left_alone(con, monkeypatch):
    mostly_dead(con, 25)
    monkeypatch.setattr(maintain, "REWRITE_STEPS", ((0.95, 1), (0.05, 1)))
    assert maintain.rewrite(con) == ({}, [])


def test_a_rewrite_that_loses_rows_is_not_kept(con):
    before = mostly_dead(con, 25)

    class Lossy:
        """Deletes a row right after every rewrite, inside the same transaction."""

        def execute(self, sql, *args):
            out = con.execute(sql, *args)
            if "ducklake_rewrite_data_files" in sql:
                out.fetchall()
                return con.execute("DELETE FROM lake.gold.churn WHERE f = 3")
            return out

    done, errors = maintain.rewrite(Lossy())
    assert not done and "not kept" in errors[0]
    assert con.execute("SELECT count(*), sum(f), sum(a) FROM lake.gold.churn").fetchone() == before
    assert maintain._dead_files(con, "gold", "churn", 0.95)[0] == 25


def test_live_exports_are_weekly(settings, con, tmp_path):
    s = dataclasses.replace(settings, backup_path=tmp_path / "b")
    con.execute("CREATE TABLE lake.silver.subway_trips (a INTEGER)")
    con.execute("INSERT INTO lake.silver.subway_trips VALUES (1)")
    out = backup.export_live(s, con)
    assert (out / "subway_trips.parquet").exists()
    assert backup.export_live(s, con) is None


def test_settings_never_print_the_password():
    s = Settings(pg_dsn="password=hunter2", data_path=None, tmp_path=None, spool_path=None)
    assert "hunter2" not in repr(s) and "hunter2" not in json.dumps(str(s))


# off-site


@pytest.fixture
def storage(settings, tmp_path, monkeypatch):
    """Encrypted storage on a local folder, the same shape as the real one."""
    if not shutil.which("rclone"):
        pytest.skip("rclone is not installed")
    obscured = subprocess.run(
        ["rclone", "obscure", "not-the-real-key"], capture_output=True, text=True, check=True
    ).stdout.strip()
    conf = tmp_path / "rclone.conf"
    conf.write_text(
        "[plain]\ntype = local\n\n[offsite]\ntype = crypt\n"
        f"remote = plain:{tmp_path / 'cloud'}\nfilename_encryption = off\n"
        f"directory_name_encryption = false\npassword = {obscured}\n"
    )
    monkeypatch.setattr(offsite, "CONFIG", conf)
    monkeypatch.setattr(offsite, "APP_BACKUPS", tmp_path / "app")
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "nycynapse-1.dump").write_bytes(b"app secret")
    s = dataclasses.replace(settings, backup_path=tmp_path / "backups")
    (s.backup_path / "exports" / "20261101").mkdir(parents=True)
    (s.backup_path / "exports" / "20261101" / "a.parquet").write_bytes(b"x" * 100)
    (s.backup_path / "exports" / "20261108.part").mkdir()
    (s.backup_path / "exports" / "20261108.part" / "half.parquet").write_bytes(b"half")
    (s.backup_path / "catalog-20261101-0000.dump").write_bytes(b"catalog secret")
    (s.backup_path / "state-20261101-0000.tar.gz").write_bytes(b"state")
    return s, tmp_path / "cloud"


def test_off_site_copies_are_encrypted_and_outlive_the_ones_here(storage):
    s, cloud = storage
    offsite.run(s)
    stored = [p for p in cloud.rglob("*") if p.is_file()]
    assert len(stored) == 4  # the half-written export stays here
    assert not any(b"secret" in p.read_bytes() for p in stored)
    (s.backup_path / "catalog-20261101-0000.dump").unlink()
    (s.backup_path / "catalog-20261102-0000.dump").write_bytes(b"newer")
    report = offsite.run(s)
    assert report["newest"] == "catalog-20261102-0000.dump"
    assert [f["Path"] for f in offsite._there("catalog")] == [
        "catalog-20261101-0000.dump",
        "catalog-20261102-0000.dump",
    ]


def test_off_site_keeps_only_the_newest(storage, monkeypatch):
    s, _ = storage
    monkeypatch.setitem(offsite.KEEP, "catalog", 2)
    for d in range(2, 6):
        (s.backup_path / f"catalog-2026110{d}-0000.dump").write_bytes(b"c")
    assert offsite.run(s)["removed"] == 3
    assert len(offsite._there("catalog")) == 2


def test_off_site_brings_back_what_was_sent(storage, monkeypatch):
    s, _ = storage
    offsite.run(s)
    seen = []
    monkeypatch.setattr(
        backup, "restore_check", lambda settings, dump: seen.append(dump.read_bytes()) or {}
    )
    offsite.check(s)
    assert seen == [b"catalog secret"]
    (s.backup_path / "catalog-20261101-0000.dump").write_bytes(b"changed here")
    with pytest.raises(RuntimeError, match="different"):
        offsite.check(s)


def test_storage_nobody_signed_in_to_is_not_a_problem(settings, ctl, tmp_path, monkeypatch):
    monkeypatch.setattr(offsite, "CONFIG", tmp_path / "missing.conf")
    assert not offsite.ready()
    assert "job:offsite" not in {p.key for p in health.jobs(ctl)}


def test_off_site_copies_stay_watched_when_the_login_goes_missing(
    settings, ctl, tmp_path, monkeypatch
):
    monkeypatch.setattr(offsite, "CONFIG", tmp_path / "missing.conf")
    ctl.record_job("offsite", True, {})
    ctl.conn.execute("UPDATE ops.jobs SET finished_at = now() - interval '2 days'")
    assert "job:offsite" in {p.key for p in health.jobs(ctl)}
