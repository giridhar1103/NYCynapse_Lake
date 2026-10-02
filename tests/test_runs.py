import httpx
import pyarrow as pa
import pytest

from nycynapse_lake.breaker import CircuitOpen
from nycynapse_lake.contracts import parse
from nycynapse_lake.http import Http
from nycynapse_lake.quality import QualityFailure
from nycynapse_lake.retry import Retryable, RetryPolicy
from nycynapse_lake.runs import SourceBusy, run_source

CONTRACT = parse({
    "source": "demo",
    "version": 2,
    "domain": "test",
    "description": "demo source",
    "cadence": "hourly",
    "freshness_sla": "2 hours",
    "tables": [{
        "name": "obs",
        "description": "observations",
        "grain": "one row per id",
        "primary_key": ["id"],
        "write": "merge",
        "event_time": "at",
        "max_reject_rate": 0.5,
        "columns": [
            {"name": "id", "type": "integer", "nullable": False, "description": "id"},
            {"name": "at", "type": "timestamptz", "nullable": False, "description": "time"},
            {"name": "v", "type": "integer", "min": 0, "description": "value"},
        ],
    }],
})


def data(*rows):
    return pa.table({"id": [r[0] for r in rows], "at": [r[1] for r in rows],
                     "v": [r[2] for r in rows]})


def go(settings, ctl, con, **kw):
    return run_source(CONTRACT, settings, ctl=ctl, con=con, **kw)


def runs(ctl):
    return ctl.conn.execute(
        "SELECT status, rows_in, rows_written, rows_quarantined FROM ops.runs ORDER BY started_at"
    ).fetchall()


def test_successful_run_saves_checkpoint_and_stats(settings, ctl, con):
    with go(settings, ctl, con) as ctx:
        ctx.load("obs", data((1, "2024-01-01T00:00:00Z", 5), (2, "2024-01-02T00:00:00Z", -1)))
        ctx.advance("cursor", {"page": 3})
    assert ctl.get_checkpoint("demo", "cursor") == {"page": 3}
    assert runs(ctl) == [{"status": "succeeded", "rows_in": 2, "rows_written": 1,
                          "rows_quarantined": 1}]
    q = ctl.conn.execute("SELECT reason, record FROM ops.quarantine").fetchone()
    assert q["reason"] == "v below 0" and q["record"]["id"] == 2
    fresh = ctl.conn.execute("SELECT * FROM ops.table_freshness").fetchone()
    assert fresh["table_name"] == "lake.silver.obs" and fresh["row_count"] == 1


def test_failed_run_keeps_old_checkpoint(settings, ctl, con):
    with go(settings, ctl, con) as ctx:
        ctx.advance("cursor", 1)
    with pytest.raises(RuntimeError), go(settings, ctl, con) as ctx:
        ctx.advance("cursor", 2)
        raise RuntimeError("parser bug")
    assert ctl.get_checkpoint("demo", "cursor") == 1
    assert [r["status"] for r in runs(ctl)] == ["succeeded", "failed"]


def test_quality_failure_writes_nothing(settings, ctl, con):
    with pytest.raises(QualityFailure), go(settings, ctl, con) as ctx:
        ctx.load("obs", data((1, "2024-01-01T00:00:00Z", -1), (2, "bad", 1)))
    exists = con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = 'obs'"
    ).fetchone()[0]
    assert exists == 0
    dq = ctl.conn.execute("SELECT count(*) AS n FROM ops.dq_results WHERE NOT passed").fetchone()
    assert dq["n"] >= 1


def test_rerunning_a_batch_does_not_duplicate(settings, ctl, con):
    batch = data((1, "2024-01-01T00:00:00Z", 5), (2, "2024-01-01T01:00:00Z", 6))
    for _ in range(2):
        with go(settings, ctl, con) as ctx:
            ctx.load("obs", batch)
    assert con.execute("SELECT count(*) FROM lake.silver.obs").fetchone()[0] == 2


def test_same_source_cannot_run_twice_at_once(settings, ctl, con):
    from nycynapse_lake.control.db import Control

    other = Control(settings.pg_dsn)
    assert other.try_lock("demo")
    try:
        with pytest.raises(SourceBusy), go(settings, ctl, con):
            pass
    finally:
        other.unlock("demo")
        other.close()


def test_breaker_opens_after_repeated_upstream_failures(settings, ctl, con):
    def down(req):
        return httpx.Response(503)

    http = Http(policy=RetryPolicy(attempts=1), transport=httpx.MockTransport(down))
    for _ in range(3):
        with pytest.raises(Retryable), go(settings, ctl, con, http=http) as ctx:
            ctx.http.get("https://example.test/feed")
    with pytest.raises(CircuitOpen), go(settings, ctl, con, http=http):
        pass
    assert [r["status"] for r in runs(ctl)] == ["failed"] * 3 + ["skipped"]
