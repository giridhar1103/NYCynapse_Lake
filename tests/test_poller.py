import dataclasses
import time

import pyarrow as pa

from nycynapse_lake.contracts import parse
from nycynapse_lake.realtime.base import Poller

CONTRACT = parse(
    {
        "source": "demo_live",
        "version": 1,
        "domain": "test",
        "description": "d",
        "cadence": "every minute",
        "freshness_sla": "5 minutes",
        "tables": [
            {
                "name": "reading",
                "description": "r",
                "grain": "g",
                "primary_key": ["id"],
                "write": "append_new",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False, "description": "id"},
                    {"name": "doubled", "type": "INTEGER", "description": "derived in SQL"},
                ],
            }
        ],
    }
)


class Demo(Poller):
    def __init__(self, settings):
        super().__init__(settings, CONTRACT)
        self.rows = []
        self.transforms = {"reading": lambda rel: f"SELECT id, id * 2 AS doubled FROM {rel}"}

    def poll(self, now):
        self.rows.append({"id": len(self.rows) + 1})

    def take(self):
        rows, self.rows = self.rows, []
        return {"reading": pa.Table.from_pylist(rows) if rows else None}


def test_flush_applies_transforms_and_replays_the_spool(settings, ctl, con):
    p = Demo(settings)
    p.con = con
    # A batch left in the spool by an earlier failed flush, without the derived column.
    p.spool.put("reading", pa.table({"id": [10]}))
    p.poll(0)
    p.poll(0)
    p.flush()
    rows = con.execute("SELECT id, doubled FROM lake.silver.reading ORDER BY id").fetchall()
    assert rows == [(1, 2), (2, 4), (10, 20)]
    assert p.spool.pending() == []
    assert p.state_file.exists()


def test_a_batch_that_breaks_its_contract_is_set_aside_and_can_be_replayed(settings, ctl, con):
    p = Demo(dataclasses.replace(settings))
    p.con = con
    good = p.transforms
    p.transforms = {"reading": lambda rel: f"SELECT id FROM {rel}"}  # breaks the contract
    p.poll(0)
    p.flush()
    assert (len(p.spool.pending()), len(p.spool.dead())) == (0, 1)
    p.transforms = good
    p.poll(0)
    p.flush()  # later batches are not held up
    assert p.spool.revive() == 1
    p.flush()
    assert con.execute("SELECT count(*) FROM lake.silver.reading").fetchone()[0] >= 1
    assert p.spool.pending() == p.spool.dead() == []


def test_an_unreachable_lake_keeps_every_batch(settings, ctl, con, monkeypatch):
    p = Demo(settings)

    def down(*a, **k):
        raise ConnectionError("no lake")

    monkeypatch.setattr("nycynapse_lake.realtime.base.lake.connect", down)
    for _ in range(3):
        p.poll(0)
        p.flush()
    assert len(p.spool.pending()) == 3
    p.con = con
    p.flush()
    assert p.spool.pending() == []


def test_a_damaged_state_file_does_not_stop_the_start(settings, ctl):
    p = Demo(settings)
    p.state_file.write_text('{"saved_at": 1')
    p.restore()
    assert p.state_file.with_suffix(".damaged").exists()
    assert not p.state_file.exists()


def test_a_feed_that_keeps_answering_with_an_old_message_is_down(settings, ctl):
    p = Demo(settings)
    now = int(time.time())
    assert not p.fresh("a", now - 3600, now)
    assert "a" in p.down_since
    assert p.fresh("a", now + 200, now + 200)
    assert p.down_since == {}
    assert ctl.conn.execute("SELECT count(*) AS n FROM ops.feed_gaps").fetchone()["n"] == 1


def test_a_poll_that_raises_does_not_end_the_process(settings, ctl, con):
    p = Demo(settings)
    p.con = con
    p.interval = 0
    calls = []

    def poll(now):
        calls.append(now)
        if len(calls) >= 4:
            p.stop.set()
        raise ValueError("odd message")

    p.poll = poll
    p.run_forever()
    assert len(calls) == 4


def test_subway_take_returns_alerts_and_forgets_old_ones(settings, ctl):
    from nycynapse_lake.config import REPO_CONTRACTS
    from nycynapse_lake.contracts import load
    from nycynapse_lake.realtime.poller import SubwayPoller

    p = SubwayPoller(settings, load(REPO_CONTRACTS / "subway_realtime.yaml"))
    p.alert_seen["gone"] = ["2020-01-01T00:00:00+00:00", "2020-01-01T00:00:00+00:00"]
    p._alerts(
        {
            "entity": [
                {
                    "id": "a1",
                    "alert": {
                        "header_text": {"translation": [{"language": "en", "text": "[A] delays"}]}
                    },
                }
            ]
        },
        int(time.time()),
    )
    batches = p.take()
    assert batches["subway_alerts"].column("alert_id").to_pylist() == ["a1"]
    assert batches["subway_stop_events"] is None
    assert "gone" not in p.alert_seen and "a1" in p.alert_seen
    assert p.take()["subway_alerts"] is None
