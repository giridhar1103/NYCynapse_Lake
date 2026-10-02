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


def test_failed_flush_keeps_rows_in_the_spool(settings, ctl, con):
    p = Demo(dataclasses.replace(settings))
    p.con = con
    p.transforms = {"reading": lambda rel: f"SELECT id FROM {rel}"}  # breaks the contract
    p.poll(0)
    p.flush()
    assert len(p.spool.pending()) == 1


def test_subway_take_returns_alerts_and_forgets_old_ones(settings, ctl):
    from nycynapse_lake.config import REPO_CONTRACTS
    from nycynapse_lake.contracts import load
    from nycynapse_lake.realtime.poller import SubwayPoller

    p = SubwayPoller(settings, load(REPO_CONTRACTS / "subway_realtime.yaml"))
    p.alert_seen["gone"] = ["2020-01-01T00:00:00+00:00", "2020-01-01T00:00:00+00:00"]
    p._alerts({"entity": [{"id": "a1", "alert": {"header_text": {"translation": [
        {"language": "en", "text": "[A] delays"}]}}}]}, int(time.time()))
    batches = p.take()
    assert batches["subway_alerts"].column("alert_id").to_pylist() == ["a1"]
    assert batches["subway_stop_events"] is None
    assert "gone" not in p.alert_seen and "a1" in p.alert_seen
    assert p.take()["subway_alerts"] is None
