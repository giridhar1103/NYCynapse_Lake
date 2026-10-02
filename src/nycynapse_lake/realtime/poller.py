"""Long-running poller for the subway realtime feeds.

Feeds are read every 30 seconds. Derived rows are written to the lake every five minutes as
one run, so each write shows up in ops.runs like any batch load. If the write fails, the rows
go to the local spool and are written on a later flush. Tracker state is saved to disk after
every flush, so a restart picks trains up where it left off.
"""

import json
import logging
import os
import signal
import threading
import time
from datetime import UTC, datetime

import pyarrow as pa
from google.transit import gtfs_realtime_pb2

from .. import lake
from ..config import Settings
from ..contracts import Contract
from ..control.db import Control
from ..http import Http
from ..retry import RetryPolicy
from ..runs import run_source
from ..spool import Spool
from .subway import Tracker, alert_rows

log = logging.getLogger(__name__)

BASE = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/"
FEEDS = {
    "1234567": "nyct%2Fgtfs",
    "ace": "nyct%2Fgtfs-ace",
    "bdfm": "nyct%2Fgtfs-bdfm",
    "g": "nyct%2Fgtfs-g",
    "jz": "nyct%2Fgtfs-jz",
    "nqrw": "nyct%2Fgtfs-nqrw",
    "l": "nyct%2Fgtfs-l",
    "si": "nyct%2Fgtfs-si",
}
ALERTS = BASE + "camsys%2Fsubway-alerts.json"
GAP_SECONDS = 90

ALERT_SCHEMA = pa.schema(
    [
        ("alert_id", pa.string()),
        ("alert_type", pa.string()),
        ("header_text", pa.string()),
        ("description_text", pa.string()),
        ("route_ids", pa.list_(pa.string())),
        ("stop_ids", pa.list_(pa.string())),
        (
            "active_periods",
            pa.list_(
                pa.struct(
                    [
                        ("starts_at", pa.timestamp("s", tz="UTC")),
                        ("ends_at", pa.timestamp("s", tz="UTC")),
                    ]
                )
            ),
        ),
        ("created_at", pa.timestamp("s", tz="UTC")),
        ("updated_at", pa.timestamp("s", tz="UTC")),
        ("first_seen_at", pa.timestamp("s", tz="UTC")),
        ("last_seen_at", pa.timestamp("s", tz="UTC")),
    ]
)


class SubwayPoller:
    def __init__(
        self,
        settings: Settings,
        contract: Contract,
        *,
        interval: int = 30,
        flush_every: int = 300,
        alerts_every: int = 60,
    ):
        self.settings = settings
        self.contract = contract
        self.interval = interval
        self.flush_every = flush_every
        self.alerts_every = alerts_every
        self.http = Http(policy=RetryPolicy(attempts=2, base=1, cap=3), timeout=15)
        self.ctl = Control(settings.pg_dsn)
        self.con = None
        self.spool = Spool(settings.spool_path, contract.source)
        self.tracker = Tracker()
        self.alerts: dict[str, dict] = {}
        self.dirty_alerts: set[str] = set()
        self.down_since: dict[str, int] = {}
        self.stop = threading.Event()
        settings.state_path.mkdir(parents=True, exist_ok=True)
        self.state_file = settings.state_path / f"{contract.source}.json"

    # lifecycle

    def run_forever(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.stop.set())
        signal.signal(signal.SIGINT, lambda *_: self.stop.set())
        self.restore()
        next_flush = time.time() + self.flush_every
        next_alerts = 0.0
        while not self.stop.is_set():
            started = time.time()
            self.poll_feeds(int(started))
            if started >= next_alerts:
                self.poll_alerts(int(started))
                next_alerts = started + self.alerts_every
            if started >= next_flush:
                self.flush()
                next_flush = started + self.flush_every
            self.stop.wait(max(0.0, self.interval - (time.time() - started)))
        log.info("stopping, writing what is left")
        self.flush()

    # polling

    def poll_feeds(self, now: int) -> None:
        for name, path in FEEDS.items():
            try:
                body = self.http.get(BASE + path).content
                message = gtfs_realtime_pb2.FeedMessage()
                message.ParseFromString(body)
            except Exception as exc:  # noqa: BLE001 - one bad feed must not stop the rest
                self.down_since.setdefault(name, now)
                log.warning("feed %s failed: %s", name, exc)
                continue
            self.tracker.observe(name, message, now)
            self._recovered(name, now)

    def poll_alerts(self, now: int) -> None:
        try:
            payload = self.http.get_json(ALERTS)
        except Exception as exc:  # noqa: BLE001
            self.down_since.setdefault("alerts", now)
            log.warning("alerts failed: %s", exc)
            return
        stamp = datetime.fromtimestamp(now, UTC)
        for row in alert_rows(payload):
            known = self.alerts.get(row["alert_id"])
            row["first_seen_at"] = known["first_seen_at"] if known else stamp
            row["last_seen_at"] = stamp
            self.alerts[row["alert_id"]] = row
            self.dirty_alerts.add(row["alert_id"])
        self._recovered("alerts", now)

    def _recovered(self, name: str, now: int) -> None:
        since = self.down_since.pop(name, None)
        if since is not None and now - since >= GAP_SECONDS:
            self.ctl.record_gap(
                f"{self.contract.source}:{name}", _dt(since), _dt(now), "feed unavailable"
            )

    # writing

    def flush(self) -> None:
        events, trips = self.tracker.take()
        alerts = [self.alerts[a] for a in sorted(self.dirty_alerts)]
        self.dirty_alerts.clear()
        batches = {
            "subway_stop_events": pa.Table.from_pylist(events) if events else None,
            "subway_trips": pa.Table.from_pylist(trips) if trips else None,
            "subway_alerts": pa.Table.from_pylist(alerts, ALERT_SCHEMA) if alerts else None,
        }
        batches = {k: v for k, v in batches.items() if v is not None}
        try:
            if self.con is None:
                self.con = lake.connect(self.settings)
                lake.ensure_schemas(self.con)
            with run_source(
                self.contract, self.settings, ctl=self.ctl, con=self.con, http=self.http
            ) as ctx:
                ctx.detail["spooled_batches"] = self.spool.drain(
                    lambda table, batch, meta: ctx.load(table, batch)
                )
                for table, batch in batches.items():
                    ctx.load(table, batch)
                ctx.detail["active_trips"] = len(self.tracker.trips)
        except Exception as exc:  # noqa: BLE001 - keep the rows, retry on the next flush
            log.error("flush failed, spooling %d batches: %s", len(batches), exc)
            for table, batch in batches.items():
                self.spool.put(table, batch)
            if self.con is not None:
                self.con.close()
                self.con = None
        self.save()
        self._forget_old_alerts()

    def _forget_old_alerts(self) -> None:
        cutoff = datetime.now(UTC).timestamp() - 2 * 86400
        for alert_id in [
            a for a, r in self.alerts.items() if r["last_seen_at"].timestamp() < cutoff
        ]:
            del self.alerts[alert_id]

    # state

    def save(self) -> None:
        tmp = self.state_file.with_suffix(".tmp")
        alerts = {
            k: {
                **v,
                "first_seen_at": v["first_seen_at"].isoformat(),
                "last_seen_at": v["last_seen_at"].isoformat(),
            }
            for k, v in self.alerts.items()
        }
        tmp.write_text(
            json.dumps(
                {
                    "saved_at": int(time.time()),
                    "trips": self.tracker.dump(),
                    "alert_first_seen": {k: v["first_seen_at"] for k, v in alerts.items()},
                }
            )
        )
        os.replace(tmp, self.state_file)

    def restore(self) -> None:
        if not self.state_file.exists():
            return
        data = json.loads(self.state_file.read_text())
        now = int(time.time())
        saved = data.get("saved_at", now)
        if now - saved > 3 * 3600:
            log.warning("state is %d minutes old, starting fresh", (now - saved) // 60)
        else:
            self.tracker.load(data.get("trips", {}))
            for alert_id, first in data.get("alert_first_seen", {}).items():
                self.alerts[alert_id] = {
                    "first_seen_at": datetime.fromisoformat(first),
                    "last_seen_at": datetime.fromisoformat(first),
                }
        if now - saved >= GAP_SECONDS:
            self.ctl.record_gap(self.contract.source, _dt(saved), _dt(now), "poller not running")
        log.info("restored %d trips", len(self.tracker.trips))


def _dt(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, UTC)
