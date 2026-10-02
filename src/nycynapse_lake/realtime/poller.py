"""Poller for the subway realtime feeds: trip updates every 30 seconds, alerts every minute."""

import logging
from datetime import UTC, datetime

import pyarrow as pa
from google.transit import gtfs_realtime_pb2

from .base import Poller
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
TS = pa.timestamp("s", tz="UTC")
ALERT_SCHEMA = pa.schema(
    [
        ("alert_id", pa.string()),
        ("alert_type", pa.string()),
        ("header_text", pa.string()),
        ("description_text", pa.string()),
        ("route_ids", pa.list_(pa.string())),
        ("stop_ids", pa.list_(pa.string())),
        ("active_periods", pa.list_(pa.struct([("starts_at", TS), ("ends_at", TS)]))),
        ("created_at", TS),
        ("updated_at", TS),
        ("first_seen_at", TS),
        ("last_seen_at", TS),
    ]
)


class SubwayPoller(Poller):
    alerts_every = 60

    def __init__(self, settings, contract):
        super().__init__(settings, contract)
        self.tracker = Tracker()
        self.alerts: dict[str, dict] = {}
        self.alert_seen: dict[str, list[str]] = {}  # alert id: [first seen, last seen]
        self.dirty_alerts: set[str] = set()
        self.next_alerts = 0

    def poll(self, now: int) -> None:
        for name, path in FEEDS.items():
            body = self.fetch(name, BASE + path, now)
            if body is None:
                continue
            message = gtfs_realtime_pb2.FeedMessage()
            try:
                message.ParseFromString(body)
            except Exception as exc:  # noqa: BLE001 - a truncated message is skipped
                log.warning("feed %s did not parse: %s", name, exc)
                continue
            self.tracker.observe(name, message, now)
        if now >= self.next_alerts:
            self.next_alerts = now + self.alerts_every
            payload = self.fetch("alerts", ALERTS, now, json_body=True)
            if payload is not None:
                self._alerts(payload, now)

    def _alerts(self, payload: dict, now: int) -> None:
        stamp = datetime.fromtimestamp(now, UTC)
        for row in alert_rows(payload):
            seen = self.alert_seen.setdefault(row["alert_id"], [stamp.isoformat(), ""])
            seen[1] = stamp.isoformat()
            row["first_seen_at"] = datetime.fromisoformat(seen[0])
            row["last_seen_at"] = stamp
            self.alerts[row["alert_id"]] = row
            self.dirty_alerts.add(row["alert_id"])

    def take(self) -> dict[str, pa.Table]:
        events, trips = self.tracker.take()
        alerts = [self.alerts.pop(a) for a in sorted(self.dirty_alerts)]
        self.dirty_alerts.clear()
        # Forget alerts gone from the feed for two days. One that comes back after that
        # starts a new first_seen_at.
        cutoff = datetime.now(UTC).timestamp() - 2 * 86400
        self.alert_seen = {
            k: v
            for k, v in self.alert_seen.items()
            if datetime.fromisoformat(v[1]).timestamp() > cutoff
        }
        return {
            "subway_stop_events": pa.Table.from_pylist(events) if events else None,
            "subway_trips": pa.Table.from_pylist(trips) if trips else None,
            "subway_alerts": pa.Table.from_pylist(alerts, ALERT_SCHEMA) if alerts else None,
        }

    def describe(self) -> dict:
        return {"active_trips": len(self.tracker.trips)}

    def dump_state(self) -> dict:
        return {"trips": self.tracker.dump(), "alert_seen": self.alert_seen}

    def load_state(self, data: dict) -> None:
        self.tracker.load(data.get("trips", {}))
        self.alert_seen = data.get("alert_seen", {})
        log.info("restored %d trips", len(self.tracker.trips))
