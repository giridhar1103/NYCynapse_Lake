"""Poller for Citi Bike's live station feeds."""

import logging
from datetime import UTC, datetime

import pyarrow as pa

from .. import geo
from .base import Poller

log = logging.getLogger(__name__)

BASE = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/"
VALID_SINCE = 1_577_836_800  # 2020-01-01. Uninstalled stations report a placeholder time.
STATION_EVERY = 3600
FIELDS = (
    "bikes_available",
    "ebikes_available",
    "bikes_disabled",
    "docks_available",
    "docks_disabled",
    "is_installed",
    "is_renting",
    "is_returning",
)


def _dt(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, UTC)


def status_rows(payload: dict, last: dict[str, list], now: int) -> list[dict]:
    """Rows for stations whose values differ from the last ones kept. Updates `last`."""
    out = []
    for s in payload.get("data", {}).get("stations", []):
        values = [
            s.get("num_bikes_available", 0),
            s.get("num_ebikes_available"),
            s.get("num_bikes_disabled"),
            s.get("num_docks_available", 0),
            s.get("num_docks_disabled"),
            bool(s.get("is_installed")),
            bool(s.get("is_renting")),
            bool(s.get("is_returning")),
        ]
        sid = s["station_id"]
        if last.get(sid) == values:
            continue
        last[sid] = values
        reported = s.get("last_reported") or 0
        out.append(
            {
                "station_id": sid,
                "reported_at": _dt(reported if reported >= VALID_SINCE else now),
                **dict(zip(FIELDS, values, strict=True)),
                "polled_at": _dt(now),
            }
        )
    return out


def station_rows(
    info: dict, regions: dict | None, first_seen: dict[str, int], now: int
) -> list[dict]:
    names = {r["region_id"]: r["name"] for r in (regions or {}).get("data", {}).get("regions", [])}
    out = []
    for s in info.get("data", {}).get("stations", []):
        sid = s["station_id"]
        first = first_seen.setdefault(sid, now)
        out.append(
            {
                "station_id": sid,
                "short_name": s.get("short_name"),
                "name": s.get("name"),
                "latitude": s.get("lat"),
                "longitude": s.get("lon"),
                "capacity": s.get("capacity"),
                "region": names.get(s.get("region_id")),
                "has_charging": s.get("is_charging"),
                "first_seen_at": _dt(first),
                "last_seen_at": _dt(now),
            }
        )
    return out


class GbfsPoller(Poller):
    interval = 60

    def __init__(self, settings, contract):
        super().__init__(settings, contract)
        self.last: dict[str, list] = {}
        self.first_seen: dict[str, int] = {}
        self.status: list[dict] = []
        self.stations: list[dict] = []
        self.next_stations = 0
        self.transforms = {
            "bike_station": lambda rel: geo.tag_points(
                f"SELECT * FROM {rel}", lon="longitude", lat="latitude"
            )
        }

    def poll(self, now: int) -> None:
        payload = self.fetch("station_status", BASE + "station_status.json", now, json_body=True)
        if payload is not None:
            self.status += status_rows(payload, self.last, now)
        if now >= self.next_stations:
            info = self.fetch(
                "station_information", BASE + "station_information.json", now, json_body=True
            )
            if info is not None:
                regions = self.fetch(
                    "system_regions", BASE + "system_regions.json", now, json_body=True
                )
                self.stations = station_rows(info, regions, self.first_seen, now)
                self.next_stations = now + STATION_EVERY

    def take(self) -> dict[str, pa.Table]:
        status, stations = self.status, self.stations
        self.status, self.stations = [], []
        return {
            "bike_station_status": pa.Table.from_pylist(status) if status else None,
            "bike_station": pa.Table.from_pylist(stations) if stations else None,
        }

    def describe(self) -> dict:
        return {"stations_tracked": len(self.last)}

    def dump_state(self) -> dict:
        return {"last": self.last, "first_seen": self.first_seen}

    def load_state(self, data: dict) -> None:
        self.last = data.get("last", {})
        self.first_seen = data.get("first_seen", {})
        log.info("restored %d stations", len(self.last))
