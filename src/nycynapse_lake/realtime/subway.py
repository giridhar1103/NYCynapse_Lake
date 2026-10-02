"""Turn GTFS-realtime subway polls into stop events.

The MTA feed lists, for each active train, the stops it has not left yet with predicted times.
It never says "train X arrived at stop Y". The tracker keeps every train's stops between polls:
when a stop drops off a train's list, the train has left it. If the last prediction for that
stop was already due, or the train reported standing there, it arrived. If the stop vanished
while still in the future, it was skipped. When a whole trip leaves the feed, its remaining
stops are settled the same way and a trip summary is written.
"""

import re
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime

STOPPED_AT = 1
DIRECTION = re.compile(r"_[^.]*\.+([NS])")


def _ts(epoch: int | None) -> datetime | None:
    return datetime.fromtimestamp(epoch, UTC) if epoch else None


@dataclass
class StopState:
    first_pred: int | None = None
    last_arr: int | None = None
    last_dep: int | None = None
    stopped_at: int | None = None
    seen: int = 0


@dataclass
class TripState:
    trip_id: str
    service_date: str
    route_id: str
    direction: str | None
    scheduled_start: str | None
    feed: str
    first_seen: int
    last_seen: int
    stops: dict[str, StopState] = field(default_factory=dict)
    done: list[str] = field(default_factory=list)
    arrived: int = 0
    skipped: int = 0
    unreached: int = 0

    @property
    def uid(self) -> str:
        return f"{self.service_date}/{self.trip_id}"


class Tracker:
    def __init__(self, *, grace: int = 60, gone_after: int = 180):
        self.grace = grace  # a prediction this close to the poll time counts as due
        self.gone_after = gone_after  # seconds a trip may be missing before it is settled
        self.trips: dict[str, TripState] = {}
        self.events: list[dict] = []
        self.finished: list[dict] = []

    # feed handling

    def observe(self, feed: str, message, now: int) -> None:
        ts = message.header.timestamp or now
        vehicles = {}
        for e in message.entity:
            if e.HasField("vehicle") and e.vehicle.trip.trip_id:
                v = e.vehicle
                vehicles[(v.trip.trip_id, v.trip.start_date)] = v

        seen = set()
        for e in message.entity:
            if not e.HasField("trip_update"):
                continue
            tu = e.trip_update
            trip = tu.trip
            if not trip.trip_id or not trip.start_date:
                continue
            service_date = date(
                int(trip.start_date[:4]), int(trip.start_date[4:6]), int(trip.start_date[6:])
            ).isoformat()
            uid = f"{service_date}/{trip.trip_id}"
            seen.add(uid)
            state = self.trips.get(uid)
            if state is None:
                m = DIRECTION.search(trip.trip_id)
                state = TripState(
                    trip_id=trip.trip_id,
                    service_date=service_date,
                    route_id=trip.route_id,
                    direction=m.group(1) if m else None,
                    scheduled_start=trip.start_time or None,
                    feed=feed,
                    first_seen=ts,
                    last_seen=ts,
                )
                self.trips[uid] = state

            current = []
            for stu in tu.stop_time_update:
                if not stu.stop_id or stu.stop_id in state.done:
                    continue
                current.append(stu.stop_id)
                s = state.stops.setdefault(stu.stop_id, StopState())
                arr = stu.arrival.time or None
                dep = stu.departure.time or None
                if s.first_pred is None:
                    s.first_pred = arr or dep
                s.last_arr = arr or s.last_arr
                s.last_dep = dep or s.last_dep
                s.seen += 1

            v = vehicles.get((trip.trip_id, trip.start_date))
            if v is not None and v.current_status == STOPPED_AT and v.stop_id in state.stops:
                s = state.stops[v.stop_id]
                stamp = v.timestamp or ts
                s.stopped_at = min(s.stopped_at, stamp) if s.stopped_at else stamp

            # An empty list is usually a train waiting to be assigned, not one that left
            # every stop, so it settles nothing.
            if current:
                for stop_id in [s for s in state.stops if s not in current and s not in state.done]:
                    self._settle(state, stop_id, ts, trip_over=False)
            state.last_seen = ts

        for uid in [
            u
            for u, t in self.trips.items()
            if t.feed == feed and u not in seen and ts - t.last_seen > self.gone_after
        ]:
            self._finish(self.trips.pop(uid), ts)

    def _settle(self, trip: TripState, stop_id: str, ts: int, *, trip_over: bool) -> None:
        s = trip.stops[stop_id]
        due = s.last_arr or s.last_dep
        reference = trip.last_seen if trip_over else ts
        if s.stopped_at or (due is not None and due <= reference + self.grace):
            outcome, arrived = "arrived", s.stopped_at or due
            source = "vehicle_stopped" if s.stopped_at else "last_prediction"
            trip.arrived += 1
        else:
            outcome, arrived, source = ("unreached" if trip_over else "skipped"), None, None
            if trip_over:
                trip.unreached += 1
            else:
                trip.skipped += 1
        trip.done.append(stop_id)
        self.events.append(
            {
                "trip_uid": trip.uid,
                "service_date": trip.service_date,
                "trip_id": trip.trip_id,
                "route_id": trip.route_id,
                "direction": trip.direction or (stop_id[-1] if stop_id[-1] in "NS" else None),
                "stop_id": stop_id,
                "station_id": stop_id[:-1] if stop_id[-1] in "NS" else stop_id,
                "outcome": outcome,
                "arrived_at": _ts(arrived),
                "departed_at": _ts(s.last_dep) if outcome == "arrived" else None,
                "arrival_source": source,
                "first_predicted_at": _ts(s.first_pred),
                "predictions_seen": s.seen,
                "feed": trip.feed,
                "recorded_at": _ts(ts),
            }
        )

    def _finish(self, trip: TripState, ts: int) -> None:
        for stop_id in [s for s in trip.stops if s not in trip.done]:
            self._settle(trip, stop_id, ts, trip_over=True)
        self.finished.append(
            {
                "trip_uid": trip.uid,
                "service_date": trip.service_date,
                "trip_id": trip.trip_id,
                "route_id": trip.route_id,
                "direction": trip.direction,
                "scheduled_start": trip.scheduled_start,
                "first_seen_at": _ts(trip.first_seen),
                "last_seen_at": _ts(trip.last_seen),
                "stops_arrived": trip.arrived,
                "stops_skipped": trip.skipped,
                "stops_unreached": trip.unreached,
                "feed": trip.feed,
            }
        )

    # output and persistence

    def take(self) -> tuple[list[dict], list[dict]]:
        events, finished = self.events, self.finished
        self.events, self.finished = [], []
        return events, finished

    def dump(self) -> dict:
        return {uid: asdict(t) for uid, t in self.trips.items()}

    def load(self, data: dict) -> None:
        self.trips = {}
        for uid, raw in data.items():
            stops = {k: StopState(**v) for k, v in raw.pop("stops").items()}
            self.trips[uid] = TripState(**raw, stops=stops)


def alert_rows(payload: dict) -> list[dict]:
    """Flatten the MTA's JSON alert feed into one dict per alert."""
    rows = []
    for e in payload.get("entity", []):
        a = e.get("alert")
        if not a:
            continue
        mercury = a.get("transit_realtime.mercury_alert", {})
        entities = a.get("informed_entity", [])
        rows.append(
            {
                "alert_id": e["id"],
                "alert_type": mercury.get("alert_type"),
                "header_text": _english(a.get("header_text")),
                "description_text": _english(a.get("description_text")),
                "route_ids": sorted({x["route_id"] for x in entities if x.get("route_id")}),
                "stop_ids": sorted({x["stop_id"] for x in entities if x.get("stop_id")}),
                "active_periods": [
                    {"starts_at": _ts(p.get("start")), "ends_at": _ts(p.get("end"))}
                    for p in a.get("active_period", [])
                ],
                "created_at": _ts(mercury.get("created_at")),
                "updated_at": _ts(mercury.get("updated_at")),
            }
        )
    return rows


def _english(text: dict | None) -> str | None:
    for t in (text or {}).get("translation", []):
        if t.get("language") == "en":
            return t.get("text")
    return None
