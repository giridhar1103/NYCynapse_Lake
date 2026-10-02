from google.transit import gtfs_realtime_pb2 as rt

from nycynapse_lake.realtime.subway import Tracker, alert_rows

T0 = 1_790_000_000
TRIP = "087450_A..N55R"


def feed(ts, trips):
    """trips: list of (trip_id, [(stop_id, arrival, departure)], vehicle) where vehicle is
    None or (stop_id, status, timestamp)."""
    m = rt.FeedMessage()
    m.header.gtfs_realtime_version = "1.0"
    m.header.timestamp = ts
    for trip_id, stops, vehicle in trips:
        e = m.entity.add(id=f"tu-{trip_id}")
        e.trip_update.trip.trip_id = trip_id
        e.trip_update.trip.start_date = "20261002"
        e.trip_update.trip.route_id = "A"
        e.trip_update.trip.start_time = "14:34:30"
        for stop_id, arr, dep in stops:
            s = e.trip_update.stop_time_update.add(stop_id=stop_id)
            if arr:
                s.arrival.time = arr
            if dep:
                s.departure.time = dep
        if vehicle:
            v = m.entity.add(id=f"v-{trip_id}").vehicle
            v.trip.trip_id = trip_id
            v.trip.start_date = "20261002"
            v.stop_id, v.current_status, v.timestamp = vehicle
    return m


def by_stop(events):
    return {e["stop_id"]: e for e in events}


def test_stop_that_drops_off_after_its_time_counts_as_arrived():
    t = Tracker()
    t.observe(
        "ace", feed(T0, [(TRIP, [("A03N", T0 + 30, T0 + 30), ("A02N", T0 + 120, None)], None)]), T0
    )
    t.observe("ace", feed(T0 + 60, [(TRIP, [("A02N", T0 + 125, None)], None)]), T0 + 60)
    events, _ = t.take()
    assert len(events) == 1
    e = events[0]
    assert e["stop_id"] == "A03N" and e["outcome"] == "arrived"
    assert e["arrival_source"] == "last_prediction"
    assert int(e["arrived_at"].timestamp()) == T0 + 30
    assert e["trip_uid"] == f"2026-10-02/{TRIP}"
    assert e["direction"] == "N" and e["station_id"] == "A03"


def test_vehicle_standing_at_the_stop_is_preferred():
    t = Tracker()
    t.observe("ace", feed(T0, [(TRIP, [("A03N", T0 + 30, T0 + 40)], ("A03N", 2, T0))]), T0)
    t.observe(
        "ace",
        feed(
            T0 + 30,
            [(TRIP, [("A03N", T0 + 30, T0 + 40), ("A02N", T0 + 150, None)], ("A03N", 1, T0 + 25))],
        ),
        T0 + 30,
    )
    t.observe("ace", feed(T0 + 60, [(TRIP, [("A02N", T0 + 150, None)], None)]), T0 + 60)
    (e,) = t.take()[0]
    assert e["arrival_source"] == "vehicle_stopped"
    assert int(e["arrived_at"].timestamp()) == T0 + 25
    assert int(e["departed_at"].timestamp()) == T0 + 40


def test_future_stop_that_vanishes_is_skipped():
    t = Tracker()
    t.observe(
        "ace", feed(T0, [(TRIP, [("A03N", T0 + 30, None), ("A02N", T0 + 600, None)], None)]), T0
    )
    t.observe("ace", feed(T0 + 30, [(TRIP, [("A03N", T0 + 30, None)], None)]), T0 + 30)
    (e,) = t.take()[0]
    assert e["stop_id"] == "A02N" and e["outcome"] == "skipped" and e["arrived_at"] is None


def test_empty_update_settles_nothing():
    t = Tracker()
    t.observe("ace", feed(T0, [(TRIP, [("A03N", T0 + 30, None)], None)]), T0)
    t.observe("ace", feed(T0 + 30, [(TRIP, [], None)]), T0 + 30)
    assert t.take() == ([], [])


def test_trip_that_leaves_the_feed_is_finished():
    t = Tracker(gone_after=60)
    t.observe(
        "ace", feed(T0, [(TRIP, [("A03N", T0 + 10, None), ("A02N", T0 + 900, None)], None)]), T0
    )
    t.observe(
        "ace",
        feed(
            T0 + 30,
            [],
        ),
        T0 + 30,
    )
    assert t.take() == ([], [])
    t.observe("ace", feed(T0 + 120, []), T0 + 120)
    events, trips = t.take()
    outcomes = {e["stop_id"]: e["outcome"] for e in events}
    assert outcomes == {"A03N": "arrived", "A02N": "unreached"}
    (trip,) = trips
    assert (trip["stops_arrived"], trip["stops_unreached"]) == (1, 1)
    assert TRIP not in str(t.trips)


def test_other_feeds_do_not_finish_trips():
    t = Tracker(gone_after=60)
    t.observe("ace", feed(T0, [(TRIP, [("A03N", T0 + 10, None)], None)]), T0)
    t.observe("bdfm", feed(T0 + 600, []), T0 + 600)
    assert t.take() == ([], [])


def test_state_survives_a_restart():
    t = Tracker()
    t.observe(
        "ace", feed(T0, [(TRIP, [("A03N", T0 + 30, None), ("A02N", T0 + 120, None)], None)]), T0
    )
    saved = t.dump()
    fresh = Tracker()
    fresh.load(saved)
    fresh.observe("ace", feed(T0 + 60, [(TRIP, [("A02N", T0 + 120, None)], None)]), T0 + 60)
    (e,) = fresh.take()[0]
    assert e["stop_id"] == "A03N" and e["outcome"] == "arrived"


def test_alerts_are_flattened():
    payload = {
        "entity": [
            {
                "id": "lmm:alert:1",
                "alert": {
                    "active_period": [{"start": T0, "end": T0 + 600}, {"start": T0 + 86400}],
                    "header_text": {
                        "translation": [
                            {"language": "en", "text": "[E] delays"},
                            {"language": "en-html", "text": "<p>x</p>"},
                        ]
                    },
                    "informed_entity": [{"route_id": "E"}, {"route_id": "A"}, {"stop_id": "A27"}],
                    "transit_realtime.mercury_alert": {
                        "alert_type": "Delays",
                        "created_at": T0,
                        "updated_at": T0 + 5,
                    },
                },
            }
        ]
    }
    (a,) = alert_rows(payload)
    assert a["header_text"] == "[E] delays"
    assert a["route_ids"] == ["A", "E"] and a["stop_ids"] == ["A27"]
    assert a["active_periods"][1]["ends_at"] is None
    assert a["alert_type"] == "Delays"
