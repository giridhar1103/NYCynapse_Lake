from datetime import UTC, datetime

from nycynapse_lake.sources import weather, weather_alerts


def location(lat, temp):
    return {
        "latitude": lat,
        "longitude": -73.9,
        "hourly": {
            "time": ["2026-10-01T00:00", "2026-10-01T01:00"],
            "temperature_2m": [temp, temp + 1],
            "is_day": [0, 1],
            "precipitation": [0.0, 1.2],
        },
    }


def test_flattens_one_row_per_borough_and_hour():
    payload = [location(40.0 + i, 10.0 * i) for i in range(5)]
    t = weather.to_table(payload)
    assert t.num_rows == 10
    assert t["boro_code"].to_pylist()[:3] == [1, 1, 2]
    assert t["hour_start"].to_pylist()[1] == "2026-10-01T01:00:00Z"
    assert t["is_day"].to_pylist()[:2] == [False, True]
    assert t["snowfall_cm"].null_count == 10


def test_marks_hours_after_fetch_as_forecast():
    payload = [location(40.0, 1.0)] * 5
    t = weather.to_table(payload, fetched_at=datetime(2026, 10, 1, 0, 30, tzinfo=UTC))
    assert t["is_forecast"].to_pylist()[:2] == [False, True]


def alert(vtec, zones, sent="2026-09-28T05:01:00-04:00", status="Actual", event="Heat Advisory"):
    return {
        "properties": {
            "status": status,
            "event": event,
            "sent": sent,
            "severity": "Moderate",
            "affectedZones": [f"https://api.weather.gov/zones/forecast/{z}" for z in zones],
            "parameters": {"VTEC": [vtec]},
            "headline": "h",
        }
    }


def test_live_alert_becomes_one_row_per_nyc_zone():
    payload = {
        "features": [
            alert(
                "/O.NEW.KOKX.HT.Y.0004.260715T1600Z-260716T0000Z/", ["NYZ072", "NYZ176", "NJZ006"]
            )
        ]
    }
    rows = weather_alerts.live_rows(payload)
    assert [(r["zone"], r["boro_code"]) for r in rows] == [("NYZ072", 1), ("NYZ176", 4)]
    r = rows[0]
    key = (r["office"], r["phenomena"], r["significance"], r["event_number"])
    assert key == ("OKX", "HT", "Y", 4)
    assert r["event_year"] == 2026
    assert r["starts_at"] == datetime(2026, 7, 15, 16, tzinfo=UTC)
    assert r["ends_at"] == datetime(2026, 7, 16, 0, tzinfo=UTC)


def test_cancelled_alert_ends_when_it_was_cancelled():
    payload = {"features": [alert("/O.CAN.KOKX.CF.S.0030.000000T0000Z-260928T1600Z/", ["NYZ075"])]}
    (r,) = weather_alerts.live_rows(payload)
    assert r["last_action"] == "CAN"
    assert r["ends_at"] == datetime(2026, 9, 28, 9, 1, tzinfo=UTC)


def test_test_messages_are_ignored():
    payload = {
        "features": [
            alert("/T.NEW.KOKX.HT.Y.0001.260715T1600Z-260716T0000Z/", ["NYZ072"], status="Test")
        ]
    }
    assert weather_alerts.live_rows(payload) == []
