from nycynapse_lake.realtime.gbfs import VALID_SINCE, station_rows, status_rows

NOW = 1_790_976_251


def payload(*stations):
    return {"data": {"stations": list(stations)}}


def station(sid="s1", bikes=3, docks=10, reported=NOW - 30, installed=1):
    return {
        "station_id": sid,
        "num_bikes_available": bikes,
        "num_ebikes_available": 1,
        "num_bikes_disabled": 0,
        "num_docks_available": docks,
        "num_docks_disabled": 0,
        "is_installed": installed,
        "is_renting": 1,
        "is_returning": 1,
        "last_reported": reported,
    }


def test_only_changes_are_kept():
    last = {}
    first = status_rows(payload(station(), station("s2")), last, NOW)
    assert [r["station_id"] for r in first] == ["s1", "s2"]
    same = status_rows(payload(station(), station("s2")), last, NOW + 60)
    assert same == []
    moved = status_rows(payload(station(bikes=2, docks=11), station("s2")), last, NOW + 120)
    assert [(r["station_id"], r["bikes_available"]) for r in moved] == [("s1", 2)]


def test_placeholder_report_time_falls_back_to_poll_time():
    (row,) = status_rows(payload(station(reported=86400, installed=0)), {}, NOW)
    assert int(row["reported_at"].timestamp()) == NOW
    assert row["is_installed"] is False
    (row,) = status_rows(payload(station(reported=VALID_SINCE + 5)), {}, NOW)
    assert int(row["reported_at"].timestamp()) == VALID_SINCE + 5


def test_station_rows_keep_first_seen_and_region_names():
    info = {
        "data": {
            "stations": [
                {
                    "station_id": "s1",
                    "name": "A & B",
                    "short_name": "1.01",
                    "lat": 40.7,
                    "lon": -73.9,
                    "region_id": "71",
                    "capacity": 20,
                }
            ]
        }
    }
    regions = {"data": {"regions": [{"region_id": "71", "name": "NYC District"}]}}
    first_seen = {}
    (a,) = station_rows(info, regions, first_seen, NOW)
    (b,) = station_rows(info, regions, first_seen, NOW + 3600)
    assert a["region"] == "NYC District"
    assert b["first_seen_at"] == a["first_seen_at"]
    assert int(b["last_seen_at"].timestamp()) == NOW + 3600
