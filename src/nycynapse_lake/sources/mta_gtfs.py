"""Static subway schedule (GTFS). A new version is loaded whenever the MTA publishes one."""

import zipfile

from .. import geo
from ..runs import RunContext
from .files import fetch_if_changed

URL = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip"


def _seconds(col: str) -> str:
    # HH:MM:SS where HH can pass 24.
    return (
        f"TRY_CAST(split_part({col}, ':', 1) AS INTEGER) * 3600 "
        f"+ TRY_CAST(split_part({col}, ':', 2) AS INTEGER) * 60 "
        f"+ TRY_CAST(split_part({col}, ':', 3) AS INTEGER)"
    )


def _date(col: str) -> str:
    return f"strptime({col}, '%Y%m%d')::DATE"


def statements(d: str, version: str) -> dict[str, str]:
    def src(name: str) -> str:
        return f"read_csv('{d}/{name}', header = true, all_varchar = true)"

    v = f"'{version}' AS feed_version"
    days = ", ".join(
        f"{day} = '1' AS {day}"
        for day in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
    )
    stops = f"""
        SELECT {v}, stop_id, stop_name, stop_lat AS latitude, stop_lon AS longitude,
               COALESCE(location_type, '') = '1' AS is_station,
               NULLIF(parent_station, '') AS parent_station,
               CASE WHEN stop_id LIKE '%N' THEN 'N' WHEN stop_id LIKE '%S' THEN 'S' END AS direction
        FROM {src("stops.txt")}
    """
    return {
        "gtfs_route": f"""
            SELECT {v}, route_id, route_short_name, route_long_name, route_desc, route_type,
                   route_url, route_color, route_text_color, route_sort_order
            FROM {src("routes.txt")}""",
        "gtfs_stop": geo.tag_points(stops, lon="longitude", lat="latitude"),
        "gtfs_trip": f"""
            SELECT {v}, trip_id, route_id, service_id, trip_headsign, direction_id, shape_id,
                   NULLIF(regexp_extract(trip_id, '_([0-9]{{6}}_.+)$', 1), '') AS realtime_trip_id
            FROM {src("trips.txt")}""",
        "gtfs_stop_time": f"""
            SELECT {v}, trip_id, stop_sequence, stop_id, arrival_time, departure_time,
                   {_seconds("arrival_time")} AS arrival_seconds,
                   {_seconds("departure_time")} AS departure_seconds
            FROM {src("stop_times.txt")}""",
        "gtfs_calendar": f"""
            SELECT {v}, service_id, {days}, {_date("start_date")} AS start_date,
                   {_date("end_date")} AS end_date
            FROM {src("calendar.txt")}""",
        "gtfs_calendar_date": f"""
            SELECT {v}, service_id, {_date("date")} AS service_date, exception_type
            FROM {src("calendar_dates.txt")}""",
        "gtfs_shape": f"""
            SELECT {v}, shape_id, shape_pt_sequence AS point_sequence,
                   shape_pt_lat AS latitude, shape_pt_lon AS longitude
            FROM {src("shapes.txt")}""",
        "gtfs_transfer": f"""
            SELECT {v}, from_stop_id, to_stop_id, transfer_type,
                   min_transfer_time AS min_transfer_seconds
            FROM {src("transfers.txt")}""",
    }


def run(ctx: RunContext, *, force: bool = False) -> None:
    with fetch_if_changed(ctx, URL, force=force) as got:
        if got is None:
            return
        version = got.sha256[:12]
        unpacked = got.path.with_suffix(".d")
        try:
            with zipfile.ZipFile(got.path) as z:
                z.extractall(unpacked)
            # Child tables first, the version row last: a version is listed only when complete.
            for table, sql in statements(str(unpacked), version).items():
                ctx.load(table, sql)
            ctx.load(
                "gtfs_feed_version",
                f"""
                SELECT '{version}' AS feed_version, feed_version AS publisher_version,
                       {_date("feed_start_date")} AS feed_start_date,
                       {_date("feed_end_date")} AS feed_end_date,
                       '{got.sha256}' AS file_sha256, now() AS loaded_at
                FROM read_csv('{unpacked}/feed_info.txt', header = true, all_varchar = true)
            """,
            )
            ctx.detail["feed_version"] = version
        finally:
            for f in unpacked.glob("*"):
                f.unlink()
            if unpacked.exists():
                unpacked.rmdir()
