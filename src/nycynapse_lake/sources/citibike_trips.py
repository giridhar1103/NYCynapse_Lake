"""Citi Bike monthly trip files: a zip of CSVs per month, loaded whole into its partition."""

import shutil
import zipfile
from datetime import date

from .. import geo
from ..runs import RunContext
from .files import fetch_if_changed

URL = "https://s3.amazonaws.com/tripdata/{month}-citibike-tripdata.zip"
START = date(2024, 1, 1)
LOOKBACK_MONTHS = 3
TAGS = ("boro_code", "nta_code", "taxi_zone_id")


def _next(d: date) -> date:
    return date(d.year + d.month // 12, d.month % 12 + 1, 1)


def _ny(col: str) -> str:
    return f"timezone('America/New_York', TRY_CAST({col} AS TIMESTAMP))"


def trips_sql(csv_glob: str, month: date) -> str:
    base = f"""
        SELECT DATE '{month.isoformat()}' AS file_month, ride_id,
               rideable_type AS bike_type, member_casual AS rider_type,
               {_ny("started_at")} AS started_at, {_ny("ended_at")} AS ended_at,
               NULLIF(start_station_id, '') AS start_station_id,
               NULLIF(start_station_name, '') AS start_station_name,
               NULLIF(end_station_id, '') AS end_station_id,
               NULLIF(end_station_name, '') AS end_station_name,
               start_lat AS start_latitude, start_lng AS start_longitude,
               end_lat AS end_latitude, end_lng AS end_longitude
        FROM read_csv('{csv_glob}', header = true, all_varchar = true)
    """
    tagged = geo.tag_points(
        base, lon="start_longitude", lat="start_latitude", prefix="start_", columns=TAGS
    )
    return geo.tag_points(
        tagged, lon="end_longitude", lat="end_latitude", prefix="end_", columns=TAGS
    )


def run(ctx: RunContext, *, force: bool = False) -> None:
    loaded = set(ctx.checkpoint("loaded_months", []))
    latest = date.today().replace(day=1)
    months, m = [], START
    while m < latest:
        months.append(m)
        m = _next(m)
    recent = set(months[-LOOKBACK_MONTHS:])
    for month in [m for m in months if m.isoformat() not in loaded or m in recent]:
        url = URL.format(month=month.strftime("%Y%m"))
        try:
            ctx.http.client.head(url).raise_for_status()
        except Exception:  # noqa: BLE001 - not published yet
            continue
        with fetch_if_changed(ctx, url, force=force) as got:
            if got is not None:
                unpacked = got.path.with_suffix(".d")
                unpacked.mkdir()
                try:
                    with zipfile.ZipFile(got.path) as z:
                        for name in z.namelist():
                            if name.endswith(".csv") and "__MACOSX" not in name:
                                with (
                                    z.open(name) as src,
                                    open(unpacked / name.split("/")[-1], "wb") as dst,
                                ):
                                    shutil.copyfileobj(src, dst, 1 << 20)
                    ctx.load(
                        "bike_trips",
                        trips_sql(f"{unpacked}/*.csv", month),
                        partition_filter=f"file_month = DATE '{month.isoformat()}'",
                    )
                    ctx.detail.setdefault("loaded", []).append(month.isoformat()[:7])
                finally:
                    shutil.rmtree(unpacked, ignore_errors=True)
        loaded.add(month.isoformat())
        ctx.save("loaded_months", sorted(loaded))
