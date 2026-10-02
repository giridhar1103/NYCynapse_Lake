"""MTA hourly subway ridership and the station list."""

from datetime import date, timedelta

from .. import geo
from ..runs import RunContext
from ..socrata import Dataset, Socrata
from .socrata_feed import borough, text

DATASETS = {2024: Dataset("data.ny.gov", "wujg-7c2s")}
CURRENT = Dataset("data.ny.gov", "5wq4-mkjj")
STATIONS = Dataset("data.ny.gov", "39hk-dx4f")
START = date(2024, 1, 1)
PAGE = 200_000
COLUMNS = (
    "transit_timestamp, transit_mode, station_complex_id, station_complex, borough, "
    "payment_method, fare_class_category, ridership, transfers, latitude, longitude"
)
BORO_CODES = {
    "M": "Manhattan",
    "Bx": "Bronx",
    "Bk": "Brooklyn",
    "Q": "Queens",
    "SI": "Staten Island",
}


def _next(d: date) -> date:
    return date(d.year + d.month // 12, d.month % 12 + 1, 1)


def _local(d: date) -> str:
    return f"timezone('America/New_York', TIMESTAMP '{d.isoformat()} 00:00:00')"


def ridership_sql(files: list[str]) -> str:
    paths = ", ".join(f"'{f}'" for f in files)
    return f"""
        SELECT timezone('America/New_York', CAST(transit_timestamp AS TIMESTAMP)) AS hour_start,
               transit_mode, station_complex_id, {text("station_complex")} AS station_complex,
               {borough("borough")} AS borough, payment_method,
               fare_class_category AS fare_class, ridership AS riders, transfers,
               latitude, longitude
        FROM read_csv([{paths}], header = true, all_varchar = true)
    """


def stations_sql(path: str) -> str:
    boros = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in BORO_CODES.items())
    base = f"""
        SELECT gtfs_stop_id, station_id, complex_id, stop_name, division, line,
               CASE borough {boros} END AS borough,
               cbd = 'true' AS in_congestion_zone,
               string_split(trim(daytime_routes), ' ') AS daytime_routes,
               structure, gtfs_latitude AS latitude, gtfs_longitude AS longitude,
               NULLIF(north_direction_label, '') AS north_direction_label,
               NULLIF(south_direction_label, '') AS south_direction_label,
               CASE ada WHEN '0' THEN 'none' WHEN '1' THEN 'full' WHEN '2' THEN 'partial' END
                   AS accessibility,
               ada_northbound = '1' AS accessible_northbound,
               ada_southbound = '1' AS accessible_southbound,
               NULLIF(NULLIF(ada_notes, 'NaN'), '') AS accessibility_notes
        FROM read_csv('{path}', header = true, all_varchar = true)
    """
    return geo.tag_points(base, lon="longitude", lat="latitude")


def _load_month(ctx: RunContext, soda: Socrata, month: date) -> int:
    """Fetch a month one day at a time, then replace the month's partition in one transaction.

    data.ny.gov answers a one-day window in seconds but can stall for many minutes on a
    month-wide one, so days are the unit of download and the month is the unit of commit.
    """
    ds = DATASETS.get(month.year, CURRENT)
    nxt = _next(month)
    pages = []
    try:
        day = month
        while day < nxt:
            following = day + timedelta(days=1)
            where = (
                f"transit_timestamp >= '{day.isoformat()}T00:00:00' "
                f"AND transit_timestamp < '{following.isoformat()}T00:00:00'"
            )
            pages += soda.pages(
                ds,
                ctx.settings.tmp_path / ctx.source,
                columns=COLUMNS,
                where=where,
                page_size=PAGE,
            )
            day = following
        if not pages:
            return 0
        return ctx.load(
            "subway_ridership_hourly",
            ridership_sql([str(p.file.path) for p in pages]),
            partition_filter=f"hour_start >= {_local(month)} AND hour_start < {_local(nxt)}",
        )
    finally:
        for p in pages:
            p.file.path.unlink(missing_ok=True)


def run(ctx: RunContext, *, force: bool = False) -> None:
    soda = Socrata(ctx.http, ctx.settings.socrata_app_token)

    pages = list(
        soda.pages(
            STATIONS,
            ctx.settings.tmp_path / ctx.source,
            columns=(
                "gtfs_stop_id, station_id, complex_id, stop_name, division, line, borough, cbd, "
                "daytime_routes, structure, gtfs_latitude, gtfs_longitude, north_direction_label, "
                "south_direction_label, ada, ada_northbound, ada_southbound, ada_notes"
            ),
            where="1 = 1",
        )
    )
    try:
        ctx.load("subway_station", stations_sql(str(pages[0].file.path)))
    finally:
        for p in pages:
            p.file.path.unlink(missing_ok=True)

    current = date.today().replace(day=1)
    month = date.fromisoformat(ctx.checkpoint("backfill_next", START.isoformat()))
    while month <= current:
        _load_month(ctx, soda, month)
        ctx.save("backfill_next", _next(month).isoformat())
        ctx.detail.setdefault("months", []).append(month.isoformat()[:7])
        month = _next(month)

    # Recent weeks get revised, so reload the last two months every time.
    previous = date(current.year - (current.month == 1), (current.month - 2) % 12 + 1, 1)
    for m in (previous, current):
        if m.isoformat()[:7] not in ctx.detail.get("months", []):
            _load_month(ctx, soda, m)
            ctx.detail.setdefault("months", []).append(m.isoformat()[:7])
