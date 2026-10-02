"""NYC DOT traffic speeds: readings per road link, plus the links themselves."""

from datetime import date, datetime, timedelta

import pyarrow as pa

from .. import geo
from ..runs import RunContext
from ..socrata import Dataset, Socrata
from .socrata_feed import borough, load_pages

DS = Dataset("data.cityofnewyork.us", "i4gi-tjb9")
START = date(2024, 1, 1)
PAGE = 250_000
OVERLAP = timedelta(hours=3)  # readings can arrive late; re-read a window, duplicates are dropped
OBS_COLUMNS = "link_id, data_as_of, speed, travel_time, status"


def _next(d: date) -> date:
    return date(d.year + d.month // 12, d.month % 12 + 1, 1)


def obs_sql(files: list[str]) -> str:
    paths = ", ".join(f"'{f}'" for f in files)
    return f"""
        SELECT link_id,
               timezone('America/New_York', CAST(data_as_of AS TIMESTAMP)) AS observed_at,
               speed AS speed_mph, TRY_CAST(travel_time AS INTEGER) AS travel_time_seconds,
               status AS status_code
        FROM read_csv([{paths}], header = true, all_varchar = true)
    """


def links_sql(relation: str) -> str:
    # link_points is "lat,lon lat,lon ..." and is sometimes cut off mid-pair or carries a stray
    # point far outside the city. Keep complete pairs inside the city before building the line.
    base = f"""
        WITH raw AS (
            SELECT link_id, link_name, borough, owner, transcom_id, last_reading,
                   list_filter(string_split(trim(link_points), ' '),
                               p -> regexp_full_match(p, '-?[0-9.]+,-?[0-9.]+')
                                    AND TRY_CAST(split_part(p, ',', 1) AS DOUBLE)
                                        BETWEEN 40.4 AND 41.1
                                    AND TRY_CAST(split_part(p, ',', 2) AS DOUBLE)
                                        BETWEEN -74.4 AND -73.5) AS pairs
            FROM {relation}
        ),
        lines AS (
            SELECT *, CASE WHEN len(pairs) >= 2 THEN ST_MakeLine(list_transform(pairs, p ->
                       ST_Point(CAST(split_part(p, ',', 2) AS DOUBLE),
                                CAST(split_part(p, ',', 1) AS DOUBLE)))) END AS geom
            FROM raw
        )
        SELECT link_id, NULLIF(trim(link_name), '') AS link_name, {borough("borough")} AS borough,
               owner, transcom_id, geom,
               ST_Length_Spheroid(ST_FlipCoordinates(geom)) AS length_m,
               ST_Y(ST_LineInterpolatePoint(geom, 0.5)) AS mid_latitude,
               ST_X(ST_LineInterpolatePoint(geom, 0.5)) AS mid_longitude,
               timezone('America/New_York', CAST(last_reading AS TIMESTAMP)) AS last_reading_at
        FROM lines
    """
    return geo.tag_points(
        base,
        lon="mid_longitude",
        lat="mid_latitude",
        columns=("nta_code", "community_district", "taxi_zone_id"),
    )


def run(ctx: RunContext, *, force: bool = False) -> None:
    soda = Socrata(ctx.http, ctx.settings.socrata_app_token)
    tmp = ctx.settings.tmp_path / ctx.source

    if not ctx.checkpoint("backfill_done"):
        month = date.fromisoformat(ctx.checkpoint("backfill_next", START.isoformat()))
        current = date.today().replace(day=1)
        while month <= current:
            nxt = _next(month)
            where = (
                f"data_as_of >= '{month.isoformat()}T00:00:00' "
                f"AND data_as_of < '{nxt.isoformat()}T00:00:00'"
            )
            load_pages(
                ctx,
                "traffic_speed_obs",
                soda.pages(DS, tmp, columns=OBS_COLUMNS, where=where, page_size=PAGE),
                obs_sql,
            )
            ctx.save("backfill_next", nxt.isoformat())
            month = nxt
        ctx.save("backfill_done", True)

    latest = ctx.checkpoint("latest_reading")
    if latest is None:
        latest = soda.scalar(DS, "min(data_as_of)", f"data_as_of >= '{START.isoformat()}'")
    since = (datetime.fromisoformat(latest) - OVERLAP).strftime("%Y-%m-%dT%H:%M:%S")
    load_pages(
        ctx,
        "traffic_speed_obs",
        soda.pages(DS, tmp, columns=OBS_COLUMNS, where=f"data_as_of > '{since}'", page_size=PAGE),
        obs_sql,
    )
    newest = soda.scalar(DS, "max(data_as_of)")
    if newest:
        ctx.advance("latest_reading", newest)

    # Links change rarely. Refresh them once a day from the latest reading of each.
    today = date.today().isoformat()
    if ctx.checkpoint("links_refreshed") != today:
        recent = (date.today() - timedelta(days=7)).isoformat()
        rows = soda.query(
            DS,
            select="link_id, max(link_name) AS link_name, max(borough) AS borough, "
            "max(owner) AS owner, max(transcom_id) AS transcom_id, "
            "max(link_points) AS link_points, max(data_as_of) AS last_reading",
            where=f"data_as_of >= '{recent}'",
            group="link_id",
            limit=5000,
        )
        if rows:
            ctx.con.register("_traffic_links", pa.Table.from_pylist(rows))
            try:
                ctx.load("traffic_link", links_sql("_traffic_links"))
            finally:
                ctx.con.unregister("_traffic_links")
        ctx.advance("links_refreshed", today)
