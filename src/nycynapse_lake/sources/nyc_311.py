"""NYC 311 service requests.

The first run backfills month by month from January 2024, saving progress after each month.
Loading by month keeps each month's rows together in the lake's files. After that, each run
reads the rows Open Data changed since the last run and merges them in.
"""

from datetime import date

from .. import geo
from ..runs import RunContext
from ..socrata import Dataset, Socrata
from .socrata_feed import borough, follow_changes, in_nyc, load_pages, local_time, text

DS = Dataset("data.cityofnewyork.us", "erm2-nwe9")
START = date(2024, 1, 1)
COLUMNS = (
    "unique_key, created_date, closed_date, due_date, resolution_action_updated_date, status, "
    "agency, agency_name, complaint_type, descriptor, descriptor_2, location_type, "
    "resolution_description, open_data_channel_type, borough, incident_zip, incident_address, "
    "street_name, cross_street_1, cross_street_2, intersection_street_1, intersection_street_2, "
    "address_type, city, landmark, facility_type, bbl, community_board, council_district, "
    "police_precinct, x_coordinate_state_plane, y_coordinate_state_plane, latitude, longitude, "
    "park_facility_name, vehicle_type, taxi_company_borough, taxi_pick_up_location, "
    "bridge_highway_name, bridge_highway_direction, road_ramp, bridge_highway_segment"
)


def transform(files: list[str]) -> str:
    paths = ", ".join(f"'{f}'" for f in files)
    lon, lat = in_nyc("longitude", "latitude")
    base = f"""
        SELECT
            unique_key,
            {local_time("created_date")}                         AS created_at,
            {local_time("closed_date")}                          AS closed_at,
            {local_time("due_date")}                             AS due_at,
            {local_time("resolution_action_updated_date")}       AS resolution_updated_at,
            {text("status")}                                AS status,
            {text("agency")}                                AS agency,
            {text("agency_name")}                           AS agency_name,
            {text("complaint_type")}                        AS complaint_type,
            {text("descriptor")}                            AS descriptor,
            {text("descriptor_2")}                          AS descriptor_2,
            {text("location_type")}                         AS location_type,
            {text("resolution_description")}                AS resolution_description,
            lower(NULLIF(trim(open_data_channel_type), ''))  AS channel,
            {borough("borough")}                             AS borough,
            {text("incident_zip")}                          AS incident_zip,
            {text("incident_address")}                      AS incident_address,
            {text("street_name")}                           AS street_name,
            {text("cross_street_1")}                        AS cross_street_1,
            {text("cross_street_2")}                        AS cross_street_2,
            {text("intersection_street_1")}                 AS intersection_street_1,
            {text("intersection_street_2")}                 AS intersection_street_2,
            {text("address_type")}                          AS address_type,
            {text("city")}                                  AS city,
            {text("landmark")}                              AS landmark,
            {text("facility_type")}                         AS facility_type,
            {text("bbl")}                                   AS bbl,
            NULLIF(NULLIF(trim(community_board), ''), '0 Unspecified') AS reported_community_board,
            TRY_CAST(council_district AS TINYINT)            AS reported_council_district,
            TRY_CAST(regexp_extract(police_precinct, '([0-9]+)', 1) AS SMALLINT)
                                                             AS reported_police_precinct,
            x_coordinate_state_plane                         AS x_state_plane,
            y_coordinate_state_plane                         AS y_state_plane,
            {lat}                                            AS latitude,
            {lon}                                            AS longitude,
            {text("park_facility_name")}                    AS park_facility_name,
            {text("vehicle_type")}                          AS vehicle_type,
            {text("taxi_company_borough")}                  AS taxi_company_borough,
            {text("taxi_pick_up_location")}                 AS taxi_pick_up_location,
            {text("bridge_highway_name")}                   AS bridge_highway_name,
            {text("bridge_highway_direction")}              AS bridge_highway_direction,
            {text("road_ramp")}                             AS road_ramp,
            {text("bridge_highway_segment")}                AS bridge_highway_segment,
            CAST(":updated_at" AS TIMESTAMPTZ)               AS source_updated_at
        FROM read_csv([{paths}], header = true, all_varchar = true)
    """
    return geo.tag_points(base, lon="longitude", lat="latitude")


def _month_after(d: date) -> date:
    return date(d.year + d.month // 12, d.month % 12 + 1, 1)


def run(ctx: RunContext, *, force: bool = False) -> None:
    soda = Socrata(ctx.http, ctx.settings.socrata_app_token)
    tmp = ctx.settings.tmp_path / ctx.source

    # Remember where the change feed starts before backfilling, so edits made while the
    # backfill runs are picked up afterwards.
    if ctx.checkpoint("changes_after") is None:
        mark = soda.scalar(DS, "max(:updated_at)")
        ctx.save("changes_after", [mark, ""])

    if not ctx.checkpoint("backfill_done"):
        month = date.fromisoformat(ctx.checkpoint("backfill_next", START.isoformat()))
        current = date.today().replace(day=1)
        while month <= current:
            nxt = _month_after(month)
            where = (
                f"created_date >= '{month.isoformat()}T00:00:00' "
                f"AND created_date < '{nxt.isoformat()}T00:00:00'"
            )
            pages = soda.pages(DS, tmp, columns=COLUMNS, where=where, keys=(":updated_at", ":id"))
            load_pages(ctx, "requests_311", pages, transform)
            ctx.save("backfill_next", nxt.isoformat())
            ctx.detail.setdefault("backfilled", []).append(month.isoformat()[:7])
            month = nxt
        ctx.save("backfill_done", True)

    follow_changes(
        ctx,
        soda,
        DS,
        table="requests_311",
        columns=COLUMNS,
        where=f"created_date >= '{START.isoformat()}T00:00:00'",
        transform=transform,
    )
