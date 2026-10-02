"""NYPD motor vehicle collisions: crashes and the people involved."""

from .. import geo
from ..runs import RunContext
from ..socrata import Dataset, Socrata
from .socrata_feed import borough, follow_changes, in_nyc, text

CRASHES = Dataset("data.cityofnewyork.us", "h9gi-nx95")
PERSONS = Dataset("data.cityofnewyork.us", "f55k-p6yu")
SINCE = "crash_date >= '2024-01-01T00:00:00'"


def _crashed_at() -> str:
    # crash_date is a midnight timestamp and crash_time is H:MM, both New York time.
    return (
        "timezone('America/New_York', CAST(TRY_CAST(crash_date AS TIMESTAMP) AS DATE) "
        "+ TRY_CAST(lpad(crash_time, 5, '0') AS TIME))"
    )


def _count(col: str) -> str:
    return f"COALESCE(TRY_CAST({col} AS SMALLINT), 0)"


def crashes_sql(files: list[str]) -> str:
    paths = ", ".join(f"'{f}'" for f in files)
    lon, lat = in_nyc("longitude", "latitude")
    base = f"""
        SELECT
            collision_id,
            {_crashed_at()}                           AS crashed_at,
            {borough("borough")}                      AS borough,
            {text("zip_code")}                        AS zip_code,
            {lat}                                     AS latitude,
            {lon}                                     AS longitude,
            {text("on_street_name")}                  AS on_street_name,
            {text("cross_street_name")}               AS cross_street_name,
            {text("off_street_name")}                 AS off_street_name,
            {_count("number_of_persons_injured")}     AS persons_injured,
            {_count("number_of_persons_killed")}      AS persons_killed,
            {_count("number_of_pedestrians_injured")} AS pedestrians_injured,
            {_count("number_of_pedestrians_killed")}  AS pedestrians_killed,
            {_count("number_of_cyclist_injured")}     AS cyclists_injured,
            {_count("number_of_cyclist_killed")}      AS cyclists_killed,
            {_count("number_of_motorist_injured")}    AS motorists_injured,
            {_count("number_of_motorist_killed")}     AS motorists_killed,
            {text("contributing_factor_vehicle_1")}   AS contributing_factor_1,
            {text("contributing_factor_vehicle_2")}   AS contributing_factor_2,
            {text("contributing_factor_vehicle_3")}   AS contributing_factor_3,
            {text("contributing_factor_vehicle_4")}   AS contributing_factor_4,
            {text("contributing_factor_vehicle_5")}   AS contributing_factor_5,
            {text("vehicle_type_code1")}              AS vehicle_type_1,
            {text("vehicle_type_code2")}              AS vehicle_type_2,
            {text("vehicle_type_code_3")}             AS vehicle_type_3,
            {text("vehicle_type_code_4")}             AS vehicle_type_4,
            {text("vehicle_type_code_5")}             AS vehicle_type_5,
            CAST(":updated_at" AS TIMESTAMPTZ)        AS source_updated_at
        FROM read_csv([{paths}], header = true, all_varchar = true)
    """
    return geo.tag_points(base, lon="longitude", lat="latitude")


def persons_sql(files: list[str]) -> str:
    paths = ", ".join(f"'{f}'" for f in files)
    return f"""
        SELECT
            unique_id                                 AS person_record_id,
            collision_id,
            {_crashed_at()}                           AS crashed_at,
            {text("person_id")}                       AS person_id,
            {text("vehicle_id")}                      AS vehicle_id,
            {text("person_type")}                     AS person_type,
            {text("person_injury")}                   AS person_injury,
            CASE WHEN TRY_CAST(person_age AS SMALLINT) BETWEEN 0 AND 110
                 THEN TRY_CAST(person_age AS SMALLINT) END AS person_age,
            {text("person_sex")}                      AS person_sex,
            {text("ped_role")}                        AS ped_role,
            {text("position_in_vehicle")}             AS position_in_vehicle,
            {text("safety_equipment")}                AS safety_equipment,
            {text("ejection")}                        AS ejection,
            {text("emotional_status")}                AS emotional_status,
            {text("bodily_injury")}                   AS bodily_injury,
            {text("complaint")}                       AS complaint,
            {text("ped_location")}                    AS ped_location,
            {text("ped_action")}                      AS ped_action,
            {text("contributing_factor_1")}           AS contributing_factor_1,
            {text("contributing_factor_2")}           AS contributing_factor_2,
            CAST(":updated_at" AS TIMESTAMPTZ)        AS source_updated_at
        FROM read_csv([{paths}], header = true, all_varchar = true)
    """


CRASH_COLUMNS = (
    "collision_id, crash_date, crash_time, borough, zip_code, latitude, longitude, "
    "on_street_name, cross_street_name, off_street_name, number_of_persons_injured, "
    "number_of_persons_killed, number_of_pedestrians_injured, number_of_pedestrians_killed, "
    "number_of_cyclist_injured, number_of_cyclist_killed, number_of_motorist_injured, "
    "number_of_motorist_killed, contributing_factor_vehicle_1, contributing_factor_vehicle_2, "
    "contributing_factor_vehicle_3, contributing_factor_vehicle_4, contributing_factor_vehicle_5, "
    "vehicle_type_code1, vehicle_type_code2, vehicle_type_code_3, vehicle_type_code_4, "
    "vehicle_type_code_5"
)
PERSON_COLUMNS = (
    "unique_id, collision_id, crash_date, crash_time, person_id, vehicle_id, person_type, "
    "person_injury, person_age, person_sex, ped_role, position_in_vehicle, safety_equipment, "
    "ejection, emotional_status, bodily_injury, complaint, ped_location, ped_action, "
    "contributing_factor_1, contributing_factor_2"
)


def run(ctx: RunContext, *, force: bool = False) -> None:
    soda = Socrata(ctx.http, ctx.settings.socrata_app_token)
    follow_changes(
        ctx,
        soda,
        CRASHES,
        table="collision_crashes",
        columns=CRASH_COLUMNS,
        where=SINCE,
        transform=crashes_sql,
        key="crashes_after",
    )
    follow_changes(
        ctx,
        soda,
        PERSONS,
        table="collision_persons",
        columns=PERSON_COLUMNS,
        where=SINCE,
        transform=persons_sql,
        key="persons_after",
    )
