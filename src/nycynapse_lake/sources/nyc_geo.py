"""NYC boundary files from NYC Open Data, one GeoJSON export per table."""

from .. import geo
from ..runs import RunContext
from .files import fetch_if_changed

BASE = "https://data.cityofnewyork.us/resource/{}.geojson?$limit=50000"

DATASETS = {
    "geo_borough": (
        "gthc-hcne",
        """
        SELECT borocode AS boro_code, boroname AS boro_name,
               shape_area AS area_sq_ft, geom
        FROM {src}
    """,
    ),
    "geo_nta": (
        "9nt8-h7nd",
        """
        SELECT nta2020 AS nta_code, ntaname AS nta_name, ntaabbrev AS nta_abbrev,
               CASE ntatype WHEN '0' THEN 'residential' WHEN '5' THEN 'jail'
                    WHEN '6' THEN 'special_use' WHEN '7' THEN 'cemetery'
                    WHEN '8' THEN 'airport' WHEN '9' THEN 'park' END AS nta_kind,
               cdta2020 AS cdta_code, borocode AS boro_code, boroname AS boro_name,
               countyfips AS county_fips, shape_area AS area_sq_ft, geom
        FROM {src}
    """,
    ),
    "geo_cdta": (
        "xn3r-zk6y",
        """
        SELECT cdta2020 AS cdta_code, cdtaname AS cdta_name,
               CASE cdtatype WHEN '0' THEN 'community_district'
                    WHEN '1' THEN 'joint_interest_area' END AS cdta_kind,
               borocode AS boro_code, boroname AS boro_name, countyfips AS county_fips,
               shape_area AS area_sq_ft, geom
        FROM {src}
    """,
    ),
    "geo_community_district": (
        "5crt-au7u",
        """
        SELECT boro_cd, CAST(boro_cd AS INTEGER) // 100 AS boro_code,
               CAST(boro_cd AS INTEGER) % 100 AS district_number,
               CAST(boro_cd AS INTEGER) % 100 > 18 AS is_joint_interest_area,
               shape_area AS area_sq_ft, geom
        FROM {src}
    """,
    ),
    "geo_modzcta": (
        "pri4-ifjk",
        """
        SELECT modzcta, label, zcta AS zctas, pop_est AS population_estimate, geom
        FROM {src}
        WHERE modzcta <> '99999'
    """,
    ),
    "geo_council_district": (
        "872g-cjhh",
        """
        SELECT coundist AS council_district, shape_area AS area_sq_ft, geom FROM {src}
    """,
    ),
    "geo_police_precinct": (
        "y76i-bdw7",
        """
        SELECT precinct, shape_area AS area_sq_ft, geom FROM {src}
    """,
    ),
    # Zones 56 and 103 come as several features. Merge them into one shape per id.
    "geo_taxi_zone": (
        "8meu-9t5y",
        """
        SELECT locationid AS location_id, any_value(zone) AS zone_name,
               any_value(borough) AS borough, ST_Union_Agg(geom) AS geom
        FROM {src}
        GROUP BY locationid
    """,
    ),
}


def run(ctx: RunContext, *, force: bool = False) -> None:
    changed = False
    for table, (dataset, sql) in DATASETS.items():
        with fetch_if_changed(ctx, BASE.format(dataset), force=force) as got:
            if got is None:
                continue
            ctx.load(table, sql.format(src=f"ST_Read('{got.path}')"))
            changed = True
    if changed or not _has_pieces(ctx):
        ctx.load("geo_piece", geo.pieces_sql())


def _has_pieces(ctx: RunContext) -> bool:
    return bool(
        ctx.con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' "
            "AND schema_name = 'silver' AND table_name = 'geo_piece'"
        ).fetchone()[0]
    )
