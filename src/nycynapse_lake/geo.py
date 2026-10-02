"""Tag points with the boundaries they fall in, at load time.

Doing this once during ingestion means queries filter on plain codes such as nta_code instead
of running spatial joins over millions of rows every time.

Lookups go against geo_piece: every boundary cut along a 0.01 degree grid. A point only has
to be tested against the few small pieces whose box contains it, which is about ten times
faster than testing against whole precincts or council districts.
"""

GRID = 0.01
LON0, LAT0 = -74.30, 40.47
COLS, ROWS = 65, 48

LAYERS = {
    # layer: (table, code expression)
    "borough": ("geo_borough", "CAST(boro_code AS VARCHAR)"),
    "nta": ("geo_nta", "nta_code"),
    "community_district": ("geo_community_district", "CAST(boro_cd AS VARCHAR)"),
    "modzcta": ("geo_modzcta", "modzcta"),
    "council_district": ("geo_council_district", "CAST(council_district AS VARCHAR)"),
    "police_precinct": ("geo_police_precinct", "CAST(precinct AS VARCHAR)"),
    "taxi_zone": ("geo_taxi_zone", "CAST(location_id AS VARCHAR)"),
}

# output column, layer, type
GEO_COLUMNS = (
    ("boro_code", "borough", "TINYINT"),
    ("nta_code", "nta", "VARCHAR"),
    ("community_district", "community_district", "SMALLINT"),
    ("modzcta", "modzcta", "VARCHAR"),
    ("council_district", "council_district", "TINYINT"),
    ("police_precinct", "police_precinct", "SMALLINT"),
    ("taxi_zone_id", "taxi_zone", "SMALLINT"),
)


def pieces_sql(schema: str = "lake.silver") -> str:
    union = " UNION ALL ".join(
        f"SELECT '{layer}' AS layer, {code} AS code, geom FROM {schema}.{table}"
        for layer, (table, code) in LAYERS.items()
    )
    return f"""
        WITH cells AS (
            SELECT x AS cell_x, y AS cell_y,
                   ST_MakeEnvelope({LON0} + x * {GRID}, {LAT0} + y * {GRID},
                                   {LON0} + (x + 1) * {GRID}, {LAT0} + (y + 1) * {GRID}) AS cell
            FROM range({COLS}) a(x), range({ROWS}) b(y)
        ),
        cut AS (
            SELECT g.layer, g.code, cells.cell_x, cells.cell_y,
                   ST_Intersection(g.geom, cells.cell) AS geom
            FROM ({union}) g
            JOIN cells ON ST_Intersects(g.geom, cells.cell)
        )
        SELECT * FROM cut WHERE NOT ST_IsEmpty(geom) AND ST_Dimension(geom) = 2
    """


def tag_points(
    source_sql: str,
    *,
    lon: str,
    lat: str,
    schema: str = "lake.silver",
    prefix: str = "",
    columns: tuple[str, ...] | None = None,
) -> str:
    """Wrap a query so each row gains the GEO_COLUMNS for its longitude and latitude.

    Points outside the grid, or with missing coordinates, get nulls. Each distinct coordinate
    is looked up once, which matters for sources like 311 where many rows share an address.
    `columns` limits the output to some of the GEO_COLUMNS and `prefix` is put in front of each
    name, which lets one row be tagged twice, for example at the start and end of a ride.
    """
    lon_hi, lat_hi = LON0 + COLS * GRID, LAT0 + ROWS * GRID
    wanted = [c for c in GEO_COLUMNS if columns is None or c[0] in columns]
    pivots = ",\n".join(
        f"TRY_CAST(max(code) FILTER (WHERE layer = '{layer}') AS {kind}) AS {prefix}{col}"
        for col, layer, kind in wanted
    )
    out = ", ".join(f"g.{prefix}{col}" for col, _, _ in wanted)
    return f"""
        WITH src AS (
            SELECT s.*, TRY_CAST({lon} AS DOUBLE) AS _lon, TRY_CAST({lat} AS DOUBLE) AS _lat
            FROM ({source_sql}) s
        ),
        pts AS (
            SELECT DISTINCT _lon, _lat, ST_Point(_lon, _lat) AS _pt
            FROM src
            WHERE _lon BETWEEN {LON0} AND {lon_hi} AND _lat BETWEEN {LAT0} AND {lat_hi}
        ),
        hits AS (
            SELECT pts._lon, pts._lat, p.layer, any_value(p.code) AS code
            FROM pts JOIN {schema}.geo_piece p ON ST_Intersects(p.geom, pts._pt)
            GROUP BY ALL
        ),
        g AS (
            SELECT _lon, _lat, {pivots}
            FROM hits GROUP BY _lon, _lat
        )
        SELECT src.* EXCLUDE (_lon, _lat), {out}
        FROM src LEFT JOIN g USING (_lon, _lat)
    """
