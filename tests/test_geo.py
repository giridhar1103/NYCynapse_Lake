from nycynapse_lake import geo


def square(x0, y0, x1, y1):
    return f"ST_MakeEnvelope({x0}, {y0}, {x1}, {y1})"


def setup_layers(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS lake.t")
    # Two neighbourhoods side by side in the same borough, split at longitude -73.95.
    con.execute(f"""CREATE TABLE lake.t.geo_nta AS
        SELECT 'MN0101' AS nta_code, {square(-74.0, 40.70, -73.95, 40.80)} AS geom
        UNION ALL SELECT 'MN0102', {square(-73.95, 40.70, -73.90, 40.80)}""")
    con.execute(
        f"CREATE TABLE lake.t.geo_borough AS SELECT 1 AS boro_code, "
        f"{square(-74.0, 40.70, -73.90, 40.80)} AS geom"
    )
    for table, col, value in [
        ("geo_community_district", "boro_cd", 101),
        ("geo_modzcta", "modzcta", "'10001'"),
        ("geo_council_district", "council_district", 3),
        ("geo_police_precinct", "precinct", 14),
        ("geo_taxi_zone", "location_id", 161),
    ]:
        con.execute(
            f"CREATE TABLE lake.t.{table} AS SELECT {value} AS {col}, "
            f"{square(-74.0, 40.70, -73.90, 40.80)} AS geom"
        )
    con.execute(f"CREATE TABLE lake.t.geo_piece AS {geo.pieces_sql('lake.t')}")


def test_points_get_every_code(con):
    setup_layers(con)
    con.execute("""CREATE TEMP TABLE pts AS SELECT * FROM (VALUES
        (1, -73.97, 40.75), (2, -73.92, 40.75), (3, -73.92, 40.75),
        (4, -73.50, 40.75), (5, NULL, NULL)) t(id, lon, lat)""")
    rows = con.execute(
        f"SELECT id, nta_code, boro_code, community_district, police_precinct, taxi_zone_id "
        f"FROM ({geo.tag_points('SELECT * FROM pts', lon='lon', lat='lat', schema='lake.t')}) "
        f"ORDER BY id"
    ).fetchall()
    assert rows == [
        (1, "MN0101", 1, 101, 14, 161),
        (2, "MN0102", 1, 101, 14, 161),
        (3, "MN0102", 1, 101, 14, 161),
        (4, None, None, None, None, None),
        (5, None, None, None, None, None),
    ]


def test_cell_edges_do_not_drop_or_duplicate_points(con):
    setup_layers(con)
    # -73.95 is both a grid line and the border between the two neighbourhoods.
    con.execute("CREATE TEMP TABLE edge AS SELECT 1 AS id, -73.95 AS lon, 40.75 AS lat")
    rows = con.execute(
        f"SELECT id, nta_code FROM "
        f"({geo.tag_points('SELECT * FROM edge', lon='lon', lat='lat', schema='lake.t')})"
    ).fetchall()
    assert len(rows) == 1 and rows[0][1] in {"MN0101", "MN0102"}


def test_prefix_and_subset(con):
    setup_layers(con)
    con.execute("CREATE TEMP TABLE ride AS SELECT -73.97 AS a_lon, 40.75 AS a_lat, "
                "-73.92 AS b_lon, 40.75 AS b_lat")
    inner = geo.tag_points("SELECT * FROM ride", lon="a_lon", lat="a_lat", schema="lake.t",
                           prefix="start_", columns=("nta_code", "boro_code"))
    outer = geo.tag_points(inner, lon="b_lon", lat="b_lat", schema="lake.t", prefix="end_",
                           columns=("nta_code",))
    row = con.execute(f"SELECT start_nta_code, start_boro_code, end_nta_code FROM ({outer})")
    assert row.fetchall() == [("MN0101", 1, "MN0102")]
    names = [d[0] for d in con.execute(f"SELECT * FROM ({outer})").description]
    assert "start_taxi_zone_id" not in names and "nta_code" not in names
