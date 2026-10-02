from pathlib import Path

import httpx

from nycynapse_lake.config import REPO_CONTRACTS
from nycynapse_lake.contracts import load
from nycynapse_lake.http import Http
from nycynapse_lake.runs import run_source
from nycynapse_lake.sources import tlc_zones

FIXTURE = (Path(__file__).parent / "fixtures" / "taxi_zone_lookup.csv").read_bytes()
CONTRACT = load(REPO_CONTRACTS / "tlc_zones.yaml")


def server(calls):
    def handler(req):
        calls.append(dict(req.headers))
        if req.headers.get("if-none-match") == '"v1"':
            return httpx.Response(304)
        return httpx.Response(200, content=FIXTURE, headers={"ETag": '"v1"'})

    return Http(transport=httpx.MockTransport(handler))


def test_loads_zones_then_skips_unchanged_file(settings, ctl, con):
    calls = []
    http = server(calls)
    with run_source(CONTRACT, settings, ctl=ctl, con=con, http=http) as ctx:
        tlc_zones.run(ctx)

    rows = con.execute(
        "SELECT location_id, borough, zone_kind FROM lake.silver.tlc_zone "
        "WHERE location_id IN (1, 4, 264, 265) ORDER BY 1"
    ).fetchall()
    assert rows == [(1, None, "newark_airport"), (4, "Manhattan", "nyc"),
                    (264, None, "unknown"), (265, None, "outside_nyc")]
    assert con.execute("SELECT count(*) FROM lake.silver.tlc_zone").fetchone()[0] == 265

    with run_source(CONTRACT, settings, ctl=ctl, con=con, http=http) as ctx:
        tlc_zones.run(ctx)
        assert ctx.detail["unchanged"] == [tlc_zones.URL]
    assert calls[-1]["if-none-match"] == '"v1"'
    assert not any((settings.tmp_path / "tlc_zones").iterdir())
