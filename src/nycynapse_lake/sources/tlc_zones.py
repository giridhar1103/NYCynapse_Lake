from ..runs import RunContext
from .files import fetch_if_changed

URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def run(ctx: RunContext, *, force: bool = False) -> None:
    with fetch_if_changed(ctx, URL, force=force) as got:
        if got is None:
            return
        ctx.load(
            "tlc_zone",
            f"""
            SELECT
                "LocationID"                                   AS location_id,
                trim("Zone")                                   AS zone_name,
                CASE WHEN "Borough" IN ('EWR', 'Unknown', 'N/A') THEN NULL
                     ELSE trim("Borough") END                  AS borough,
                NULLIF(trim(service_zone), 'N/A')              AS service_zone,
                CASE "LocationID"
                    WHEN '1'   THEN 'newark_airport'
                    WHEN '264' THEN 'unknown'
                    WHEN '265' THEN 'outside_nyc'
                    ELSE 'nyc' END                             AS zone_kind
            FROM read_csv('{got.path}', header = true, all_varchar = true)
        """,
        )
