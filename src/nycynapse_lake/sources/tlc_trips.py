"""TLC monthly trip files. One file per service and month, loaded whole into its partition."""

from datetime import date

from ..runs import RunContext
from .files import fetch_if_changed

BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data/{kind}_tripdata_{month}.parquet"
START = date(2024, 1, 1)
KINDS = {"fhvhv": "tlc_fhvhv_trips", "yellow": "tlc_yellow_trips", "green": "tlc_green_trips"}
LOOKBACK_MONTHS = 4  # TLC sometimes republishes recent files; recheck these every run


def _next(d: date) -> date:
    return date(d.year + d.month // 12, d.month % 12 + 1, 1)


def _ny(col: str) -> str:
    return f"timezone('America/New_York', {col})"


def _opt(present: set[str], col: str, alias: str) -> str:
    return f"{col} AS {alias}" if col.lower() in present else f"NULL AS {alias}"


def _flag(col: str) -> str:
    return f"CASE upper({col}) WHEN 'Y' THEN true WHEN 'N' THEN false END"


def fhvhv_sql(path: str, month: date, present: set[str]) -> str:
    return f"""
        SELECT DATE '{month.isoformat()}' AS file_month,
               CASE hvfhs_license_num WHEN 'HV0003' THEN 'Uber' WHEN 'HV0005' THEN 'Lyft'
                    WHEN 'HV0004' THEN 'Via' WHEN 'HV0002' THEN 'Juno' END AS company,
               hvfhs_license_num AS license_number,
               dispatching_base_num AS dispatching_base,
               NULLIF(originating_base_num, '') AS originating_base,
               {_ny("request_datetime")} AS requested_at,
               {_ny("on_scene_datetime")} AS on_scene_at,
               {_ny("pickup_datetime")} AS pickup_at,
               {_ny("dropoff_datetime")} AS dropoff_at,
               PULocationID AS pickup_zone_id, DOLocationID AS dropoff_zone_id,
               trip_miles, trip_time AS trip_seconds,
               base_passenger_fare AS base_fare_usd, tolls AS tolls_usd,
               bcf AS black_car_fund_usd, sales_tax AS sales_tax_usd,
               congestion_surcharge AS congestion_surcharge_usd,
               {_opt(present, "airport_fee", "airport_fee_usd")},
               {_opt(present, "cbd_congestion_fee", "cbd_congestion_fee_usd")},
               tips AS tips_usd, driver_pay AS driver_pay_usd,
               {_flag("shared_request_flag")} AS shared_requested,
               {_flag("shared_match_flag")} AS shared_matched,
               {_flag("access_a_ride_flag")} AS access_a_ride,
               {_flag("wav_request_flag")} AS wav_requested,
               {_flag("wav_match_flag")} AS wav_matched
        FROM read_parquet('{path}')
    """


def taxi_sql(path: str, month: date, present: set[str], *, green: bool) -> str:
    p = "lpep" if green else "tpep"
    extra = (
        f"trip_type, {_opt(present, 'ehail_fee', 'ehail_fee_usd')},"
        if green
        else f"{_opt(present, 'airport_fee', 'airport_fee_usd')},"
    )
    return f"""
        SELECT DATE '{month.isoformat()}' AS file_month,
               VendorID AS vendor_id,
               {_ny(p + "_pickup_datetime")} AS pickup_at,
               {_ny(p + "_dropoff_datetime")} AS dropoff_at,
               passenger_count, trip_distance AS trip_miles, RatecodeID AS rate_code,
               {_flag("store_and_fwd_flag")} AS stored_and_forwarded,
               PULocationID AS pickup_zone_id, DOLocationID AS dropoff_zone_id,
               payment_type, {extra}
               fare_amount AS fare_usd, extra AS extra_usd, mta_tax AS mta_tax_usd,
               tip_amount AS tip_usd, tolls_amount AS tolls_usd,
               improvement_surcharge AS improvement_surcharge_usd,
               congestion_surcharge AS congestion_surcharge_usd,
               {_opt(present, "cbd_congestion_fee", "cbd_congestion_fee_usd")},
               total_amount AS total_usd
        FROM read_parquet('{path}')
    """


def _months(ctx: RunContext) -> list[date]:
    """Months to look at: anything not loaded yet, plus the last few for republished files."""
    latest = date.today().replace(day=1)
    months, m = [], START
    while m < latest:
        months.append(m)
        m = _next(m)
    loaded = set(ctx.checkpoint("loaded_months", []))
    recent = months[-LOOKBACK_MONTHS:]
    return [m for m in months if m.isoformat() not in loaded or m in recent]


def run(ctx: RunContext, *, force: bool = False, kinds: tuple[str, ...] = tuple(KINDS)) -> None:
    loaded = set(ctx.checkpoint("loaded_months", []))
    for month in _months(ctx):
        complete = True
        for kind in kinds:
            url = BASE.format(kind=kind, month=month.strftime("%Y-%m"))
            try:
                ctx.http.client.head(url).raise_for_status()
            except Exception:  # noqa: BLE001 - not published yet
                complete = False
                continue
            with fetch_if_changed(ctx, url, force=force) as got:
                if got is None:
                    continue
                present = {
                    r[0].lower()
                    for r in ctx.con.execute(
                        f"DESCRIBE SELECT * FROM read_parquet('{got.path}')"
                    ).fetchall()
                }
                sql = (
                    fhvhv_sql(str(got.path), month, present)
                    if kind == "fhvhv"
                    else taxi_sql(str(got.path), month, present, green=kind == "green")
                )
                ctx.load(
                    KINDS[kind], sql, partition_filter=f"file_month = DATE '{month.isoformat()}'"
                )
                ctx.detail.setdefault("loaded", []).append(f"{kind} {month:%Y-%m}")
        if complete:
            loaded.add(month.isoformat())
            ctx.save("loaded_months", sorted(loaded))
