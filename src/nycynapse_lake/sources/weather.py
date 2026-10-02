"""Hourly weather per borough from Open-Meteo."""

from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pyarrow.compute as pc

from ..runs import RunContext

ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
FORECAST = "https://api.open-meteo.com/v1/forecast"
START = date(2024, 1, 1)
ARCHIVE_LAG_DAYS = 6
CHUNK_DAYS = 92

# One reading point per borough, near its middle and away from the water.
POINTS = {
    1: (40.7812, -73.9665),  # Manhattan, Central Park
    2: (40.8448, -73.8648),  # Bronx
    3: (40.6501, -73.9496),  # Brooklyn
    4: (40.7282, -73.7949),  # Queens
    5: (40.5834, -74.1496),  # Staten Island
}

VARIABLES = {
    "temperature_2m": "temperature_c",
    "apparent_temperature": "apparent_temperature_c",
    "dew_point_2m": "dew_point_c",
    "relative_humidity_2m": "relative_humidity_pct",
    "precipitation": "precipitation_mm",
    "rain": "rain_mm",
    "snowfall": "snowfall_cm",
    "snow_depth": "snow_depth_m",
    "weather_code": "weather_code",
    "cloud_cover": "cloud_cover_pct",
    "pressure_msl": "pressure_msl_hpa",
    "wind_speed_10m": "wind_speed_kmh",
    "wind_gusts_10m": "wind_gusts_kmh",
    "wind_direction_10m": "wind_direction_deg",
    "is_day": "is_day",
}


def _params(**extra) -> dict:
    boros = list(POINTS)
    return {
        "latitude": ",".join(str(POINTS[b][0]) for b in boros),
        "longitude": ",".join(str(POINTS[b][1]) for b in boros),
        "hourly": ",".join(VARIABLES),
        "timezone": "UTC",
        **extra,
    }


def to_table(payload, *, fetched_at: datetime | None = None) -> pa.Table:
    """Flatten Open-Meteo's per-location response into one row per borough and hour."""
    locations = payload if isinstance(payload, list) else [payload]
    if len(locations) != len(POINTS):
        raise ValueError(f"expected {len(POINTS)} locations, got {len(locations)}")
    cols: dict[str, list] = {"boro_code": [], "hour_start": [], "latitude": [], "longitude": []}
    for name in VARIABLES.values():
        cols[name] = []
    for boro, loc in zip(POINTS, locations, strict=True):
        hourly = loc["hourly"]
        times = hourly["time"]
        cols["boro_code"] += [boro] * len(times)
        cols["hour_start"] += [f"{t}:00Z" for t in times]
        cols["latitude"] += [loc["latitude"]] * len(times)
        cols["longitude"] += [loc["longitude"]] * len(times)
        for src, dst in VARIABLES.items():
            values = hourly.get(src) or [None] * len(times)
            if dst == "is_day":
                values = [None if v is None else bool(v) for v in values]
            cols[dst] += values
    table = pa.table(cols)
    if fetched_at is not None:
        hours = pa.array(cols["hour_start"]).cast(pa.timestamp("s", tz="UTC"))
        is_forecast = [h.as_py() > fetched_at for h in hours]
        table = table.append_column("is_forecast", pa.array(is_forecast))
        table = table.append_column(
            "fetched_at", pa.array([fetched_at] * len(table), pa.timestamp("s", tz="UTC"))
        )
    return table


def run(ctx: RunContext, *, force: bool = False) -> None:
    through = date.fromisoformat(ctx.checkpoint("archive_through", "2023-12-31"))
    settled = date.today() - timedelta(days=ARCHIVE_LAG_DAYS)
    while through < settled:
        start = through + timedelta(days=1)
        end = min(settled, start + timedelta(days=CHUNK_DAYS - 1))
        payload = ctx.http.get_json(
            ARCHIVE, params=_params(start_date=start.isoformat(), end_date=end.isoformat())
        )
        ctx.load("weather_hourly", _drop_empty_hours(to_table(payload)))
        ctx.save("archive_through", end.isoformat())
        through = end

    fetched_at = datetime.now(UTC).replace(microsecond=0)
    payload = ctx.http.get_json(FORECAST, params=_params(past_days=7, forecast_days=2))
    ctx.load("weather_recent_hourly", to_table(payload, fetched_at=fetched_at))


def _drop_empty_hours(table: pa.Table) -> pa.Table:
    # The archive returns the requested range even where the reanalysis has no value yet.
    return table.filter(pc.is_valid(table["temperature_c"]))
