{{ config(materialized='table') }}
{#
  Settled archive values where they exist, model values for the most recent days and the
  next day. is_forecast marks hours that had not happened when the values were fetched.
#}
with archive as (
    select *, false as is_forecast, 'archive' as values_from
    from {{ source('silver', 'weather_hourly') }}
),
recent as (
    select r.* exclude (fetched_at), 'model' as values_from
    from {{ source('silver', 'weather_recent_hourly') }} r
    where r.hour_start > (select max(hour_start) from archive)
),
hours as (
    select boro_code, hour_start, temperature_c, apparent_temperature_c, dew_point_c,
           relative_humidity_pct, precipitation_mm, rain_mm, snowfall_cm, snow_depth_m,
           weather_code, cloud_cover_pct, pressure_msl_hpa, wind_speed_kmh, wind_gusts_kmh,
           wind_direction_deg, is_day, is_forecast, values_from
    from archive
    union all
    select boro_code, hour_start, temperature_c, apparent_temperature_c, dew_point_c,
           relative_humidity_pct, precipitation_mm, rain_mm, snowfall_cm, snow_depth_m,
           weather_code, cloud_cover_pct, pressure_msl_hpa, wind_speed_kmh, wind_gusts_kmh,
           wind_direction_deg, is_day, is_forecast, values_from
    from recent
)
select
    h.boro_code,
    b.boro_name                                     as borough,
    h.hour_start,
    {{ ny_parts('h.hour_start', 'hour') }},
    round(h.temperature_c, 1)                       as temperature_c,
    round(h.temperature_c * 9 / 5 + 32, 1)          as temperature_f,
    round(h.apparent_temperature_c * 9 / 5 + 32, 1) as feels_like_f,
    round(h.dew_point_c * 9 / 5 + 32, 1)            as dew_point_f,
    h.relative_humidity_pct,
    h.precipitation_mm,
    round(h.precipitation_mm / 25.4, 3)             as precipitation_in,
    h.rain_mm,
    h.snowfall_cm,
    round(h.snowfall_cm / 2.54, 2)                  as snowfall_in,
    round(h.snow_depth_m * 39.37, 1)                as snow_depth_in,
    h.weather_code,
    case
        when h.weather_code = 0 then 'clear'
        when h.weather_code between 1 and 3 then 'cloudy'
        when h.weather_code between 45 and 48 then 'fog'
        when h.weather_code between 51 and 57 then 'drizzle'
        when h.weather_code between 61 and 67 or h.weather_code between 80 and 82 then 'rain'
        when h.weather_code between 71 and 77 or h.weather_code between 85 and 86 then 'snow'
        when h.weather_code >= 95 then 'thunderstorm'
        else 'other' end                            as condition,
    h.precipitation_mm >= 0.1                       as is_wet_hour,
    h.precipitation_mm >= 7.6                       as is_heavy_precipitation_hour,
    h.snowfall_cm > 0                               as is_snow_hour,
    h.cloud_cover_pct,
    h.pressure_msl_hpa,
    round(h.wind_speed_kmh / 1.609344, 1)           as wind_speed_mph,
    round(h.wind_gusts_kmh / 1.609344, 1)           as wind_gust_mph,
    h.wind_direction_deg,
    h.is_day,
    h.is_forecast,
    h.values_from
from hours h
join {{ source('silver', 'geo_borough') }} b using (boro_code)
