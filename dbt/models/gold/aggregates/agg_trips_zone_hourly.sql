{{ config(
    materialized='incremental',
    unique_key=['file_month', 'service', 'pickup_date', 'pickup_hour', 'pickup_zone_id'],
    incremental_strategy='delete+insert'
) }}
{#
  Trips per pickup zone and local hour for every for-hire and taxi service. Most questions
  about trip volume, fares or waits by place and time can be answered here without scanning
  hundreds of millions of trips. Measures are sums and counts so they add up across rows;
  divide sums by counts for averages.
#}
{% set months %}
    {% if is_incremental() %}
    and file_month >= (select max(file_month) - interval 4 month from {{ this }})
    {% endif %}
{% endset %}
with trips as (
    select file_month, lower(company) as service, pickup_date, pickup_hour, pickup_zone_id,
           trip_miles, trip_minutes, wait_minutes, rider_paid_usd as fare_usd, tips_usd,
           driver_pay_usd, cbd_congestion_fee_usd, is_plausible
    from {{ ref('fct_rideshare_trip') }}
    where true {{ months }}
    union all
    select file_month, taxi_type, pickup_date, pickup_hour, pickup_zone_id,
           trip_miles, trip_minutes, null, total_usd - coalesce(tip_usd, 0), tip_usd,
           null, cbd_congestion_fee_usd, is_plausible
    from {{ ref('fct_taxi_trip') }}
    where true {{ months }}
)
select
    file_month,
    service,
    pickup_date,
    pickup_hour,
    pickup_zone_id,
    count(*)                                        as trips,
    sum(trip_miles)                                 as trip_miles_sum,
    sum(trip_minutes)                               as trip_minutes_sum,
    sum(wait_minutes)                               as wait_minutes_sum,
    count(wait_minutes)                             as trips_with_wait,
    sum(fare_usd)                                   as fare_usd_sum,
    sum(tips_usd)                                   as tips_usd_sum,
    sum(driver_pay_usd)                             as driver_pay_usd_sum,
    sum(cbd_congestion_fee_usd)                     as cbd_congestion_fee_usd_sum,
    count(*) filter (where cbd_congestion_fee_usd > 0) as trips_charged_cbd_fee
from trips
where is_plausible
group by all
