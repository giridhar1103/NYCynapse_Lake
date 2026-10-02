select
    company,
    file_month,
    {{ ny_parts('pickup_at', 'pickup') }},
    pickup_at,
    dropoff_at,
    {{ ny('requested_at') }}                        as requested_at_local,
    pickup_zone_id,
    dropoff_zone_id,
    trip_miles,
    round(trip_seconds / 60.0, 2)                   as trip_minutes,
    case when requested_at <= pickup_at
         then round(epoch(pickup_at - requested_at) / 60.0, 2) end as wait_minutes,
    base_fare_usd,
    tolls_usd,
    sales_tax_usd,
    black_car_fund_usd,
    congestion_surcharge_usd,
    airport_fee_usd,
    cbd_congestion_fee_usd,
    tips_usd,
    driver_pay_usd,
    round(base_fare_usd + coalesce(tolls_usd, 0) + coalesce(sales_tax_usd, 0)
          + coalesce(black_car_fund_usd, 0) + coalesce(congestion_surcharge_usd, 0)
          + coalesce(airport_fee_usd, 0) + coalesce(cbd_congestion_fee_usd, 0), 2) as rider_paid_usd,
    shared_requested,
    shared_matched,
    access_a_ride,
    wav_requested,
    wav_matched,
    dropoff_at > pickup_at and trip_miles >= 0 and base_fare_usd >= 0
        and trip_miles < 200                         as is_plausible
from {{ source('silver', 'tlc_fhvhv_trips') }}
