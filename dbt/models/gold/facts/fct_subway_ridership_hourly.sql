select
    {{ ny_parts('hour_start', 'hour') }},
    hour_start,
    transit_mode,
    station_complex_id                              as complex_id,
    station_complex                                 as complex_name,
    borough,
    payment_method,
    fare_class,
    riders,
    transfers
from {{ source('silver', 'subway_ridership_hourly') }}
