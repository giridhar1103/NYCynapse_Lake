select
    link_id,
    observed_at,
    {{ ny_parts('observed_at', 'observed') }},
    speed_mph,
    case when travel_time_seconds >= 0 then travel_time_seconds end as travel_time_seconds,
    status_code,
    status_code = 0                                 as is_valid
from {{ source('silver', 'traffic_speed_obs') }}
