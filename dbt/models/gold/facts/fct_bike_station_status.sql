select
    station_id,
    reported_at,
    {{ ny_parts('reported_at', 'reported') }},
    bikes_available,
    ebikes_available,
    bikes_available - coalesce(ebikes_available, 0) as classic_bikes_available,
    docks_available,
    bikes_disabled,
    docks_disabled,
    is_installed,
    is_renting,
    is_returning,
    bikes_available = 0 and is_renting              as is_empty,
    docks_available = 0 and is_returning            as is_full
from {{ source('silver', 'bike_station_status') }}
