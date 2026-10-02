select
    trip_uid,
    service_date,
    trip_id,
    {{ subway_route('route_id') }}                   as route_id,
    direction,
    stop_id,
    station_id,
    outcome,
    arrived_at,
    {{ ny('arrived_at') }}                          as arrived_at_local,
    first_predicted_at,
    predictions_seen,
    recorded_at
from {{ source('silver', 'subway_stop_events') }}
