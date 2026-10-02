select
    trip_uid,
    service_date,
    trip_id,
    route_id,
    direction,
    scheduled_start,
    first_seen_at,
    last_seen_at,
    {{ ny('first_seen_at') }}                       as first_seen_at_local,
    stops_arrived,
    stops_skipped,
    stops_unreached,
    stops_arrived + stops_skipped + stops_unreached as stops_tracked
from {{ source('silver', 'subway_trips') }}
