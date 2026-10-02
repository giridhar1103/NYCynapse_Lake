select
    alert_id,
    alert_type,
    alert_type like 'Planned%'                      as is_planned_work,
    header_text,
    description_text,
    route_ids,
    stop_ids,
    active_periods,
    (select min(p.starts_at) from unnest(active_periods) t(p))  as first_active_at,
    (select max(p.ends_at) from unnest(active_periods) t(p))    as last_active_at,
    created_at,
    {{ ny('created_at') }}                          as created_at_local,
    updated_at,
    first_seen_at,
    last_seen_at
from {{ source('silver', 'subway_alerts') }}
