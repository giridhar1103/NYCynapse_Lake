select
    office || '.' || phenomena || '.' || significance || '.' || event_number || '.' || event_year as event_key,
    event_name,
    phenomena                                       as hazard_code,
    case significance when 'W' then 'warning' when 'A' then 'watch' when 'Y' then 'advisory'
        when 'S' then 'statement' end               as alert_level,
    zone,
    boro_code,
    case boro_code when 1 then 'Manhattan' when 2 then 'Bronx' when 3 then 'Brooklyn'
        when 4 then 'Queens' when 5 then 'Staten Island' end as borough,
    starts_at,
    ends_at,
    {{ ny('starts_at') }}                           as starts_at_local,
    {{ ny('ends_at') }}                             as ends_at_local,
    round(epoch(ends_at - starts_at) / 3600.0, 1)   as duration_hours,
    last_action,
    severity,
    headline,
    captured_from
from {{ source('silver', 'weather_alerts') }}
