{{ config(allow_empty=true) }}
{# Problems the health check found, open and recent. #}
select key, source, kind, severity, message, opened_at, last_seen_at, resolved_at,
       resolved_at is null as is_open
from {{ source('ops', 'incidents') }}
where resolved_at is null or resolved_at > now() - interval 30 day
