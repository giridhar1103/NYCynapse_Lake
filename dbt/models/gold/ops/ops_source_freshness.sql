{#
  How current each lake table is, refreshed every 15 minutes. coverage_end is the latest
  event time in the table; last_loaded_at is when a load last finished. A table is stale when
  its last successful load is older than the source's freshness SLA.
#}
with last_run as (
    select source, max(finished_at) filter (where status = 'succeeded') as last_success_at,
           arg_max(status, started_at) as last_status, arg_max(error, started_at) as last_error
    from {{ source('ops', 'runs') }}
    group by source
)
select
    replace(f.table_name, 'lake.silver.', '')       as table_name,
    f.source,
    s.domain,
    s.cadence,
    s.freshness_sla,
    f.coverage_start,
    f.coverage_end,
    f.row_count,
    f.last_loaded_at,
    r.last_success_at,
    r.last_status,
    r.last_error,
    now() - r.last_success_at > s.freshness_sla      as is_stale,
    round(epoch(now() - f.coverage_end) / 3600.0, 1) as hours_since_latest_event,
    now()                                           as checked_at
from {{ source('ops', 'table_freshness') }} f
join {{ source('ops', 'sources') }} s using (source)
left join last_run r using (source)
