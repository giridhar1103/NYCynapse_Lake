{#
  How current each lake table is, refreshed every 15 minutes. coverage_end is the latest
  event time in the table; last_loaded_at is when a load last finished. A table is stale when
  its last successful load is older than the source's freshness SLA.

  Coverage is what a question may rely on, so stray event times do not stretch it: the taxi files
  hold a few trips dated 2001 or a week past their month, and the live feeds a few stops from 1970.
  Taxi tables cover the months whose files were loaded, live feeds start at their first run, and
  nothing starts before the lake does (January 2024, New York time).
#}
with tlc_files as (
    select 'tlc_' || regexp_extract(url, '(yellow|green|fhvhv)_tripdata_', 1) || '_trips' as tbl,
           timezone('America/New_York',
               max(cast(regexp_extract(url, '_(\d{4}-\d{2})', 1) || '-01' as date))
               + interval 1 month - interval 1 second) as ends
    from {{ source('ops', 'upstream_files') }}
    where source = 'tlc_trips' and loaded_run_id is not null
      and regexp_matches(url, '(yellow|green|fhvhv)_tripdata_\d{4}-\d{2}')
    group by 1
),
first_run as (
    select source, min(started_at) as at from {{ source('ops', 'runs') }} group by source
),
last_run as (
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
    case when f.coverage_start is not null then greatest(
        f.coverage_start,
        case when f.source in ('subway_realtime', 'citibike_live') then first_run.at
             else timestamptz '2024-01-01 00:00:00-05' end
    ) end                                           as coverage_start,
    least(f.coverage_end, coalesce(tlc_files.ends, f.coverage_end)) as coverage_end,
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
left join first_run using (source)
left join tlc_files on tlc_files.tbl = replace(f.table_name, 'lake.silver.', '')
