{{ config(
    materialized='incremental',
    unique_key=['trip_uid', 'stop_id'],
    incremental_strategy='delete+insert'
) }}
{#
  Observed subway arrivals matched to the schedule in force on the service date.
  The realtime trip id is the tail of the schedule's trip id, and the service pattern has to
  run on that date. Trips with no scheduled match (added or rerouted trains) are kept with
  an empty scheduled time.
#}
with events as (
    select *
    from {{ source('silver', 'subway_stop_events') }}
    where outcome = 'arrived'
    {% if is_incremental() %}
      and service_date >= (select max(service_date) - 1 from {{ this }})
    {% endif %}
),
versions as (
    select d.service_date, v.feed_version
    from (select distinct service_date from events) d
    join {{ source('silver', 'gtfs_feed_version') }} v
      on d.service_date between v.feed_start_date and v.feed_end_date
    qualify row_number() over (partition by d.service_date order by v.loaded_at desc) = 1
),
active as (
    select v.service_date, v.feed_version, c.service_id
    from versions v
    join {{ source('silver', 'gtfs_calendar') }} c
      on c.feed_version = v.feed_version
     and v.service_date between c.start_date and c.end_date
     and case isodow(v.service_date)
            when 1 then c.monday when 2 then c.tuesday when 3 then c.wednesday
            when 4 then c.thursday when 5 then c.friday when 6 then c.saturday
            else c.sunday end
    union
    select v.service_date, v.feed_version, x.service_id
    from versions v
    join {{ source('silver', 'gtfs_calendar_date') }} x
      on x.feed_version = v.feed_version and x.service_date = v.service_date
     and x.exception_type = 1
    except
    select v.service_date, v.feed_version, x.service_id
    from versions v
    join {{ source('silver', 'gtfs_calendar_date') }} x
      on x.feed_version = v.feed_version and x.service_date = v.service_date
     and x.exception_type = 2
),
scheduled_trips as (
    select a.service_date, a.feed_version, t.realtime_trip_id, any_value(t.trip_id) as trip_id
    from active a
    join {{ source('silver', 'gtfs_trip') }} t
      on t.feed_version = a.feed_version and t.service_id = a.service_id
    group by all
),
matched as (
    select
        e.*,
        s.feed_version,
        s.trip_id                                   as scheduled_trip_id,
        timezone('America/New_York', cast(e.service_date as timestamp)
                 + to_seconds(st.arrival_seconds))  as scheduled_arrival_at,
        st.stop_sequence
    from events e
    left join scheduled_trips s
      on s.service_date = e.service_date and s.realtime_trip_id = e.trip_id
    left join {{ source('silver', 'gtfs_stop_time') }} st
      on st.feed_version = s.feed_version and st.trip_id = s.trip_id and st.stop_id = e.stop_id
)
select
    m.trip_uid,
    m.service_date,
    m.trip_id,
    {{ subway_route('m.route_id') }}                 as route_id,
    m.direction,
    m.stop_id,
    m.station_id,
    st.station_name,
    st.complex_id,
    st.borough,
    st.nta_code,
    m.arrived_at,
    {{ ny_parts('m.arrived_at', 'arrived') }},
    m.departed_at,
    m.arrival_source,
    m.scheduled_trip_id is not null                 as is_scheduled_trip,
    m.scheduled_arrival_at,
    cast(epoch(m.arrived_at - m.scheduled_arrival_at) as integer) as delay_seconds,
    m.stop_sequence,
    m.feed_version
from matched m
left join {{ ref('dim_subway_station') }} st on st.station_id = m.station_id
