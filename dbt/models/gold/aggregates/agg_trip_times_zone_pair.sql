{#
  How long a car trip between two taxi zones usually takes, and what riders usually pay, by
  weekday or weekend and local hour, for yellow and green taxis and for app rides (Uber, Lyft)
  separately, since their fares differ (a yellow taxi to JFK pays a flat fare). Built from the
  last three months of plausible taxi and app trips. Medians come from 2 minute (and 2 dollar) buckets, so the aggregation can spill to disk
  instead of holding every trip in memory. Pairs with fewer than 8 trips in an hour are left out.
#}
with recent as (
    select max(file_month) - interval 2 month as since from {{ ref('fct_rideshare_trip') }}
),
trips as (
    select 'app' as ride_kind, pickup_zone_id, dropoff_zone_id, pickup_date, pickup_hour,
           trip_minutes, rider_paid_usd as fare_usd
    from {{ ref('fct_rideshare_trip') }}
    where is_plausible and file_month >= (select since from recent)
    union all
    select 'taxi', pickup_zone_id, dropoff_zone_id, pickup_date, pickup_hour,
           trip_minutes, total_usd - coalesce(tip_usd, 0)
    from {{ ref('fct_taxi_trip') }}
    where is_plausible and file_month >= (select since from recent)
),
bucketed as (
    select
        ride_kind,
        pickup_zone_id,
        dropoff_zone_id,
        case when dayofweek(pickup_date) in (0, 6) then 'weekend' else 'weekday' end as day_type,
        pickup_hour,
        least(floor(trip_minutes / 2), 90)::int            as minute_bucket,
        least(floor(greatest(fare_usd, 0) / 2), 150)::int   as fare_bucket
    from trips
    where pickup_zone_id <> dropoff_zone_id and trip_minutes between 1 and 240
),
minutes as (
    select ride_kind, pickup_zone_id, dropoff_zone_id, day_type, pickup_hour, minute_bucket,
           count(*) as n
    from bucketed
    group by all
),
minutes_cum as (
    select *,
        sum(n) over w / sum(n) over (partition by ride_kind, pickup_zone_id, dropoff_zone_id,
                                     day_type, pickup_hour) as share
    from minutes
    window w as (partition by ride_kind, pickup_zone_id, dropoff_zone_id, day_type, pickup_hour
                 order by minute_bucket rows between unbounded preceding and current row)
),
fares as (
    select ride_kind, pickup_zone_id, dropoff_zone_id, day_type, pickup_hour, fare_bucket,
           count(*) as n
    from bucketed
    group by all
),
fares_cum as (
    select *,
        sum(n) over w / sum(n) over (partition by ride_kind, pickup_zone_id, dropoff_zone_id,
                                     day_type, pickup_hour) as share
    from fares
    window w as (partition by ride_kind, pickup_zone_id, dropoff_zone_id, day_type, pickup_hour
                 order by fare_bucket rows between unbounded preceding and current row)
),
minute_stats as (
    select ride_kind, pickup_zone_id, dropoff_zone_id, day_type, pickup_hour,
        sum(n)                                                  as trips,
        min(minute_bucket) filter (where share >= 0.25) * 2 + 1 as minutes_p25,
        min(minute_bucket) filter (where share >= 0.5) * 2 + 1  as minutes_median,
        min(minute_bucket) filter (where share >= 0.75) * 2 + 1 as minutes_p75
    from minutes_cum
    group by all
),
fare_stats as (
    select ride_kind, pickup_zone_id, dropoff_zone_id, day_type, pickup_hour,
        min(fare_bucket) filter (where share >= 0.5) * 2 + 1   as fare_usd_median
    from fares_cum
    group by all
)
select
    m.ride_kind,
    m.pickup_zone_id,
    m.dropoff_zone_id,
    m.day_type,
    m.pickup_hour,
    m.trips,
    m.minutes_p25,
    m.minutes_median,
    m.minutes_p75,
    f.fare_usd_median,
    (select since from recent)                                as since_month
from minute_stats m
join fare_stats f using (ride_kind, pickup_zone_id, dropoff_zone_id, day_type, pickup_hour)
where m.trips >= 8
