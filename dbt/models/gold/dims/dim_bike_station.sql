{{ config(materialized='table') }}
select
    station_id,
    short_name                                  as station_number,
    name                                        as station_name,
    capacity                                    as dock_count,
    region,
    region in ('JC District', 'Hoboken District') as is_new_jersey,
    has_charging,
    boro_code,
    nta_code,
    community_district,
    taxi_zone_id,
    latitude,
    longitude,
    first_seen_at,
    last_seen_at
from {{ source('silver', 'bike_station') }}
