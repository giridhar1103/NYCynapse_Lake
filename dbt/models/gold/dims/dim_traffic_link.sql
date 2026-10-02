{{ config(materialized='table') }}
select
    link_id,
    link_name,
    borough,
    round(length_m / 1609.344, 2)               as length_mi,
    nta_code,
    community_district,
    taxi_zone_id,
    mid_latitude                                as latitude,
    mid_longitude                               as longitude,
    last_reading_at
from {{ source('silver', 'traffic_link') }}
