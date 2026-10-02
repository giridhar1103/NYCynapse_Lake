{{ config(materialized='table') }}
with shapes as (
    select location_id, ST_Centroid(geom) as c
    from {{ source('silver', 'geo_taxi_zone') }}
),
centroids as (
    select location_id, ST_Y(c) as centroid_latitude, ST_X(c) as centroid_longitude from shapes
),
tagged as (
    {{ tag_points("select * from centroids", "centroid_longitude", "centroid_latitude",
                  ["nta_code"]) }}
)
select
    z.location_id                               as zone_id,
    z.zone_name,
    z.borough,
    z.service_zone,
    z.zone_kind,
    z.zone_name ilike '%airport%'               as is_airport,
    z.service_zone = 'Yellow Zone'              as is_yellow_zone,
    t.nta_code                                  as centroid_nta_code,
    round(t.centroid_latitude, 6)               as centroid_latitude,
    round(t.centroid_longitude, 6)              as centroid_longitude
from {{ source('silver', 'tlc_zone') }} z
left join tagged t on t.location_id = z.location_id
