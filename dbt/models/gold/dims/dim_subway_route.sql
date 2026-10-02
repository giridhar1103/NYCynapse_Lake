{{ config(materialized='table') }}
with latest as (
    select feed_version from {{ source('silver', 'gtfs_feed_version') }}
    order by loaded_at desc limit 1
)
select
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_desc                                as route_description,
    '#' || r.route_color                        as route_color,
    r.route_sort_order,
    r.route_id in ('GS', 'FS', 'H')             as is_shuttle
from {{ source('silver', 'gtfs_route') }} r
join latest using (feed_version)
