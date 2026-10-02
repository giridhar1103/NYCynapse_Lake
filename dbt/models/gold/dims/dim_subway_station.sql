{{ config(materialized='table') }}
select
    gtfs_stop_id                                as station_id,
    complex_id,
    stop_name                                   as station_name,
    borough,
    boro_code,
    nta_code,
    community_district,
    council_district,
    police_precinct,
    daytime_routes,
    array_to_string(daytime_routes, ' ')        as daytime_routes_text,
    line,
    division,
    structure,
    accessibility,
    accessibility in ('full', 'partial')        as is_accessible,
    in_congestion_zone,
    north_direction_label,
    south_direction_label,
    latitude,
    longitude
from {{ source('silver', 'subway_station') }}
