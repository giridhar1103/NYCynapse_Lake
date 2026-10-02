{{ config(materialized='table') }}
select
    complex_id,
    string_agg(distinct station_name, ' / ' order by station_name) as complex_name,
    any_value(borough)                          as borough,
    any_value(boro_code)                        as boro_code,
    any_value(nta_code)                         as nta_code,
    list_sort(list_distinct(flatten(list(daytime_routes)))) as daytime_routes,
    count(*)                                    as station_count,
    bool_or(is_accessible)                      as is_accessible,
    avg(latitude)                               as latitude,
    avg(longitude)                              as longitude
from {{ ref('dim_subway_station') }}
group by complex_id
