{{ config(materialized='table') }}
select
    boro_code,
    boro_name                                   as borough,
    round(area_sq_ft / 27878400.0, 2)           as land_area_sq_mi
from {{ source('silver', 'geo_borough') }}
