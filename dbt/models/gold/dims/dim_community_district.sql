{{ config(materialized='table') }}
select
    d.boro_cd                                   as community_district,
    d.boro_code,
    b.boro_name                                 as borough,
    d.district_number,
    case when d.is_joint_interest_area
         then b.boro_name || ' joint interest area ' || d.district_number
         else b.boro_name || ' Community District ' || d.district_number end as district_name,
    d.is_joint_interest_area,
    round(d.area_sq_ft / 27878400.0, 3)         as land_area_sq_mi
from {{ source('silver', 'geo_community_district') }} d
join {{ source('silver', 'geo_borough') }} b using (boro_code)
