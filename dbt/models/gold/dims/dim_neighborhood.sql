{{ config(materialized='table') }}
select
    n.nta_code,
    n.nta_name,
    n.nta_abbrev,
    n.nta_kind,
    n.nta_kind = 'residential'                  as is_residential,
    n.cdta_code,
    c.cdta_name,
    n.boro_code,
    n.boro_name                                 as borough,
    round(n.area_sq_ft / 27878400.0, 3)         as land_area_sq_mi,
    round(ST_Y(ST_Centroid(n.geom)), 6)         as centroid_latitude,
    round(ST_X(ST_Centroid(n.geom)), 6)         as centroid_longitude
from {{ source('silver', 'geo_nta') }} n
left join {{ source('silver', 'geo_cdta') }} c using (cdta_code)
