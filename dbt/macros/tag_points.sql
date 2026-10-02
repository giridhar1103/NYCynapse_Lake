{#
  Same lookup the loaders use (nycynapse_lake.geo.tag_points), for points computed in dbt.
  Returns the source columns plus the requested geo columns.
#}
{% macro tag_points(source_sql, lon, lat, columns) -%}
{%- set layers = {
    'boro_code': ('borough', 'TINYINT'), 'nta_code': ('nta', 'VARCHAR'),
    'community_district': ('community_district', 'SMALLINT'), 'modzcta': ('modzcta', 'VARCHAR'),
    'council_district': ('council_district', 'TINYINT'),
    'police_precinct': ('police_precinct', 'SMALLINT'), 'taxi_zone_id': ('taxi_zone', 'SMALLINT')
} -%}
select src.*{% for c in columns %}, g.{{ c }}{% endfor %}
from ({{ source_sql }}) src
left join (
    select _lon, _lat
    {%- for c in columns %},
        try_cast(max(p.code) filter (where p.layer = '{{ layers[c][0] }}') as {{ layers[c][1] }}) as {{ c }}
    {%- endfor %}
    from (select distinct {{ lon }} as _lon, {{ lat }} as _lat from ({{ source_sql }})) pts
    join {{ source('silver', 'geo_piece') }} p on ST_Intersects(p.geom, ST_Point(pts._lon, pts._lat))
    group by _lon, _lat
) g on g._lon = src.{{ lon }} and g._lat = src.{{ lat }}
{%- endmacro %}
