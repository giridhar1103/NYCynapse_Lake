{# New York wall-clock time for an instant, as a timestamp without a zone. #}
{% macro ny(col) -%}
    ({{ col }} AT TIME ZONE 'America/New_York')
{%- endmacro %}

{# The usual trio of local columns for an instant: local timestamp, date and hour. #}
{% macro ny_parts(col, prefix) -%}
    {{ ny(col) }} AS {{ prefix }}_at_local,
    CAST({{ ny(col) }} AS DATE) AS {{ prefix }}_date,
    CAST(hour({{ ny(col) }}) AS TINYINT) AS {{ prefix }}_hour
{%- endmacro %}

{# The realtime feed calls the Staten Island Railway SS; the schedule and riders call it SI. #}
{% macro subway_route(col) -%}
    case when {{ col }} = 'SS' then 'SI' else {{ col }} end
{%- endmacro %}

{# Origin time, route and direction from a subway trip id: 104950_7..N97R -> 104950_7_N. #}
{% macro subway_trip_key(col) -%}
    regexp_extract({{ col }}, '^([0-9]{6})_', 1) || '_' || regexp_extract({{ col }}, '^[0-9]{6}_([A-Z0-9]+)', 1)
    || '_' || regexp_extract({{ col }}, '^[0-9]{6}_[A-Z0-9]+\.+([NS])', 1)
{%- endmacro %}
