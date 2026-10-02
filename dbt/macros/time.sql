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
