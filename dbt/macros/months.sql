{#
  The first of the last n months that have trips, as a literal date. Written as a subquery it is not
  known when the files are picked, and every trip since 2024 is read to build five months.
#}
{% macro months_back(relation, n) %}
    {%- if execute -%}
        {%- set q = "select strftime(max(file_month) - interval " ~ n ~ " month, '%Y-%m-%d') from " ~ relation -%}
        {%- set day = run_query(q).columns[0].values()[0] -%}
        date '{{ day or "2024-01-01" }}'
    {%- else -%}
        date '2024-01-01'
    {%- endif -%}
{% endmacro %}
