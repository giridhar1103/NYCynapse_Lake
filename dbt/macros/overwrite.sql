{#
  A table whose rows are replaced in one transaction, keeping the table itself.

  The usual table materialization drops and recreates, and the lake's catalog keeps every dropped
  version for a month: the status tables alone, rebuilt every 15 minutes, left 14,000 of them.
  Here readers see the old rows until the new ones are in. If the columns changed, the table is
  rebuilt the usual way. An empty result never replaces rows unless the model allows it.
#}
{% materialization overwrite, adapter='duckdb' %}
  {%- set target = this.incorporate(type='table') -%}
  {%- set existing = load_cached_relation(this) -%}
  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- set same = false -%}
  {%- if existing is not none and existing.type == 'table' and not should_full_refresh() -%}
    {%- set have = adapter.get_columns_in_relation(existing) -%}
    {%- set want = get_column_schema_from_query(sql) -%}
    {%- set have_names = have | map(attribute='name') | list -%}
    {%- set want_names = want | map(attribute='name') | list -%}
    {%- set have_types = have | map(attribute='data_type') | map('lower') | list -%}
    {%- set want_types = want | map(attribute='data_type') | map('lower') | list -%}
    {%- set same = have_names == want_names and have_types == want_types -%}
  {%- endif -%}

  {%- if same -%}
    {%- set staged = '_overwrite_' ~ this.identifier -%}
    {% call statement('stage') -%}
      create or replace temp table {{ staged }} as {{ sql }}
    {%- endcall %}
    {%- set counts = run_query('select (select count(*) from temp.main.' ~ staged ~ '), (select count(*) from ' ~ target ~ ')') -%}
    {%- set fresh, old = counts.rows[0][0], counts.rows[0][1] -%}
    {%- if fresh == 0 and old > 0 and not config.get('allow_empty', false) -%}
      {{ exceptions.raise_compiler_error(this.identifier ~ ': the new result is empty, kept the ' ~ old ~ ' old rows') }}
    {%- endif -%}
    {% call statement('main') -%}
      delete from {{ target }};
      insert into {{ target }} select * from temp.main.{{ staged }};
    {%- endcall %}
    {% do run_query('drop table if exists temp.main.' ~ staged) %}
  {%- else -%}
    {%- if existing is not none -%}{{ drop_relation_if_exists(existing) }}{%- endif -%}
    {% call statement('main') -%}
      {{ create_table_as(False, target, sql) }}
    {%- endcall %}
  {%- endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {% do persist_docs(target, model) %}
  {{ return({'relations': [target]}) }}
{% endmaterialization %}
