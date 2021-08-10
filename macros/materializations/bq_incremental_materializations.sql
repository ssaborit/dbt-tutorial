-- modified version of dbt's default macro to add insert into strategy
{% macro dbt_bigquery_validate_get_incremental_strategy(config) %}
  {#-- Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy", default="merge") -%}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    {#------------------------ MODIFIED CODE ------------------------#}
    Expected one of: 'merge', 'insert_overwrite', 'insert_into'
  {%- endset %}
  {% if strategy not in ['merge', 'insert_overwrite', 'insert_into'] %}
    {#------------------------ MODIFIED CODE ------------------------#}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}

--modified materialization to sinclude insert_into strategy type
{% materialization incremental, adapter='bigquery' -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {%- set target_relation = this %}
  {%- set existing_relation = load_relation(this) %}
  {%- set tmp_relation = make_temp_relation(this) %}

  {#-- Validate early so we don't run SQL if the strategy is invalid --#}
  {% set strategy = dbt_bigquery_validate_get_incremental_strategy(config) -%}

  {%- set raw_partition_by = config.get('partition_by', none) -%}
  {%- set partition_by = adapter.parse_partition_by(raw_partition_by) -%}
  {%- set partitions = config.get('partitions', none) -%}
  {%- set cluster_by = config.get('cluster_by', none) -%}
  {%- set filter_column = config.get('filter_column', none) -%}
  {%- set filter_list = config.get('filter_list', none) -%}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view %}
      {#-- There's no way to atomically replace a view with a table on BQ --#}
      {{ adapter.drop_relation(existing_relation) }}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
      {#-- If the partition/cluster config has changed, then we must drop and recreate --#}
      {% if not adapter.is_replaceable(existing_relation, partition_by, cluster_by) %}
          {% do log("Hard refreshing " ~ existing_relation ~ " because it is not replaceable") %}
          {{ adapter.drop_relation(existing_relation) }}
      {% endif %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
     {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
     {#------------------------ MODIFIED CODE ------------------------#}
     {% if strategy == 'insert_into' %}
        {% set build_sql = bq_insert_into(
          target_relation,
          sql) %}

     {#-- if partitioned, use BQ scripting to get the range of partition values to be updated --#}
     {% elif strategy == 'insert_overwrite' %}
     {#------------------------ MODIFIED CODE ------------------------#}
        {% set missing_partition_msg -%}
          The 'insert_overwrite' strategy requires the `partition_by` config.
        {%- endset %}
        {% if partition_by is none %}
          {% do exceptions.raise_compiler_error(missing_partition_msg) %}
        {% endif %}

        {% set build_sql = bq_insert_overwrite(
            tmp_relation,
            target_relation,
            sql,
            unique_key,
            partition_by,
            partitions,
            dest_columns,
            filter_column,
            filter_list) %}

     {% else %}
       {#-- wrap sql in parens to make it a subquery --#}
       {%- set source_sql -%}
         (
           {{sql}}
         )
       {%- endset -%}

       {% set build_sql = get_merge_sql(target_relation, source_sql, unique_key, dest_columns) %}

     {% endif %}

  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {% endcall %}

  {{ run_hooks(post_hooks) }}

  {% set target_relation = this.incorporate(type='table') %}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
