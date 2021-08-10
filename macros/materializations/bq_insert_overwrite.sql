{% macro bq_insert_overwrite(tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, filter_column, filter_list) %}

  {% if partitions is not none and partitions != [] %} {# static #}

      {% set predicate -%}
          {{ partition_by.render(alias='DBT_INTERNAL_DEST') }} in (
              {{ '\"' + partitions|join('\", \"') + '\"' }}
          )
      {%- endset %}

      {%- set source_sql -%}
        (
          {{sql}}
        )
      {%- endset -%}

      {#------------------------ MODIFIED CODE ------------------------#}
      {%- if filter_column is not none and filter_list is not none -%} {# static filter #}
          {%- if filter_list[0] is number -%}  {# Integer: no quotes #}
            {%- set filter -%}
                DBT_INTERNAL_DEST.{{ filter_column }} in ({{ filter_list|join(', ') }})
            {%- endset -%}
          {%- else -%}  {# String: quotes #}
            {%- set filter -%}
                DBT_INTERNAL_DEST.{{ filter_column }} in ({{ '\"' + filter_list|join('\", \"') + '\"' }})
            {%- endset -%}
          {%- endif -%}
      {%- endif -%}

      {{ bq_get_insert_overwrite_merge_sql(target_relation, source_sql, dest_columns, [predicate], filter, include_sql_header=true) }}
      {#------------------------ MODIFIED CODE ------------------------#}

  {% else %} {# dynamic #}

      {% set predicate -%}
          {{ partition_by.render(alias='DBT_INTERNAL_DEST') }} in unnest(dbt_partitions_for_replacement)
      {%- endset %}

      {%- set source_sql -%}
      (
        select * from {{ tmp_relation }}
      )
      {%- endset -%}

      -- generated script to merge partitions into {{ target_relation }}
      declare dbt_partitions_for_replacement array<{{ partition_by.data_type }}>;
      declare _dbt_max_partition {{ partition_by.data_type }} default (
          select max({{ partition_by.field }}) from {{ this }}
          where {{ partition_by.field }} is not null
      );

      -- 1. create a temp table
      {{ create_table_as(True, tmp_relation, sql) }}

      -- 2. define partitions to update
      set (dbt_partitions_for_replacement) = (
          select as struct
              array_agg(distinct {{ partition_by.render() }})
          from {{ tmp_relation }}
      );

      {#------------------------ MODIFIED CODE ------------------------#}
      {%- if filter_column -%}
        {%- set filter -%}
            DBT_INTERNAL_DEST.{{ filter_column }} in unnest(dbt_filters_for_replacement)
        {%- endset -%}
        declare dbt_filters_for_replacement default(
            select array_agg(distinct {{ filter_column }}) from {{ tmp_relation }} );
      {%- endif -%}
      {#------------------------ MODIFIED CODE ------------------------#}

      {#
        TODO: include_sql_header is a hack; consider a better approach that includes
              the sql_header at the materialization-level instead
      #}
      -- 3. run the merge statement
      {#------------------------ MODIFIED CODE ------------------------#}
      {{ bq_get_insert_overwrite_merge_sql(target_relation, source_sql, dest_columns, [predicate], filter, include_sql_header=false) }};
      {#------------------------ MODIFIED CODE ------------------------#}
      -- 4. clean up the temp table
      drop table if exists {{ tmp_relation }}

  {% endif %}

{% endmacro %}
