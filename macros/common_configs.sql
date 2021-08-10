-- filtered_load(filter_column_name): add this macro on top of a model to allow loading selected list of value of column 'filtered_column_name'.
-- It will define the list of values to be used in the queries, as well as passing the filter_column config for incremental loads.
-- IMPORTANT: In order to use this macro for a new field, you need to add a variable in the dbt_project.yml file! Example: new_field: '' for a string.
-- For now, this macro is only working for string and number data types.
{% macro filtered_load(filter_column_name) -%}
    {#-- Generating list of values of filter_column_name if not already a list #}
    {%- if var(filter_column_name)|string not in ('','-1', None) -%}
        {%- if var(filter_column_name) is not iterable %}
            {% set values = [var(filter_column_name)] -%}
        {%- else %}
            {% set values = var(filter_column_name) -%}
        {%- endif -%}
        {{ config(filter_column = filter_column_name, filter_list = values) }} {#-- Configuration only when some values of filter_column_name are selected #}
    {%- endif -%}
    {%- do return(values) %}
{%- endmacro %}


-- date_range_load(): add this macro on top of a model to statically determine a date range of partitions to load.
-- It will pass a list of dates into the partitions config for incremental loads.
{% macro date_range_load(start_date = var('start_date'), end_date = var('end_date')) -%}
  {%- set partition_list = partition_range((start_date, end_date) | join(', ')) -%} {#-- Generating list of dates #}
     {{ config(partitions = partition_list) }}
{%- endmacro %}
