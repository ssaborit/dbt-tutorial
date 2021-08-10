{% macro bq_get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, filter, include_sql_header=false) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none and include_sql_header }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
        {% if predicates %} and {{ predicates | join(' and ') }} {% endif -%}
     {#------------------------ MODIFIED CODE ------------------------#}
        {%- if filter %} and {{ filter }} {% endif -%}
     {#------------------------ MODIFIED CODE ------------------------#}
        then delete

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{% endmacro %}
