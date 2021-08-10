{% macro partition_range(raw_partition_date, date_fmt='%Y-%m-%d') %}
    {% set partition_range = (raw_partition_date | string).split(",") %}

    {% if (partition_range | length) == 1 %}
      {% set start_date = partition_range[0] %}
      {% set end_date = none %}
    {% elif (partition_range | length) == 2 %}
      {% set start_date = partition_range[0] %}
      {% set end_date = partition_range[1] %}
    {% else %}
      {{ exceptions.raise_compiler_error("Invalid partition time. Expected format: {Start Date}[,{End Date}]. Got: " ~ raw_partition_date) }}
    {% endif %}

    {{ return(dates_in_range(start_date, end_date, in_fmt=date_fmt)) }}
{% endmacro %}
