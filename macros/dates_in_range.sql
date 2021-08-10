{% macro dates_in_range(start_date_str, end_date_str=none, in_fmt='%Y-%m-%d', out_fmt='%Y-%m-%d') %}
    {% set end_date_str = start_date_str if end_date_str is none else end_date_str %}

    {% set start_date = convert_datetime(start_date_str, in_fmt) %}
    {% set end_date = convert_datetime(end_date_str, in_fmt) %}

    {% set day_count = (end_date - start_date).days %}
    {% if day_count < 0 %}
        {% set msg -%}
            Partiton start date is after the end date ({{ start_date }}, {{ end_date }})
        {%- endset %}

        {{ exceptions.raise_compiler_error(msg, model) }}
    {% endif %}

    {% set date_list = [] %}
    {% for i in range(0, day_count + 1) %}
        {% set the_date = (modules.datetime.timedelta(days=i) + start_date) %}
        {% if not out_fmt %}
            {% set _ = date_list.append(the_date | string) %}
        {% else %}
            {% set _ = date_list.append(the_date.strftime(out_fmt) | string) %}
        {% endif %}
    {% endfor %}

    {{ return(date_list) }}
{% endmacro %}
