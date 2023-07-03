{#-- Macro to remove group names from data pused by airbyte connectors, to be used in staging models to rename all columns recursively --#}
{#-- Sample usage: in a staging model, {{ remove_group_name('airbyte', 'table_name') }} --#}


{% macro remove_group_name(source_name, table_name) %}
{% set colnames = dbt_utils.get_filtered_columns_in_relation(from=source(source_name, table_name)) %}

select 
    {% for column in colnames %}
    "{{ column }}"::varchar as
    {% set column_name = column %}
    {% if '/' in column %}
        {{ column[column.rfind('/')+1:] }}
    {% else %}
        {{ column_name }}
    {% endif %}

    {% if not loop.last %},
    {% endif %}
    {% endfor %}

from {{ source(source_name, table_name) }}
{% endmacro %}