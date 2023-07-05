{#-- Macro to remove group names from data pushed by Airbyte Ona Data connector, to be used in staging models to rename all columns recursively,also provides a option for column exclusion --#}
{#-- Sample usage: in a staging model, {{ remove_group_name('source', 'table', ['group1/column1', 'group2/column1']) }} --#}


{% macro remove_group_name(source_name, table_name, exclude_columns=[]) %}
{% set colnames = dbt_utils.get_filtered_columns_in_relation(from=source(source_name, table_name)) %}

select 
    {% for column in colnames %}
    "{{ column }}"::varchar as
    {% set column_name = column %}
    {% if column not in exclude_columns and '/' in column %}
        {{ column[column.rfind('/')+1:] }}
    {% else %}
        "{{ column }}"
    {% endif %}

    {% if not loop.last %},
    {% endif %}
    {% endfor %}

from {{ source(source_name, table_name) }}
{% endmacro %}