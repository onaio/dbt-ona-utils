{#-- Macro to generate a translated table from the submissions and choices table pushed by Airbyte Ona Data connector and remove group names recursively with column exclusion parameter --#}
{#-- Sample usage: in a staging model, {{ translate_table_remove_group_name('airbyte', 'sbm__table_name', 'chc__table_name', 'French (fr)', ['group1/column1', 'group2/column1']) }} --#}

{% macro translate_table_remove_group_name(source_name, rawtable, labelstable, language, exclude_columns=[]) %}


{%- set fieldlist =  dbt_utils.get_filtered_columns_in_relation(from=source(source_name, rawtable)) -%}  
{%- set optionslist = dbt_utils.get_column_values(table=source(source_name, labelstable), column='field') |list %}

select
{% for field in fieldlist %}
    {%- if field in optionslist -%}
    t{{optionslist.index(field)}}.label as
        {% if field not in exclude_columns and '/' in field %}
            "{{ field[field.rfind('/')+1:] }}"
        {% else %}
            "{{ field }}"
        {% endif %}        
    {%- else -%}
    r."{{field}}" as
        {% if field not in exclude_columns and '/' in field %}
            "{{ field[field.rfind('/')+1:] }}"
        {% else %}
            "{{ field }}"
        {% endif %}   
    {%- endif %}
    {%- if not loop.last -%}
    ,
    {%- endif %}
{% endfor %}

from {{ source(source_name, rawtable) }} as r

{% for option in optionslist -%}
{% if option in fieldlist %}
    left join {{ source(source_name, labelstable) }} as t{{ optionslist.index(option) }} on t{{ optionslist.index(option) }}.field = '{{ option }}' 
    and r."{{ option }}"::varchar = t{{ optionslist.index(option) }}.value and t{{ optionslist.index(option) }}.language = '{{ language }}'
{% endif %}
{% endfor %}
{% endmacro %}
