{#-- Macro to generate a translated table from the submissions and choices table pushed by Airbyte Ona Data connector and remove group names recursively with column exclusion parameter --#}
{#-- Optional parameters and their defaults: default value of labelstable = "chc__" ~ rawtable[5:], default value of language = "English (en)", exclude_column = [] --#}
{#-- Example of minimum usage in a model, this utilizes defaults specified above: {{ translate_table_remove_group_name('airbyte', 'sbm__table_name'}} --#}
{#-- Example of usage in a model with specified language: {{ translate_table_remove_group_name('airbyte', 'sbm__table_name', language='French (fr)' }} --#}

{% macro translate_table_remove_group_name(source_name, rawtable, labelstable=None, language=None, exclude_columns=[]) %}

{%- set labelstable = "chc__" ~ rawtable[5:] if labelstable is none else labelstable -%}
{%- set language = "English (en)" if language is none else language -%}
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
