{% macro generate_translated_table(rawtable, labelstable, language = 'und') %}

{%- set fieldlist =  dbt_utils.get_filtered_columns_in_relation(rawtable) -%}  
{%- set optionslist = dbt_utils.get_column_values(labelstable, column='option') |list %}

select
{% for field in fieldlist %}
    {%- if field in optionslist -%}
    t{{optionslist.index(field)}}.value as {{field}}
    {%- else -%}
    "{{field}}"
    {%- endif %}
    {%- if not loop.last -%}
    ,
    {%- endif %}
{% endfor %}

from {{rawtable}} r
{%- for option in optionslist %}
left join {{labelstable}} as t{{optionslist.index(option)}} on  t{{optionslist.index(option)}}.option = '{{option}}' 
    and r.{{option}}::varchar = t{{optionslist.index(option)}}.label and t{{optionslist.index(option)}}.language = '{{language}}'
{%- endfor %}

{% endmacro %}