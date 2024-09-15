{#-- Macro to generate a translated table from the submissions and choices table pushed by Airbyte Ona Data connector with option to remove group names recursively with column exclusion parameter --#}
{#-- This macro by default expects a referenced model as input but can be used on a source to also remove group names with translation. This is dictated by the remove_group_name --#}
{#-- If the macro is to be used to translate a repeat table model, then the remove group_names and repeat_table parameters need to be True --#}
{#-- The langueage default is 'undefined' this can be changed by explicitly adding the language parameter when using the macro --#}

{#-- Example of minimum usage with referenced model as input, this utilizes set defaults: {{ generate_translated_table('source', 'model', 'choice_table') }} --#}
{#-- Example of usage in a referenced model with specified language: {{ generate_translated_table('source', 'model', 'choice_table', language='French (fr)') }} --#}
{#-- Example of usage in a source using remove group name parameter: {{ generate_translated_table('source', 'table', remove_group_names=True) }} --#}
{#-- Example of usage in a referenced repeat table using the required parameters: {{ generate_translated_table('source', 'model', 'choice_table', remove_group_names=True, repeat_table=True ) }} }} --#}


{% macro generate_translated_table(source_name, sbm_table, labelstable=None, language=None, xfm_table=None, remove_group_names=False, repeat_table=False, exclude_columns=[]) %}

{%- set xfm_table = "xfm" ~ sbm_table[3:] if remove_group_names and not repeat_table else xfm_table -%}
{%- set labelstable = "chc" ~ sbm_table[3:] if remove_group_names and not repeat_table else labelstable -%}
{%- set language = "undefined" if language is none else language -%}

{% if not remove_group_names %}
    {%- set fieldlist = dbt_utils.get_filtered_columns_in_relation(from=ref(sbm_table)) -%}
{% else %}
    {%- set mapping = ona_utils.group_names_mapping(source_name, sbm_table, xfm_table, repeat_table) -%}
    {%- set fieldlist = mapping['fieldlist'] %}
    {%- set fieldname_mapping = mapping['fieldname_mapping'] %}
{% endif %}

{%- set optionslist = dbt_utils.get_column_values(table=source(source_name, labelstable), column='field') | list %}

select
{% for field in fieldlist %}
    {%- if field in optionslist %}
        t{{ optionslist.index(field) }}.label as "{{ field }}"
    {%- else %}
        {%- if not remove_group_names %}
            r."{{ field }}" as "{{ field }}"
        {%- else %}
            r."{{ fieldname_mapping[field] }}" as "{{ field }}"
        {%- endif %}
    {%- endif %}
    {%- if not loop.last -%}
        ,
    {%- endif %}
{% endfor %}
from 
{%- if not remove_group_names or repeat_table %}
    {{ ref(sbm_table) }} r
{%- else %}
    {{ source(source_name, sbm_table) }} as r
{%- endif %}

{# Generate left join conditions for translated labels #}
{% for option in optionslist %}
    {% if option in fieldlist %}
        left join {{ source(source_name, labelstable) }} as t{{ optionslist.index(option) }} 
        on t{{ optionslist.index(option) }}.field = '{{ option }}' 
        and 
        {%- if not remove_group_names %}
            r.{{ option }}::varchar
        {%- else %}
            r."{{ fieldname_mapping[option] }}"::varchar
        {%- endif %}
        = t{{ optionslist.index(option) }}.value 
        and t{{ optionslist.index(option) }}.language = '{{ language }}'
    {% endif %}
{% endfor %}

{% endmacro %}
>>>>>>> 231d1adb335397ccfea59eba7c1a2d65859120a8
