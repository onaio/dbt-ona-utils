-- Macro to remove group names from data pushed by Airbyte Ona Data connector to Destination V2
-- This macro can be used to refrence a source or model(in case of repeat tables) in dbt to rename all columns recursively, also provides a option for column exclusion
-- Sample usage with source as input: {{ remove_group_name('source', 'table', ['group1/column1', 'group2/column1']) }} --#
-- Sample usage with repeat table as input: {{remove_group_names_v2(sbm_table='repeat_model_name', repeat_table=True, ['group1/column1', 'group2/column1']) }} 

{% macro remove_group_names_v2(source_name, sbm_table, xfm_table=None, repeat_table=False, exclude_columns=[]) %}

    {% if not repeat_table %}
        {%- set xfm_table = "xfm" ~ sbm_table[3:] if xfm_table is none else xfm_table -%}
        {%- set fieldlist =  dbt_utils.get_filtered_columns_in_relation(from=source(source_name, sbm_table)) -%}  
        {%- set xfm_dict = group_field_dictionary(source_name, xfm_table) -%}
    {% else %}
        {%- set fieldlist = dbt_utils.get_filtered_columns_in_relation(from=ref(sbm_table)) -%}
    {% endif %}

    select 
    {% for field in fieldlist %}
        {%- if not repeat_table and field not in exclude_columns %}
            "{{ field }}" as "{{ find_index(xfm_dict['airbyte_name'], field, xfm_dict['field']) }}"
        {%- elif repeat_table and field not in exclude_columns and '/' in field %}
            "{{ field }}" as {{ field[field.rfind('/')+1:] }}
        {%- else -%}
            "{{ field }}"
        {%- endif %}
        {%- if not loop.last -%} 
            ,
        {%- else -%}
            {{ "\n" }}
        {%- endif -%}
    {%- endfor -%}
    from 
    {%- if not repeat_table %}
        {{ source(source_name, sbm_table) }}
    {%- else %}
        {{ ref(sbm_table) }}
    {%- endif %}

{% endmacro %}