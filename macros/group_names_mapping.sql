{% macro group_names_mapping(source_name, sbm_table, xfm_table, repeat_table,exclude_columns=[]) %}
    {% if not repeat_table %}
        {%- set colnames = dbt_utils.get_filtered_columns_in_relation(from=source(source_name, sbm_table),except=exclude_columns) -%}
        {%- set xfm_dict = ona_utils.group_field_dictionary(source_name, xfm_table) -%}
    {% else %}
        {%- set colnames = dbt_utils.get_filtered_columns_in_relation(from=ref(sbm_table),except=exclude_columns) -%}
    {% endif %}
    {%- set fieldlist = [] %}
    {%- set fieldname_mapping = {} %}

    {# Iterate through each column name and process field names #}
    {% for column in colnames %}
        {% set fieldname = column %}
        {%- if not repeat_table and column not in exclude_columns %}
            {% set fieldname = ona_utils.find_index(xfm_dict['airbyte_name'], column, xfm_dict['field']) %}
        {% elif repeat_table and column not in exclude_columns and '/' in column %}
            {% set fieldname = fieldname[fieldname.rfind('/')+1:] %}
        {% endif %}
        {% do fieldname_mapping.update({fieldname: column}) %}
        {% do fieldlist.append(fieldname) %}
    {% endfor %}

    {# Return a dictionary containing both fieldlist and fieldname_mapping #}
    {{ return({'fieldlist': fieldlist, 'fieldname_mapping': fieldname_mapping}) }}
{% endmacro %}