-- Macro to generate a translated table from the submissions and choices table pushed by Airbyte Ona Data connector and remove group names recursively with column exclusion parameter
-- Optional parameters and their defaults: default value of labelstable = "chc" ~ rawtable[3:], default value of language = "English (en)", exclude_column = []
-- Example of minimum usage in a model, this utilizes defaults specified above: {{ generate_translated_table('airbyte', 'sbm_table_name'}}
-- Example of usage in a model with specified language: {{ generate_translated_table('airbyte', 'sbm_table_name', language='French (fr)' }}

{% macro generate_translated_table_legacy(source_name, rawtable, labelstable=None, language=None, exclude_columns=[]) %}

    {# Set default values for labelstable and language if not provided #}
    {%- set labelstable = "chc" ~ rawtable[3:] if labelstable is none else labelstable -%}
    {%- set language = "English (en)" if language is none else language -%}
    
    {# Retrieve a list of column names from the source table #}
    {% set colnames = dbt_utils.get_filtered_columns_in_relation(from=source(source_name, rawtable)) %}
    
    {# Initialize an empty list to store transformed field names #}
    {% set fieldlist = [] %}
    
    {# Iterate through each column name and process field names #}
    {% for column in colnames %}
        {% set fieldname = column %}
        {% if column not in exclude_columns and '/' in column %}
            {% set fieldname = fieldname[fieldname.rfind('/')+1:] %}
        {% endif %}
        {% do fieldlist.append(fieldname) %}
    {% endfor %}
    
    {# Create a mapping of modified field names to their original column names #}
    {% set fieldname_mapping = {} %}
    {% for column in colnames %}
        {% set fieldname = column %}
        {% if column not in exclude_columns and '/' in column %}
            {% set fieldname = fieldname[fieldname.rfind('/')+1:] %}
        {% endif %}
        {% do fieldname_mapping.update({fieldname: column}) %}
    {% endfor %}
    
    {# Retrieve a list of values for the "field" column from the labelstable #}
    {%- set optionslist = dbt_utils.get_column_values(table=source(source_name, labelstable), column='field') |list %}
    
    {# Generate the SELECT clause using fieldlist and optionslist #}
    select
    {% for field in fieldlist %}
        {%- if field in optionslist -%}
            t{{optionslist.index(field)}}.label as "{{ field }}"     
        {%- else -%}
            r."{{ fieldname_mapping[field] }}" as "{{ field }}"
        {%- endif %}
        {%- if not loop.last -%}
        ,
        {%- endif %}
    {% endfor %}
    
    {# Generate the main query using rawtable, labelstable, language, and join conditions #}
    from {{ source(source_name, rawtable) }} as r
    
    {# Generate left join conditions for translated labels #}
    {% for option in optionslist -%}
    {% if option in fieldlist %}
        left join {{ source(source_name, labelstable) }} as t{{ optionslist.index(option) }} on t{{ optionslist.index(option) }}.field = '{{ option }}' 
        and r."{{ fieldname_mapping[option] }}"::varchar = t{{ optionslist.index(option) }}.value and t{{ optionslist.index(option) }}.language = '{{ language }}'
    {% endif %}
    {% endfor %}
    
{% endmacro %}
