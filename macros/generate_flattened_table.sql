-- Macro to dynamically flatten a nested json column in to a table, this only executes first level unnesting. The input can be a source or ref configuration
-- Sample usage with source as input: {{ generate_flattened_table(source('source_name', 'table'), 'json_column', parent_columns=['_id']) }}
-- Sample usage with reference model as input: {{ generate_flattened_table(ref('model'), 'json_column', parent_columns=['_id']) }}


{% macro generate_flattened_table(parent_ref, json_column, parent_columns=[]) %}

-- Generate the main query to flatten JSON data

with json_objects as (
    select 
        {{ parent_ref }}.*,  -- Select all parent columns
        jsonb_array_elements({{ parent_ref }}.{{ json_column }}) as element
    from {{ parent_ref }}
)

select
    {% if parent_columns %}
        -- Include parent columns in the select statement
        {% for col in parent_columns %}
            {{ col }},
        {% endfor %}
    {% endif %}
    {% set json_keys = run_query("WITH json_objects AS (select jsonb_array_elements(" ~ parent_ref ~ "." ~ json_column ~ ") as element from " ~ parent_ref ~ ") select DISTINCT jsonb_object_keys(element) as json_key from json_objects") %}
    {% for row in json_keys %}
        (json_objects.element->>'{{ row.json_key }}')::text as "{{ row.json_key | replace("'", "''") }}"
        {{ ", " if not loop.last else "" }}
    {% endfor %}
from json_objects

{% endmacro %}
