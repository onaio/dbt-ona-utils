{% macro group_field_dictionary(source_name, xfm_table, nlevels = 5 ) %}
    
{% set levelsquery  %}

with leveldata as (
    with level0 as (
    select x.title,
    NULL as group_prefix,
    c ->> 'name' as field,
    c ->> 'type' as type,
    c.value as children
    from {{ source(source_name, xfm_table) }} x,
    jsonb_array_elements(x.children) as c
    )
    {% for i in range(nlevels) %}
    ,
    level{{i + 1}} as (
    select 
    l.title,
    case 
        when l.group_prefix is not null 
            then l.group_prefix || '_' || l.field || '_'
        else l.field || '_' end as group_prefix,
    c ->> 'name' as field, 
    c ->> 'type' as type,
    c.value as children 
    from level{{i}} l,
    jsonb_array_elements(children -> 'children') as c
    where l.type in ('group')
    )
    {% endfor %}
    select * from level0
    where type not in ('group', 'repeat')
    {% for i in range(1, nlevels + 1) %}
    union 
    select * from level{{i}}   
    {% endfor %}
)

select 
    field, 
    group_prefix || field as airbyte_name
from leveldata

{%- endset -%}

{%- set fields_dict = dbt_utils.get_query_results_as_dict(levelsquery) -%}

{{ return(fields_dict) }}

{% endmacro %}