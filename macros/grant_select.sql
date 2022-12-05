{#-- Macro to provide permission to a user (usually the user that will be connected to the BI tool) to read from the tables generated in the DBT prod schema --#}
{#-- Sample usage: in a dbt job, execute << dbt run-operation grant_select --args '{user: tree_aid_read}' >>--#}

{% macro grant_select(schema=target.schema, user=target.user) %}

  {% if target.name == 'default' %}     {# runs only if the target for the job is the dbt schema, therefore only if the job is running in prod #}

  {% set sql %}
  grant usage on schema {{ schema }} to {{ user }};
  grant select on all tables in schema {{ schema }} to {{ user }};
  {% endset %}

  {{ log('Granting select on all tables and views in schema ' ~ target.schema ~ ' to role ' ~ user, info=True) }}
  {% do run_query(sql) %}
  {{ log('Privileges granted', info=True) }}

  {% endif %} 
{% endmacro %}