-- 2023.05.08 AP. Work in progress model to help the creation of connector tables. Might Be changed later

select 1 

{#
-- create a table name using the right configurations --
{{ config(alias= var('connector_table_name') )}}  

-- create a new table normalizing "airbyte_raw" JSON data into a specific table --
-- table name should be overridden when the model is called, by setting the 'vars' project variables --
{{normalize_onadata_form(schema_name = var('connector_schema_name'), form_name =  '_airbyte_raw_' + var('connector_table_name') )}}

#}
