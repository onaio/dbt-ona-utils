{# Macro takes an airbyte JSON string containing the details of an onadata submission and outputs the SQL to generate a table for each of the keys #}
{# Optional arguments includes a list of metadata field (exclusion list) that are usually not necessary to port over #}
{#-- 2022.12.05 AP. This is a V1 version. Limitations include:
    - All data is stored as text (?)
    - No lables, no direct link with the table schema
    - No optimization by database
    - several others that will come to mind :) --#}

{% macro normalize_onadata_form(
    schema_name="airbyte",
    form_name="",
    json_column="_airbyte_data",
    sample_submission_id="",
    exclusion_list=[
        "_tags",
        "_attachments",
        "_media_count",
        "instanceID",
        "_total_media",
        "_version",
        "_status",
        "_duration",
        "_geolocation",
        "formhub/uuid",
        "_bamboo_dataset_id",
        "_media_all_received",
        "_xform_id_string",
        "_edited"]
) %}

{# Extract the table schema from a specific sample submission. The 'sample_submission_id' is required and should be a submission with all the 
fields included (i.e. no "NULL"), that way the resulting dictionary includes all fields #}
{%- set keys_query -%}
select  jsonb_object_keys("{{json_column}}") as keyname
from (select * from {{schema_name}}."{{form_name}}" 
    {%- if sample_submission_id != '' %}
    where "_airbyte_data" ->> '_id' = {{sample_submission_id}}::VARCHAR         
    {%- endif %}
order by "_airbyte_emitted_at" desc limit 1) as submission_table
{%- endset -%}

{%- set keys = dbt_utils.get_query_results_as_dict(keys_query)["keyname"] -%}

{# Creates SQL query to extract all relevant table fields, excluding most metadata #}
select
    {% for key in keys -%}
    {%- if key not in ['_tags','_attachments', '_media_count', '_total_media', '_version', '_status', '_duration', '_geolocation', 'formhub/uuid', 
        '_bamboo_dataset_id', '_media_all_received', '_xform_id_string','_edited'] -%}
        {%- set keyname = key.split("/")[key.split("/") | length - 1] -%}
    "_airbyte_data" -> '{{key}}' as "{{keyname}}" ,
    {%- endif -%}
    {% endfor %}
_airbyte_emitted_at
from {{ schema_name }}."{{form_name}}"

{% endmacro %}