-- Unload the DIM_CONTACTS table to a csv file on Google Cloud Storage bucket
-- Reference: https://stackoverflow.com/questions/70065103/external-table-refresh-in-snowflake
-- dbt run-operation unload_table_to_gcs

{% macro unload_table_to_gcs(full_table_name, gcs_bucket) %}
    {% set sql %}

-- Assign grants to execution role
use role accountadmin;
use database dev_dwh;
use warehouse test_wh;
use schema dev_dwh.mdm_contacts;

-- Unload table to GCS
copy into '@gcs_output_contacts/dim_contacts.csv'
from dev_dwh.mdm_contacts.dim_contacts
FILE_FORMAT = (TYPE = CSV, COMPRESSION = NONE)
overwrite = true
DETAILED_OUTPUT = TRUE
SINGLE = TRUE;


    {% endset %}

    {% do run_query(sql) %}

    {% do log(
        "DIM_CONTACTS table unloaded to a csv file on Google Cloud Storage hubspot_mdm/output directory.",
        info=True,
    ) %}

{% endmacro %}
