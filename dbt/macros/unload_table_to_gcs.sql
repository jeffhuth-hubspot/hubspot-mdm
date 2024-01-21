-- With a given db.schema.table_name and GCS bucket, unload the table to a csv file on Google Cloud Storage bucket
-- Reference: https://stackoverflow.com/questions/70065103/external-table-refresh-in-snowflake
-- dbt run-operation unload_table_to_gcs

{% macro unload_table_to_gcs(full_table_name, gcs_bucket) %}
    {% set sql %}

-- Assign grants to execution role
use role accountadmin;
use database source_db;
use warehouse test_wh;
use schema source_db.gcs;

-- Unload table to GCS



    {% endset %}

    {% do run_query(sql) %}

    {% do log(
        "Source data from Google Cloud Storage files to Snowflake stages is refreshed.",
        info=True,
    ) %}

{% endmacro %}
