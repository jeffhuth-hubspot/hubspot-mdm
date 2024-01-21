-- Refresh source data from Google Cloud Storage files to Snowflake stages
-- Reference: https://stackoverflow.com/questions/70065103/external-table-refresh-in-snowflake
-- dbt run-operation trigger_gcs_stage_refresh

{% macro trigger_gcs_stage_refresh() %}
    {% set sql %}

-- Assign grants to execution role
use role accountadmin;
use database source_db;
use warehouse test_wh;
use schema source_db.gcs;

-- Refresh data sources
ALTER EXTERNAL TABLE SOURCE_DB.GCS.ACME_CONTACTS REFRESH;
ALTER EXTERNAL TABLE SOURCE_DB.GCS.CRM_CONTACTS REFRESH;
ALTER EXTERNAL TABLE SOURCE_DB.GCS.RAPID_DATA_CONTACTS REFRESH;
ALTER EXTERNAL TABLE SOURCE_DB.GCS.ISO_COUNTRIES REFRESH;

    {% endset %}

    {% do run_query(sql) %}

    {% do log(
        "Source data from Google Cloud Storage files to Snowflake stages is refreshed.",
        info=True,
    ) %}

{% endmacro %}
