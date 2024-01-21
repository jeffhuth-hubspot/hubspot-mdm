-- Reference: https://medium.com/@dipon778/continuous-copy-unload-data-from-snowflake-to-a-external-stage-google-cloud-storage-as-csv-files-3b1ff0ada2ca
-- dbt run-operation create_unload_stream_procedure

{% macro create_unload_stream_procedure() %}
    {% set sql %}

-- Assign grants to execution role
use role accountadmin;
use database source_db;
use warehouse test_wh;
use schema source_db.gcs;




    {% endset %}

    {% do run_query(sql) %}

    {% do log(
        "Created Snowflake stream and procedure to unload a table to csv on GCS",
        info=True,
    ) %}

{% endmacro %}
