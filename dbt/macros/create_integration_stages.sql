-- Reference: https://medium.com/contino-engineering/snowflake-integration-with-google-cloud-storage-d56119cf9a1a
-- dbt run-operation create_integration_stages

{% macro create_integration_stages() %}
    {% set sql %}

-- Assign grants to execution role
use role accountadmin;
use database source_db;
use warehouse test_wh;
use schema source_db.gcs;

create or replace storage integration gcs_storage_integration
    type = external_stage
    storage_provider = gcs
    enabled = true
    storage_allowed_locations = ( 'gcs://hubspot-acme', 'gcs://hubspot-crm2', 'gcs://hubspot-rapid_data', 'gcs://hubspot_mdm') -- GCS buckets
;

create or replace file format csv_format
    type = csv
    field_delimeter = ','
    skip_header = 1
    null_if = ('NULL', 'null')
    empty_field_as_null = true
    compression = none;

create or replace stage gcs_acme_contacts
    storage_integration = gcs_storage_integration
    url = 'gcs://hubspot-acme/contacts'
    file_format = csv_format;

create or replace stage gcs_crm_contacts
    storage_integration = gcs_storage_integration
    url = 'gcs://hubspot-crm2/contacts'
    file_format = csv_format;

create or replace stage gcs_rapid_data_contacts
    storage_integration = gcs_storage_integration
    url = 'gcs://hubspot-rapid_data/contacts'
    file_format = csv_format;

-- Reference: https://github.com/datasets/country-list/blob/master/data.csv
create or replace stage gcs_iso_countries
storage_integration = gcs_storage_integration
url = 'gcs://hubspot_mdm/sources/iso'
file_format = csv_format;

-- Reference: https://medium.com/@dipon778/continuous-copy-unload-data-from-snowflake-to-a-external-stage-google-cloud-storage-as-csv-files-3b1ff0ada2ca
create or replace stage gcs_output_contacts
storage_integration = gcs_storage_integration
url = 'gcs://hubspot_mdm/output'
file_format = csv_format;


CREATE OR REPLACE EXTERNAL TABLE SOURCE_DB.GCS.ACME_CONTACTS(
    NAME varchar AS (value:c1::varchar),
    EMAIL_ADDRESS varchar AS (value:c2::varchar),
    PHONE_NUMBER varchar AS (value:c3::varchar),
    COUNTRY varchar AS (value:c9::varchar),
    TITLE varchar AS (value:c4::varchar),
    COMPANY_NAME varchar AS (value:c5::varchar),
    COMPANY_DOMAIN varchar AS (value:c6::varchar),
    COMPANY_INDUSTRY varchar AS (value:c10::varchar),
    COMPANY_EMPLOYEES int AS (value:c11::int),
    COMPANY_REVENUE int AS (value:c12::int),
    CREATED_AT date AS TRY_TO_DATE(value:c7::varchar,'YYYY-MM-DD'),
    UPDATED_AT date AS TRY_TO_DATE(value:c8::varchar,'YYYY-MM-DD'),
    FILE_NM varchar as (METADATA$FILENAME::varchar),
    FILE_ROW_NUMBER int as (METADATA$FILE_ROW_NUMBER::int),
    FILE_CONTENT_KEY varchar as (METADATA$FILE_CONTENT_KEY::varchar),
    FILE_LAST_MODIFIED timestamp as (METADATA$FILE_LAST_MODIFIED::timestamp),
    START_SCAN_TIME timestamp as (METADATA$START_SCAN_TIME::timestamp)
)
LOCATION=@gcs_acme_contacts
PATTERN='.*contacts.*[.]csv'
REFRESH_ON_CREATE = TRUE
AUTO_REFRESH = FALSE
file_format = (
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_BLANK_LINES = TRUE
    TRIM_SPACE = TRUE
)
;


CREATE OR REPLACE EXTERNAL TABLE SOURCE_DB.GCS.CRM_CONTACTS(
    NAME varchar AS (value:c1::varchar),
    EMAIL_ADDRESS varchar AS (value:c2::varchar),
    PHONE_NUMBER varchar AS (value:c3::varchar),
    FAVORITE_COLOR varchar AS (value:c9::varchar),
    TITLE varchar AS (value:c4::varchar),
    COMPANY_NAME varchar AS (value:c5::varchar),
    COMPANY_DOMAIN varchar AS (value:c6::varchar),
    CREATED_AT date AS TRY_TO_DATE(value:c7::varchar,'YYYY-MM-DD'),
    UPDATED_AT date AS TRY_TO_DATE(value:c8::varchar,'YYYY-MM-DD'),
    FILE_NM varchar as (METADATA$FILENAME::varchar),
    FILE_ROW_NUMBER int as (METADATA$FILE_ROW_NUMBER::int),
    FILE_CONTENT_KEY varchar as (METADATA$FILE_CONTENT_KEY::varchar),
    FILE_LAST_MODIFIED timestamp as (METADATA$FILE_LAST_MODIFIED::timestamp),
    START_SCAN_TIME timestamp as (METADATA$START_SCAN_TIME::timestamp)
)
LOCATION=@gcs_crm_contacts
PATTERN='.*contacts.*[.]csv'
REFRESH_ON_CREATE = TRUE
AUTO_REFRESH = FALSE
file_format = (
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_BLANK_LINES = TRUE
    TRIM_SPACE = TRUE
)
;


CREATE OR REPLACE EXTERNAL TABLE SOURCE_DB.GCS.RAPID_DATA_CONTACTS(
    NAME varchar AS (value:c1::varchar),
    EMAIL_ADDRESS varchar AS (value:c2::varchar),
    PHONE_NUMBER varchar AS (value:c3::varchar),
    IP_ADDRESS varchar AS (value:c9::varchar),
    TITLE varchar AS (value:c4::varchar),
    COMPANY_NAME varchar AS (value:c5::varchar),
    COMPANY_DOMAIN varchar AS (value:c6::varchar),
    INTENT_SIGNALS variant AS (value:c10::variant),
    DO_NOT_CALL boolean AS (value:c11::boolean),
    CREATED_AT date AS TRY_TO_DATE(value:c7::varchar,'YYYY-MM-DD'),
    UPDATED_AT date AS TRY_TO_DATE(value:c8::varchar,'YYYY-MM-DD'),
    FILE_NM varchar as (METADATA$FILENAME::varchar),
    FILE_ROW_NUMBER int as (METADATA$FILE_ROW_NUMBER::int),
    FILE_CONTENT_KEY varchar as (METADATA$FILE_CONTENT_KEY::varchar),
    FILE_LAST_MODIFIED timestamp as (METADATA$FILE_LAST_MODIFIED::timestamp),
    START_SCAN_TIME timestamp as (METADATA$START_SCAN_TIME::timestamp)
)
LOCATION=@gcs_rapid_data_contacts
PATTERN='.*contacts.*[.]csv'
REFRESH_ON_CREATE = TRUE
AUTO_REFRESH = FALSE
file_format = (
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_BLANK_LINES = TRUE
    TRIM_SPACE = TRUE
)
;


CREATE OR REPLACE EXTERNAL TABLE SOURCE_DB.GCS.ISO_COUNTRIES(
    COUNTRY_NAME varchar AS (value:c1::varchar),
    COUNTRY_CODE varchar AS (value:c2::varchar),
    FILE_NM varchar as (METADATA$FILENAME::varchar),
    FILE_ROW_NUMBER int as (METADATA$FILE_ROW_NUMBER::int),
    FILE_CONTENT_KEY varchar as (METADATA$FILE_CONTENT_KEY::varchar),
    FILE_LAST_MODIFIED timestamp as (METADATA$FILE_LAST_MODIFIED::timestamp),
    START_SCAN_TIME timestamp as (METADATA$START_SCAN_TIME::timestamp)
)
LOCATION=@gcs_iso_countries
PATTERN='.*countries.*[.]csv'
REFRESH_ON_CREATE = TRUE
AUTO_REFRESH = FALSE
file_format = (
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_BLANK_LINES = TRUE
    TRIM_SPACE = TRUE
)
;


-- Create stage for other python packages
-- Standard Snowpark contains these packages needed: pandas, phonenumbers, recordlinkage, validators, email_validator
CREATE STAGE OTHER_PYTHON_PACKAGES;
-- Added external library to stage: nameparser package (wheel file and wheel loader)
-- Reference: https://pypi.org/project/nameparser/#files


    {% endset %}

    {% do run_query(sql) %}

    {% do log(
        "Created GCS stages and external tables in Snowflake",
        info=True,
    ) %}

{% endmacro %}
