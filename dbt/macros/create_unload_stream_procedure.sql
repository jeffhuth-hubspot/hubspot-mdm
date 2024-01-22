-- This creates a stage, stream, procedure, and task to automatically write a CSV to GCS every time DIM_CONTACT is updated/refreshed.
-- Reference: https://medium.com/@dipon778/continuous-copy-unload-data-from-snowflake-to-a-external-stage-google-cloud-storage-as-csv-files-3b1ff0ada2ca
-- dbt run-operation create_unload_stream_procedure

{% macro create_unload_stream_procedure() %}
    {% set sql %}

-- Assign grants to execution role
use role accountadmin;
use database dev_dwh;
use warehouse test_wh;
use schema dev_dwh.mdm_contacts;

create or replace storage integration gcs_storage_integration_out
    type = external_stage
    storage_provider = gcs
    enabled = true
    storage_allowed_locations = ( 'gcs://hubspot-acme', 'gcs://hubspot-crm2', 'gcs://hubspot-rapid_data', 'gcs://hubspot_mdm')
;

-- Create storage integration
create or replace stage gcs_output_contacts
storage_integration = gcs_storage_integration_out
url = 'gcs://hubspot_mdm/output'
file_format = ( format_name = 'csv_format', type = CSV);
;

CREATE OR REPLACE FILE format csv_format
TYPE = csv
FIELD_DELIMITER= ','
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null')
EMPTY_FIELD_AS_NULL = true;


--  Create Stream
create or replace stream output_contacts_stream
on table dev_dwh.mdm_contacts.dim_contacts;


-- Copy table to stage manually
-- copy into '@gcs_output_contacts/dim_contacts.csv'
-- from dev_dwh.mdm_contacts.dim_contacts
-- FILE_FORMAT = (TYPE = CSV, COMPRESSION = NONE)
-- overwrite = true
-- DETAILED_OUTPUT = TRUE
-- SINGLE = TRUE;


-- Create or replace a stored procedure

--? Create or replace a stored procedure
CREATE OR REPLACE PROCEDURE copy_to_gcs_output()
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS
 $$
 
    //COPY INTO OBJECT CONSTRUCT(*)
    //With Dynamic Pathing with JS MS TS for different unload naming convention
    //Copy into does not load file with same name but different timestamps
    //Result Place Holder
    var result="";
     
    //Build SQL
    var dynamicpath = Date.now().toString() || '.csv';
     
    var sql00 = `copy into @gcs_output_contacts/dim_contacts_` + dynamicpath + ` FROM output_contacts_stream
     FILE_FORMAT = (TYPE = CSV, COMPRESSION = NONE) DETAILED_OUTPUT = TRUE SINGLE = TRUE overwrite=true;
     `;
     
    var sql01 = `select count(*) from @gcs_output_contacts/dim_contacts_` + dynamicpath + ` (FILE_FORMAT = (TYPE = CSV, COMPRESSION = NONE));`;
 
    //Execute SQL
    try {
        var stmt00 = snowflake.createStatement( { sqlText: sql00 } );
        stmt00.execute();
         
        var stmt01 = snowflake.createStatement( { sqlText: sql01 } );
        var rs01 = stmt01.execute();
        rs01.next();
        var rowCount = (stmt01.getRowCount()>0) ? rs01.getColumnValue(1) : 0;
          
        result = "Succeeded! Rows Unloaded(" + rowCount + ")";
    }
     
    catch (err)  {
        result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
        result += "\n  Message: " + err.message;
        result += "\nStack Trace:\n" + err.stackTraceTxt; 
    }
     
    return result;
    $$;


-- Create or replace your task
CREATE OR REPLACE TASK copy_to_gcs_output_task
  WAREHOUSE = TEST_WH
  SCHEDULE = '1 minute'
  COMMENT = 'Task to unload data to GCS'
  WHEN
    SYSTEM$STREAM_HAS_DATA('output_contacts_stream')
AS  
BEGIN
  CALL copy_to_gcs_output();
END;

    {% endset %}

    {% do run_query(sql) %}

    {% do log(
        "Created Snowflake stream, procedure, and taskt to listen to data changese on DIM_CONTACT unload the table to csv on GCS.",
        info=True,
    ) %}

{% endmacro %}
