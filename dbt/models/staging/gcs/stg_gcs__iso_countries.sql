-- Reference: ISO Country Names and Codes Dataset, https://github.com/datasets/country-list/blob/master/data.csv

with 

source as (

    select * from {{ source('gcs', 'iso_countries') }}

),

renamed as (

    select
        -- value,
        country_code,
        country_name,
        file_nm,
        file_row_number,
        file_content_key,
        file_last_modified,
        start_scan_time

    from source

)

select * from renamed
