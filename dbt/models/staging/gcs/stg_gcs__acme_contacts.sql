with 

source as (

    select * from {{ source('gcs', 'acme_contacts') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['file_nm', 'file_row_number']) }} as source_contact_key, 
        name,
        email_address,
        phone_number,
        country,
        title,
        company_name,
        company_domain,
        company_industry,
        company_employees,
        company_revenue,
        created_at,
        updated_at,
        file_nm,
        file_row_number,
        file_content_key,
        file_last_modified,
        start_scan_time

    from source

)

select * from renamed
