with 

source as (

    select * from {{ source('gcs', 'crm_contacts') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['file_nm', 'file_row_number']) }} as source_contact_key, 
        name,
        email_address,
        phone_number,
        favorite_color,
        title,
        company_name,
        company_domain,
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
