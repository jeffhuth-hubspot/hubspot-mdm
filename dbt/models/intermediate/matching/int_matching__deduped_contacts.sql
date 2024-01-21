with 
    match_keys as (
        select
            m."matches" as match_key_arr, -- These are in alphabetic order, not created_at order
            -- Convert match key array to a hash key; then coalesce for source records without matches
            coalesce({{ dbt_utils.generate_surrogate_key(['match_key_arr']) }}, source_contact_key) as match_key,
            ec.*
        from {{ ref('int_matching__enrich_contacts') }} as ec
        left outer join {{ ref('int_matching__match_contacts') }} as m
            on array_contains(ec.source_contact_key::variant, m."matches")
        -- order by 2, 3
    ),

    -- Ordered Match Keys: source_contact_key ordered by created_at
    ordered_match_keys as (
        select
            match_key,
            array_agg(source_contact_key) within group (order by created_at) as match_key_arr,
            array_sort(array_agg(distinct source_name)) as sources_arr,
            min(created_at) as created_at,
            min(case when created_at is not null then created_at end) as min_created_at,
            max(case when created_at is not null then created_at end) as max_created_at,
            coalesce(max(case when updated_at is not null then updated_at end), max_created_at) as max_updated_at
        from match_keys
        group by 1
    ),

    -- Master contact_id = first source_contact_key
    first_source as (
        select
            match_key,
            source_name,
            source_contact_key as contact_id,
            created_at,
            updated_at,
            file_nm,
            file_row_number
        from match_keys
        qualify row_number() over (partition by match_key order by created_at asc) = 1
    ),

    latest_source as (
        select
            match_key,
            source_contact_key,
            source_name,
            concat("first_name", ' ', "last_name") as name,
            email_address,
            "phone_number_clean" as phone_number,
            title,
            company_name,
            company_domain,
            created_at,
            updated_at,
            file_nm,
            file_row_number
        from match_keys
        qualify row_number() over (partition by match_key order by updated_at desc) = 1
    ),

    latest_valid_email as (
        select
            match_key,
            source_contact_key,
            email_address,
            updated_at
        from match_keys
        where "is_email_valid"
        qualify row_number() over (partition by match_key order by updated_at desc) = 1
    ),

    latest_valid_phone as (
        select
            match_key,
            source_contact_key,
            "phone_number_clean" as phone_number,
            updated_at
        from match_keys
        where "is_phone_us_valid"
        qualify row_number() over (partition by match_key order by updated_at desc) = 1
    ),

    -- Latest CRM record (for favorite_color)
    latest_crm as (
        select
            match_key,
            source_contact_key,
            favorite_color,
            updated_at
        from match_keys
        where source_name = 'crm'
        qualify row_number() over (partition by match_key order by updated_at desc) = 1
    ),

    -- Latest RapidData for ip_address and intent_signals
    latest_rapid_data as (
        select
            match_key,
            source_contact_key,
            ip_address,
            intent_signals,
            do_not_call,
            updated_at
        from match_keys
        where source_name = 'rapid'
        qualify row_number() over (partition by match_key order by updated_at desc) = 1
    )

select
    fs.contact_id,
    fs.match_key,
    omk.match_key_arr, -- array of source_contact_keys by created_at
    array_size(omk.match_key_arr) as match_key_count,
    omk.sources_arr, -- array of all soures that have updated the record
    fs.source_name as first_source_nm,  

    -- Latest (valid) common fields
    ls.source_name as latest_source_nm,
    ls.name,
    coalesce(eml.email_address, ls.email_address) as email_address,
    coalesce(phn.phone_number, ls.phone_number) as phone_number,
    ls.title,

    -- Latest Company fields
    comp.country_code,
    comp.country_name as country,
    coalesce(ls.company_name, comp.company_name) as company_name,
    coalesce(ls.company_domain, comp.company_domain) as company_domain,
    comp.company_industry,
    comp.company_employees,
    comp.company_revenue,
    
    -- Latest CRM and RapidData fields
    crm.favorite_color,
    rd.ip_address,
    rd.intent_signals,
    rd.do_not_call,

    -- Audit fields
    fs.file_nm as first_file_nm,
    fs.file_row_number as first_file_row_number,
    ls.file_nm as latest_file_nm,
    ls.file_row_number as latest_file_row_number,
    omk.min_created_at as created_at,
    array_max([omk.max_updated_at, comp.updated_at])::date as updated_at

from first_source as fs

inner join ordered_match_keys as omk
    on fs.match_key = omk.match_key

inner join latest_source as ls
    on fs.match_key = ls.match_key

left outer join latest_valid_email as eml
    on fs.match_key = eml.match_key

left outer join latest_valid_phone as phn
    on fs.match_key = phn.match_key

left outer join latest_crm as crm
    on fs.match_key = crm.match_key

left outer join latest_rapid_data as rd
    on fs.match_key = rd.match_key

left outer join {{ ref('int_contacts__companies') }} as comp
    on ls.company_domain = comp.company_domain

qualify row_number() over (partition by fs.contact_id order by array_size(omk.match_key_arr) desc, omk.max_updated_at desc) = 1
