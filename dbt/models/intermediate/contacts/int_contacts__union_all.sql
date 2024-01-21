with
    union_all_relations as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("stg_gcs__acme_contacts"),
                    ref("stg_gcs__crm_contacts"),
                    ref("stg_gcs__rapid_data_contacts"),
                ],
                include=[

                    "SOURCE_CONTACT_KEY",
                    "NAME",
                    "EMAIL_ADDRESS",
                    "PHONE_NUMBER",
                    "TITLE",
                    "COMPANY_NAME",
                    "COMPANY_DOMAIN",

                    "CREATED_AT",
                    "UPDATED_AT",
                    "FILE_NM",
                    "FILE_ROW_NUMBER",
                    "FILE_CONTENT_KEY",
                    "FILE_LAST_MODIFIED",

                    "COUNTRY",
                    "FAVORITE_COLOR",
                    "TITLE",
                    "COMPANY_REVENUE",
                    "COMPANY_EMPLOYEES",
                    "COMPANY_INDUSTRY",
                    "INTENT_SIGNALS",
                    "DO_NOT_CALL",
                    "IP_ADDRESS"
                ],
            )
        }}

    ),

renamed as (
    select
        -- Common fields (in all files)
        r.source_contact_key, -- unique source key
        split_part(split_part(r._dbt_source_relation, '.', 3), '_', 4) as source_name, -- acme, crm, rapid
        r.name, -- Split into parts and lowercase for mathcing; Could change over time (legal name change); Also first names could be represented differently (e.g. Bob, Robert)
        lower(r.email_address) as email_address, -- Possible unique key; Needs to be validated, cleaned, normalized; Could change over time
        r.phone_number, -- Needs to be validated, cleaned, normalized; Could change over time
        r.title, -- Could change over time; Values: CEO (Chief Executive Officer), CIO (Chief Information Officer), COO (Chief Operating Officer), Director, IC, Manager, Senior Director, Senior Vice President, Team Lead, Vice President
        r.company_name, -- Could change over time (as contact gets new jobs)
        lower(r.company_domain) as company_domain, -- Could change over time (as contact gets new jobs); Needs to be validated

        -- Audit fields (also common fields)
        r.created_at,
        r.updated_at,
        r.file_nm,
        r.file_row_number,
        r.file_content_key,
        r.file_last_modified,

        -- File specific fields (not in all files)
        r.favorite_color, -- Values: aqua, black, blue, fuchsia, gray, green, lime, maroon, navy, olive, purple, silver, teal, white, yellow, NULL
        r.intent_signals, -- Array with Values: Contact Form, Demo Request, Product Documentation, Product Page, Whitepaper
        r.do_not_call, -- Values: true, false, NULL
        r.ip_address, -- Needs to be validated
        c.company_revenue, -- 8618790 to 999758851
        c.company_employees, -- 136 to 14998 
        c.company_industry, -- Values: Education, Finance, Healthcare, Logistics, Manufacturing, Tech, NULL
        c.country_name, -- Values: Chile, Germany, Japan, Spain, United Kingdom, United States, NULL
        c.country_code -- Could be used for phone nunber validation, but country is NULL from 2 sources

    from union_all_relations as r
    left outer join {{ ref('int_contacts__companies') }} as c
        on lower(r.company_domain) = c.company_domain
)

select
    *
from renamed
