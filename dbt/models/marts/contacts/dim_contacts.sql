select
    contact_id,
    name,
    email_address,
    phone_number,
    favorite_color,
    country,
    title,
    company_name,
    company_domain,
    company_revenue,
    company_employees,
    company_industry,
    intent_signals,
    do_not_call,
    created_at,
    updated_at
from {{ ref('int_matching__deduped_contacts') }}
