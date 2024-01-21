-- Acme has the complete set of fields for each company
-- CRM and RapidData only have the company_name and company_domain
-- This model provides a company lookup to use for the other sources to get the latest company data for a given company_domain
select
    lower(a.company_domain) as company_domain,
    company_name,
    a.company_industry,
    a.company_revenue,
    a.company_employees,
    a.country as country_name,
    c.country_code,
    a.updated_at
from {{ ref('stg_gcs__acme_contacts') }} as a
left outer join {{ ref('stg_gcs__iso_countries') }} as c
    on a.country = c.country_name
qualify row_number() over (partition by company_domain order by updated_at desc) = 1
