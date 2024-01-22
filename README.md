# HubSpot MDM Engineer Technical Assessment
Submitter: Jeff Huth

## Approach and Assumptions
**Approach**
- **GCS**: Created buckets on for each system with folders for each file type on Google Cloud Storage. These are folders to receive and stage source data and output transformed data.
    - hubspot-acme/contacts/acme__contacts.csv
    - hubspot-crm2/contacts/crm__contacts.csv
    - hubspot-rapid_data/contacts/rapid_data__contacts.csv
    - hubspot_mdm
        - sources/iso/iso_countries: for country names and 2-letter codes
        - output: for dim_contacts.csv
- **Snowflake**: Created and setup Snowflake instance. Snowflake was chosen over BigQuery because I am more familiar w/ Snowflake Snowpark Python and using it with dbt.
    - Initial setup: Set up a trial Snowflake account with SOURCE_DB (source database w/ GCS schema), DEV_DWH (target DWH), TEST_WH (x-small warehouse).
    - Created storage integration, file format, stages, and external tables to source files in GCS.
    - Created storage integration, file format, stage for unloading CSV to GCS.
- **GitHub**: Created personal Git repository (this repository) for dbt and Python development.
- **Python**: Set up virtualenvironment (hubspot-mdm, Python 3.10). Created Jupyter Notebooks to:
    - Installed packages:
        - pandas: for dataframes and data analysis
        - ydata-profiling: to data profile reports for CSVs
        - phonenumbers: to validate phone numbers
        - validators: validate domain and ip_address (ipv4, ipv6)
        - email-validator: to validate email addresses
        - nameparser: to parse first/last names
        - recordlinkags: to create matching rules
    - Profile Data (data-profile-csvs.ipynb): Jupyter Notebook to review each source file to see the shape of the data, unique keys, distinct values, etc.
    - Python Model Dev (python-model-dev.ipynb): Jupyter Notebook to develop the dbt Python models (see below).
        - Connected to Snowflake and analyzed/transformed the data using DataFrames and functions.
        - Experimented with the various functions available in Snowflake Snowpark Conda to see how to best validate and match data.
- **dbt-Cloud**: Set up dbt-Cloud project.
    - Connected to GitHub repo and Snowflake
    - **Models**:
        - staging/gcs: Created source.yml, stg_models, and models.yml to read GCS external tables for each source
        - intermediate:
            - contacts:
                - int_contacts__companies: SQL model to create a companies (w/ ISO countries) lookup table
                - int_contacts__union_all: SQL model to create a union table of the contacts from the 3 sources (Acme, CRM, RapidData) to review all data, all fields, and begin transforming data.
            - matching:
                - int_matching__enrich_contacts: Python model to enrich the Contacts data with additional fields for validation and matching pre-processing.
                - int_matching__match_contacts: Python model to run matching rules on the enriched Contacts to find equivalent matching keys for each Contact.
                - int_matching__deduped_contacts: SQL model to combine the matching keys with the enriched Contacts to normalize and de-duplicate the Contacts.
        - marts:
            - contacts/dim_contacts: SQL model to present the cleansed, normalized, and de-duplicated clients to the end-users.

        ![dbt_lineage](dbt_lineage.png "dbt Lineage DAG")

    - **Macros**: Created the following macros to create/run procedures in Snowflake:
        - create_integration_stages: To create the integrations, stages, and external tables.
        - create_unload_stream_procedure: Attempted to get a stream procedure to work, but ran out of time.
        - trigger_gcs_stage_refresh: A macro to refresh the stage source data from GCS.
        - unload_table_to_gcs: A macro to unload the dim_contacts table to a CSV in the GCC/output folder.
    - **Environments & Jobs**
        - dev_deployment: Created environment to deploy code to DEV_DWH database
        - dev-build-all: Created dbt job (scheduled twice daily) to:
            - Run macro to refresh stage external tables (for GCS Sources). This would pick up new files and process them.
            - dbt build: Run and test the models to create dim_contacts.
            - Run macro to unload dim_contacts as a CSV file to GCS/output bucket/folder.

**Assumptions**
- Files can be delivered to GCS buckets/folders by the source systems. Otherwise, we could set up Dagster or Airflow orchestrations for this delivery.
- Data is primarily US, Canadian, European data 
    - Data will be provided in UTF-8.
    - Functions to parse and validate names, email addresses, and phone numbers assume the Contact data is US/Canadian or European.
        - Emails have a single @ sign and follow traditional formatting rules.
        - Full Names are provided as First Name then Last Name. Logic tries to account for normal US/English prefixes and suffixes.
        - Phone numbers are generally (optional) country code followed by 10-digits.
    - Country only provided for Acme data; but this could help with cleansing, normalizing, and validating names, email addresses, and phone numbers.
    - Company Revenue (integer) is in US Dollars currency.
    - Dates (and datetimes) are in UTC timezone and format.
- Assume and test for discrete lists for job Title and Company Industry. If records come with other values, they will create a warning in dbt.
- People will have at least one of the following differences (though a child living in the same household could share all of these with a parent)
    - Different name
    - Different phone number
    - Different email address
    - Different company or title
    - (Ideally we would have additional mostly unique indicators like IP Address for all records, Device ID, Date of Birth, SSN, etc.)
- The most recent data (latest record) from any of the data sources is the best, most up-to-date version of the data for the common fields:
    - Name
    - Email Address
    - Phone Number
    - Company Name and Domain
    - Title
- Matching rules assume a match on 4+ attributes out of 7 attributes tested (5+ out of 7 for the ALL index). Rules were checked using recordlinkage Python package using 3 indices.
    - Incdices:
        - All records with same last name metaphone
        - All records with same cleaned phone number
        - All records vs. All records (which should be re-factored b/c this would be slow with 10k to 100k to millions of records)
    - Rules tested:
        - Clean last name metaphone
        - Clean first name metaphone
        - First initial
        - Email address
        - Clean phone number w/ optional extension
        - Clean company name using jarowinkler algorithm w/ 0.7 threshold
    - Matches w/ 4+ attributes created a matching key set to help with matching and de-duplicating the Contacts

## Challenges and Considerations
**Challenges**
- The libraries available to dbt Snowflake Snowpark Conda are limited. And it is technically challenging to add an external library to Snowflake/dbt.
- Configuring dbt to run Python models was somewhat challenging and the documentation isn't that great. Fortunately people are helpful on dbt Slack.
- Each dataset had a different set of fields, with only 7 or 8 common fields. I tried to share the country across the Contacts (to help w/ phone and name parsing and validation).
    - I wish I would have shared the IP Address across the Contacts for additional matching key.
- Getting the distinct set of matching keys from recordlinkage was more challenging than expeceted.
    - I struggled with getting my de-duped Contacts down to a unique set that passed the dbt tests.
- Airbyte: I originally tried Airbyte to transfer the data from GCS to Snowflake. But the community connector had issues.
- Setting GCS buckets to BigQuery is really easy. But setting up GCS buckets to Snowflake external tables required some learning.

**Considerations**
- Examine the matching rules to see if these can be improved.
- Work with source systems to see if we can get additional fields (country, IP address, device ID, birthdate, etc.).
- Determine how to create a good, stable, unique, immutable contact_id.
- PII data and security: We need to securely store, transfer, and limit access to this personal data.

## Next steps and productionization
**Next Steps**
- Matching Rules: Add IP address to the matching rules and adjust the thresholds and phonetic checks to see if we could get better match rates.
    - Replace the All vs. All index with an index for email address or IP address.
    - Add a scoring/weighting algorithm and concatenated match key for each type of match.
- Country validation: Add country validation for names, emails, and phone numbers.
- AI/ML: Look into AI algorithms for supervised and unsupervised learning and training the algorithm on existing data sets with the help of an admin analyst.
- Reporting: Create reports and dashboards to visualize and alert analysts about matching and any warnings encountered.

**Productionizing**
- Orchestration: Use a hosted Airflow/Dagster instance to trigger Extract/Load, dbt, and downstream file output
- Extract/Load: Use commercial Airbyte/Fivetran connectors for extracting/loading source data to Snowflake
- Snowflake Python: Add updated external libraries for email validation and name parsing.
- Secure data in Snowflake, GCS, Airflow/Dagset, dbt, and Fivetran/Airbyte.
    - dbt Grants: Add grants to project yaml files to control access to data files, folders, and tables.
    - Create roles, service account users, and business users in GCP/GCS and Snowflake.
    - Snowflake PII Dynamic Masking Rules: Add functions to tag and dynamically mask PII data in Snowflake.