import re
import pandas
import phonenumbers
from email_validator import validate_email, EmailNotValidError
import validators
from recordlinkage.preprocessing import phonetic, clean
import logging

logger = logging.getLogger("python_logger")


# Check Email: Uses Python library email-validator for validating and normalizing email addresses
# Reference: https://github.com/JoshData/python-email-validator
def check_email(email):
    if email is None:
        return None, None
    try:
        validator = validate_email(email, check_deliverability=False)
        is_valid = True
    except (EmailNotValidError, TypeError) as e:
        is_valid = False
        logger.warn(f"INVALID email: {email}, ERROR: {e}")
    return is_valid


# Check IP Address: Uses Python library validators for validating the IP Address
# Reference: https://validators.readthedocs.io/en/latest/#basic-validators
def check_ip_address(ip_address):
    if ip_address == '' or ip_address is None:
        return None
    is_valid = validators.ipv6(ip_address)
    if is_valid:
        # print('Valid ipv6')
        return True
    # print('Not ipv6')
    is_valid = validators.ipv4(ip_address)
    if is_valid:
        # print('Valid ipv4')
        return True
    # print('Not ipv4')
    logger.warn(f"INVALID ip_address: {ip_address}")
    return False


# Check Domain: Uses Python library validators for validating the Company Domain
# Reference: https://validators.readthedocs.io/en/latest/#basic-validators
def check_domain(company_domain):
    if company_domain == "" or company_domain is None:
        return None
    is_valid = validators.domain(company_domain)
    if is_valid:
        # print('Valid domain')
        return True    
    logger.warn(f"INVALID company_domain: {company_domain}")
    return False


# Clean Phone Number: Validate and re-format phone number to E164 w/ extension
# Reference: https://github.com/daviddrysdale/python-phonenumbers
def clean_phone_number(phone, country_code='US'):
    if phone == "" or phone is None:
        return None, None
    # Replace leading 001- with +1-
    if phone[0:4] == '001-':# Clean Phone Numbers: Uses Python library phonenumbers for validating and formatting phone numbers
        phone_number = f"+1-{phone[4:]}"
    else:
        phone_number = phone
    try:
        phone_obj = phonenumbers.parse(phone_number, country_code)
        extension = phone_obj.extension
        formattted_phone_number = phonenumbers.format_number(phone_obj, phonenumbers.PhoneNumberFormat.E164)
        if extension:
            phone_str = f'{formattted_phone_number}x{extension}'
        else:
            phone_str = formattted_phone_number
        return True, phone_str
    except Exception as e:
        logger.warn(f"INVALID phone_number: {phone}, ERROR: {e}")
        return False, str(e)


# Parse Name: Returns first_name, last_name from a full_name that may contain initials, prefixes, suffixes.
# A better option would be to use an external library: nameparser
#   but this library is not available (in standard conda libraries) in Snowflake Snowpark
def parse_name(full_name):
    if full_name == "" or full_name is None:
        return None, None
    first_name = ''
    last_name = ''
    # Remove name prefix and suffix
    prefix = re.search('^((Mr|Mrs|Ms|Miss|Dr|Prof)(\.|\s)+)?', full_name).group()
    suffix = re.search('(\,|\.|\s){0,3}((Sr|Jr|I|II|III|IV|V|JD|MD|PhD)(\.|\s)*){0,3}$', full_name).group()
    if len(suffix) == 0:
        clean_full_name = full_name[len(prefix):].strip().title()
    else:
        clean_full_name = full_name[len(prefix):-len(suffix)].strip().title()
    name_parts = re.findall(r'\s|\,|\.|[^,\s]+', clean_full_name)
    # Deal with full_name: Last, First {M. I.}
    if ',' in name_parts:
        if len(name_parts) >= 2:
            last_name = name_parts[0].strip().title()
            get_index = name_parts.index(',') + 1
            for x in range(get_index, len(name_parts)):
                if len(name_parts[x].strip()) >= 1:
                    first_name = name_parts[x].strip().title()
        elif len(name_parts) == 1:
            first_name = ''
            last_name = name_parts[0].strip().title()
        else:
            first_name = ''
            last_name = ''
    # Deal with full_name: Fist {M. I.} Last
    else:
        if len(name_parts) >= 2:
            first_name = name_parts[0].strip().title()
            last_name = name_parts[-1].strip().title()
        elif len(name_parts) == 1:
            first_name = ''
            last_name = name_parts[-1].strip().title()
        else:
            first_name = ''
            last_name = ''
    # print(f"first_name: {first_name}, last_name: {last_name}")
    return first_name, last_name


# Parse name into first and last name for matching using nameparser library.
#   nameparser is an external Python library not included in Snowflake Snowplow standard conda packages
# Reference: https://github.com/derek73/python-nameparser
# def parse_name(full_name):
#     if full_name == '' or full_name is None:
#         return '', ''
#     first_name = ''
#     last_name = ''
#     try:
#         name_obj = HumanName(full_name)
#         last_name = name_obj.last
#         first_name = name_obj.first
#     except Exception as e:
#         print(f"Error: {e}")
#     return first_name, last_name


# Get First Initial: Substring first letter of first name
def get_first_initial(first_name):
    if first_name == '' or first_name is None:
        return None
    return first_name[0]


def model(dbt, session):
    # Reference: https://blog.devgenius.io/snowflake-python-adapter-for-dbt-9c4c3cac3667
    dbt.config(
        materialized = 'table',
        python_version = '3.10',
        packages = ['pandas', 'phonenumbers', 'email-validator', 'validators', 'recordlinkage']
        # imports = ['@source_db.gcs.other_python_packages/nameparser-1.1.3.zip']
    )

    # Extract imported package zip
    # target_dir = "/tmp/extracts"
    # import zipfile
    # with zipfile.ZipFile(sys._xoptions.get("snowflake_import_directory") + "nameparser-1.1.3.zip", 'r') as zip_file:
    #     zip_file.extractall(target_dir)
    # sys.path.append(target_dir + '/nameparser-1.1.3')
    # from nameparser import HumanName

    # Load Contacts DataFrame from dbt intermediate union all model
    results = dbt.ref("int_contacts__union_all")
    contacts_df = results.to_pandas()

    # Check Email: Add is_email_valid and email_info (normalized email or error message)
    contacts_df['is_email_valid'] =  contacts_df['EMAIL_ADDRESS'].apply(check_email)

    # Check IP Address: Add is_ip_addr_valid
    contacts_df['is_ip_addr_valid'] =  contacts_df['IP_ADDRESS'].apply(check_ip_address)

    # Check Company Domain: Add is_company_domain_valid
    contacts_df['is_company_domain_valid'] =  contacts_df['COMPANY_DOMAIN'].apply(check_domain)

    # Clean Phone Number: Validate and re-format phone number to E164 w/ extension
    # This checks/cleans phone numbers based on 'US' country_code default, b/c only 1 source has country_code populated
    # Also, all phone numbers in the datasets seem to follow the US/Canada phone format.
    contacts_df['is_phone_us_valid'] = contacts_df['PHONE_NUMBER'].apply(clean_phone_number).str[0]
    contacts_df['phone_number_clean'] = contacts_df['PHONE_NUMBER'].apply(clean_phone_number).str[1]

    # Parse Name: Add first_name and last_name to the dataset
    contacts_df['first_name'] = contacts_df['NAME'].apply(parse_name).str[0]
    contacts_df['last_name'] = contacts_df['NAME'].apply(parse_name).str[1]

    # Pre-process names to clean and get phonetic metaphone versions
    # Clean the first, last, company names and add phonetic versions metaphone, match_rating, and nysiis
    contacts_df['first_name_clean'] = clean(contacts_df['first_name'])
    contacts_df['last_name_clean'] = clean(contacts_df['last_name'])
    contacts_df['company_name_clean'] = clean(contacts_df['COMPANY_NAME'])

    # Get First Initial: Add a column for first_initial
    contacts_df['first_initial'] = contacts_df['first_name'].apply(get_first_initial)

    # Metaphone Approach, Reference: https://en.wikipedia.org/wiki/Metaphone
    contacts_df['first_name_mp'] = phonetic(contacts_df['first_name_clean'], 'metaphone')
    contacts_df['last_name_mp'] = phonetic(contacts_df['last_name_clean'], 'metaphone')
    contacts_df['company_name_mp'] = phonetic(contacts_df['company_name_clean'], 'metaphone')

    return contacts_df
