{% docs matching_is_email_valid %}
(Boolean) Check if the Email Address is valid. Uses Python library [email-validator](https://github.com/JoshData/python-email-validator) for validating and normalizing email addresses.
{% enddocs %}

{% docs matching_is_ip_addr_valid %}
(Boolean) Check if the IP Address is valid. Uses Python library [validators](https://validators.readthedocs.io/en/latest/#basic-validators) for validating the IP Address.
{% enddocs %}

{% docs matching_is_company_domain_valid %}
(Boolean) Check if the Company Domain is valid. Uses Python library [validators](https://validators.readthedocs.io/en/latest/#basic-validators) for validating the Company Domain.
{% enddocs %}

{% docs matching_is_phone_us_valid %}
(Boolean) Check if the Phone Number is valid. Uses Python library [phonenumbers](https://github.com/daviddrysdale/python-phonenumbers) for validating the Phone Number.
  Assumed US as the country to check phone number, because not all source records had an assigned country.
{% enddocs %}

{% docs matching_phone_number_clean %}
(String) A validated, reformatted Phone Number (E164 standard) and appended extension. Uses Python library [phonenumbers](https://github.com/daviddrysdale/python-phonenumbers) for formatting the Phone Number.
  Assumed US as the country to check phone number, because not all source records had an assigned country.
{% enddocs %}

{% docs matching_first_name %}
(String) First name parsed from a person`s full name (which may include prefix, suffix, and first or middle intials).
  A better option would be to use an external Python library: [nameparser](https://github.com/derek73/python-nameparser), but this package is not included in the Snowflake Snowplow standard conda packages.
  The current fuction assumes the names are US, Canadian, or European names. A better option would evaluate the name format based on the Country.
{% enddocs %}

{% docs matching_last_name %}
(String) Last name parsed from a person`s full name (which may include prefix, suffix, and first or middle intials).
  A better option would be to use an external Python library: [nameparser](https://github.com/derek73/python-nameparser), but this package is not included in the Snowflake Snowplow standard conda packages.
  The current fuction assumes the names are US, Canadian, or European names. A better option would evaluate the name format based on the Country.
{% enddocs %}

{% docs matching_first_initial %}
(String) The first initial from a person`s first name.
  The current fuction assumes the names are US, Canadian, or European names. A better option would evaluate the name format based on the Country.
{% enddocs %}

{% docs matching_first_name_clean %}
(String) A lower-case version of person`s first name, where special characters and spacing has been removed.
  The current fuction assumes the names are US, Canadian, or European names. A better option would evaluate the name format based on the Country.
{% enddocs %}

{% docs matching_last_name_clean %}
(String) A lower-case version of person`s last name, where special characters and spacing has been removed.
  The current fuction assumes the names are US, Canadian, or European names. A better option would evaluate the name format based on the Country.
{% enddocs %}

{% docs matching_company_name_clean %}
(String) A lower-case version of company`s name, where special characters and spacing has been removed.
  The current fuction assumes the names are US, Canadian, or European names. A better option would evaluate the name format based on the Country.
{% enddocs %}

{% docs matching_first_name_mp %}
(String) A person`s clean first name converted to a [metaphone](https://en.wikipedia.org/wiki/Metaphone), a phonetic version of the name based on pronounciation.
{% enddocs %}

{% docs matching_last_name_mp %}
(String) A person`s clean last name converted to a [metaphone](https://en.wikipedia.org/wiki/Metaphone), a phonetic version of the name based on pronounciation.
{% enddocs %}

{% docs matching_company_name_mp %}
(String) A company`s clean name converted to a [metaphone](https://en.wikipedia.org/wiki/Metaphone), a phonetic version of the name based on pronounciation.
{% enddocs %}


{% docs contact_id %}
(String) A unique ID for each contact.
{% enddocs %}

{% docs matching_keys %}
(String) A set of equivalent keys (of source_customer_key) based on matching rules using the Python package [recordlinkage](https://recordlinkage.readthedocs.io/en/latest/) to pre-process and match equivalent records.
{% enddocs %}

{% docs match_key %}
(String) A unique key (a hash key of the matching_keys array) for each set of matched Contact records.
{% enddocs %}

{% docs sources_arr %}
(JSON Array) An array of sources that have updated the record.
{% enddocs %}
