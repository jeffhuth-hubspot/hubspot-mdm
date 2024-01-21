import re
import pandas
import pandas as pd
import recordlinkage
import logging

logger = logging.getLogger("python_logger")


# Python function to find equivalent match keys for each Contact
def model(dbt, session):
    # Reference: https://blog.devgenius.io/snowflake-python-adapter-for-dbt-9c4c3cac3667
    dbt.config(
        materialized = 'table',
        python_version = '3.10',
        packages = ['pandas', 'recordlinkage'],
    )

    # Load Contacts DataFrame from dbt intermediate match contacts model
    results = dbt.ref("int_matching__enrich_contacts")
    contacts_df = results.to_pandas()

    # Set unique index on contacts_df
    contacts_df = contacts_df.set_index('SOURCE_CONTACT_KEY')

    # Sort the dataframme by last_name
    contacts_df.sort_values('last_name').head()

    # Match and de-duplicate with the recordlinkage Python library
    # Reference: https://recordlinkage.readthedocs.io/en/latest/guides/data_deduplication.html
    # Example project Jupyter Notebook: https://github.com/mayerantoine/2019-intro-patient-matching/blob/master/Introduction%20to%20Patient%20Matching.ipynb

    # Initialize indexer: Only compare records with the same last_name_mp (metaphone)
    indexer_1 = recordlinkage.Index()
    indexer_1.block(left_on="last_name_mp")
    candidate_links_1 = indexer_1.index(contacts_df)

    # Initialize indexer: Only compare records with the same phone_number_clean
    indexer_2 = recordlinkage.Index()
    indexer_2.block(left_on="phone_number_clean")
    candidate_links_2 = indexer_2.index(contacts_df)

    # Every record against every record
    # WARNING: This works for smaller datasets (< 10k records), but takes time and should not be used for very large datasets.
    indexer_3 = recordlinkage.Index()
    indexer_3.full()
    candidate_links_3 = indexer_3.index(contacts_df)

    # Specify fields to be compared and method for comparison
    # This function uses several pre-processed fields from the int_matching__enrich_contacts model,
    #   and only includes fields common across the datasets.
    #   This would be improved if we had an IP Address or Device ID from all datasets, or to first de-dupe within a dataset.
    compare_cl = recordlinkage.Compare()
    compare_cl.exact("last_name_mp", "last_name_mp", label="last_name_mp")
    compare_cl.exact("first_name_mp", "first_name_mp", label="first_name_mp")
    compare_cl.exact("first_initial", "first_initial", label="first_initial")
    compare_cl.exact("EMAIL_ADDRESS", "EMAIL_ADDRESS", label="email_address")
    compare_cl.exact("phone_number_clean", "phone_number_clean", label="phone_number_clean")
    compare_cl.exact("TITLE", "TITLE", label="title")
    compare_cl.string("company_name_clean", "company_name_clean", method="jarowinkler", threshold=0.7, label="company_name_clean")

    # Use the comparisons with the indexes to create feature sets for matching analysis
    features_1 = compare_cl.compute(candidate_links_1, contacts_df)
    logging.info('features_1 report: Listed below are matches based on the number of matching features')
    logging.info(features_1.sum(axis=1).value_counts().sort_index(ascending=False))

    features_2 = compare_cl.compute(candidate_links_2, contacts_df)
    logging.info('features_2 report: Listed below are matches based on the number of matching features')
    logging.info(features_2.sum(axis=1).value_counts().sort_index(ascending=False))

    features_3 = compare_cl.compute(candidate_links_3, contacts_df)
    logging.info('features_3 report: Listed below are matches based on the number of matching features')
    logging.info(features_3.sum(axis=1).value_counts().sort_index(ascending=False))

    # Create dataframes of matches based on each feature set and the required number of matching features
    # Since features 1 & 2 are limited based on their index (last_name_mp and phone_number_clean), 
    #   the records need to match on 4 of the 7 compared fields.
    # The full compare needs to match on 5 of the 7 compared fields.
    matches_1 = features_1[features_1.sum(axis=1) >= 4]
    matches_2 = features_2[features_2.sum(axis=1) >= 4]
    matches_3 = features_3[features_3.sum(axis=1) >= 5]

    # The matches (above) produce a 1-to-1 matching dataframe. 
    # This next looping function builds a unique array set of all equivalent matched records within and across the data sources.
    # The 1-to-1 matches are squashed into distinct lists of matched IDs.
    # Combined Matches: combines matches_1 to 3 and sorts by the SOURCE_CONTACT_KEY
    comnbined_matches = pd.concat([matches_1, matches_2, matches_3]).sort_values(by = ['SOURCE_CONTACT_KEY_1', 'SOURCE_CONTACT_KEY_2'])
    
    # Create array of key pair mappings
    key_maps = []
    # Get all key maps and squash distinct sets of matched ids
    for idx, row in comnbined_matches.iterrows():
        keys = [idx[0], idx[1]]
        key_maps.append(keys)
    key_maps.sort(key=lambda x: [x[0], x[1]])

    # Consolidate key pair mappings into groups, so that each key only appears in one group
    i = 0
    groups = []
    for km in key_maps:
        in_group = False
        if i == 0:
            groups.append(km)
        else:
            for grp in groups:
                if km[0] in grp and km[1] not in grp:
                    grp.append(km[1])
                    in_group = True
                elif km[1] in grp and km[0] not in grp:
                    grp.append(km[0])
                    in_group = True
                elif km[1] in grp and km[0] in grp:
                    in_group = True
        if not(in_group):
            groups.append(km)
        i = i + 1

    # Ensure each group is a unique, sorted set of keys
    new_groups = []
    for grp in groups:
        if len(grp) > 0:
            new_grp = sorted(list(set(grp)))
            new_groups.append(new_grp)

    # Create DataFrame from new_groups; one column (matches), and one row for each match-set (list of IDs, SOURCE_CONTACT_KEYs)
    new_df = pd.DataFrame({"matches": new_groups})

    return new_df
