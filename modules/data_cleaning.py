import pandas as pd

def merge_billionaires_ed_stats(billionaires, ed_stats_country):
    # Mapping of country names from 'billionaires' to 'ed_stats_country' 'Short Name'
    country_mapping = {
        'Hong Kong': 'Hong Kong SAR, China',
        'South Korea': 'Korea',
        'Eswatini (Swaziland)': 'Swaziland',
        'Bahamas': 'The Bahamas',
        'British Virgin Islands': 'Virgin Islands',
        'Guernsey': 'Channel Islands',
        'Slovakia': 'Slovak Republic',
    }
    
    # Apply the mapping to the 'country' column in the billionaires DataFrame
    billionaires['country'] = billionaires['country'].map(country_mapping).fillna(billionaires['country'])
    
    # Merge the DataFrames
    merged_df = pd.merge(billionaires, ed_stats_country, left_on='country', right_on='Short Name', how='left')
    
    return merged_df