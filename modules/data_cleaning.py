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

def clean_and_prepare_df(df):
    # Drop columns with too many nulls (2000+) or redundant
    df = df.drop(columns=['organization', 'title', 'Other groups', 'Vital registration complete', 'Alternative conversion factor'])
    
    # Convert datetime columns
    placeholder_date = pd.Timestamp('2024-03-01')
    df['birthDate'] = pd.to_datetime(df['birthDate']).fillna(placeholder_date)
    df['date'] = pd.to_datetime(df['date'])
    df['birthYear'] = df['birthYear'].fillna(-1).astype(int)
    df['birthMonth'] = df['birthMonth'].fillna(-1).astype(int)
    df['birthDay'] = df['birthDay'].fillna(-1).astype(int)
    df['National accounts reference year'] = df['National accounts reference year'].fillna(-1).astype(int)
    uncleaned_dates = ['Latest population census', 'Latest agricultural census', 'Latest industrial data', 'Latest trade data', 'Latest water withdrawal data']
    
    # Convert uncleaned date columns to datetime objects, NaN --> placeholder_date
    for col in uncleaned_dates:
        df[col] = df[col].astype(str).str.extract('(\d{4})')
        df[col] = pd.to_datetime(df[col], format='%Y', errors='coerce').fillna(placeholder_date)
    
    # Mean Imputation for numerical columns
    numerical_columns = ['age', 'cpi_country', 'cpi_change_country', 'gdp_country', 'gross_tertiary_education_enrollment', 'gross_primary_education_enrollment_country', 'life_expectancy_country', 'tax_revenue_country_country', 'total_tax_rate_country', 'population_country', 'latitude_country', 'longitude_country', 'Currency Unit']
    for col in numerical_columns:
        df[col] = df[col].fillna(df['age'].mean())
    
    # Handle categorical columns
    categorical_columns = ['category', 'city', 'country', 'state', 'gender', 'status', 'residenceStateRegion', 'source', 'industries', 'countryOfCitizenship', 'Country Code', 'Short Name', 'Table Name', 'Long Name', '2-alpha code', 'Region', 'Income Group', 'WB-2 code', 'National accounts base year', 'SNA price valuation', 'Lending category', 'System of National Accounts', 'PPP survey year', 'External debt Reporting status', 'System of trade', 'Government Accounting concept', 'IMF data dissemination standard', 'Latest household survey', 'Source of most recent Income and expenditure data', 'Balance of Payments Manual in use']
    for col in categorical_columns:
        df[col] = df[col].astype('category').cat.add_categories(['Unknown'])
        df[col] = df[col].fillna('Unknown')
    
    # Fill nan for remaining columns
    df['firstName'] = df['firstName'].fillna('Unknown')
    df['Special Notes'] = df['Special Notes'].fillna('None')
    
    return df