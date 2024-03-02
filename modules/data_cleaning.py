import pandas as pd

def merge_billionaires_ed_stats(billionaires, ed_stats_country, country_df):
    # Existing mapping from 'ed_stats_country' to 'billionaires'
    country_mapping_ed_stats = {
        'Hong Kong SAR, China': 'Hong Kong',
        'Korea': 'South Korea',
        'Swaziland': 'Eswatini (Swaziland)',
        'The Bahamas': 'Bahamas',
        'Virgin Islands': 'British Virgin Islands',
        'Channel Islands': 'Guernsey',
        'Slovak Republic': 'Slovakia',
    }
    
    # Mapping for correcting 'countryOfCitizenship' in the merged dataframe to match 'country_df'
    country_correction_mapping = {
        'Hong Kong': 'China',
        'Taiwan': 'China',
        'Ireland': 'Republic of Ireland',
        'Eswatini (Swaziland)': 'Eswatini',
        'Guernsey': 'United Kingdom',
        'Macau': 'China',
        'St. Kitts and Nevis': 'Saint Kitts and Nevis',
    }
    
    # Create a temporary mapping column in 'ed_stats_country' DataFrame
    ed_stats_country['Mapped Country'] = ed_stats_country['Short Name'].map(country_mapping_ed_stats).fillna(ed_stats_country['Short Name'])
    
    # Merge the DataFrames using the new 'Mapped Country' column in 'ed_stats_country' to match 'countryOfCitizenship' in 'billionaires'
    merged_df = pd.merge(billionaires, ed_stats_country, left_on='countryOfCitizenship', right_on='Mapped Country', how='left')
    
    # Correct the 'countryOfCitizenship' in the merged dataframe to align with 'country_df'
    merged_df['countryOfCitizenship'] = merged_df['countryOfCitizenship'].map(country_correction_mapping).fillna(merged_df['countryOfCitizenship'])
    
    # Perform the second merge with 'country_df' using the corrected 'countryOfCitizenship' values
    final_merged_df = pd.merge(merged_df, country_df, left_on='countryOfCitizenship', right_on='Country', how='left')
    
    # Optionally, clean up the dataframe by dropping any temporary or unnecessary columns
    final_merged_df.drop(columns=['Mapped Country'], inplace=True)
    
    return final_merged_df

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
    numerical_columns = ['age', 'cpi_country', 'cpi_change_country', 'gdp_country', 'gross_tertiary_education_enrollment', 'gross_primary_education_enrollment_country', 'life_expectancy_country', 'tax_revenue_country_country', 'total_tax_rate_country', 'population_country', 'latitude_country', 'longitude_country', 'Density\n(P/Km2)', 'Agricultural Land( %)', 'Land Area(Km2)', 'Armed Forces size', 'Co2-Emissions', 'CPI', 'CPI Change (%)', 'Forested Area (%)', 'Gasoline Price', 'GDP', 'Gross primary education enrollment (%)', 'Gross tertiary education enrollment (%)', 'Minimum wage', 'Out of pocket health expenditure', 'Population', 'Population: Labor force participation (%)', 'Tax revenue (%)', 'Total tax rate', 'Unemployment rate', 'Urban_population', 'Birth Rate', 'Calling Code', 'Fertility Rate', 'Infant mortality', 'Life expectancy', 'Maternal mortality ratio', 'Physicians per thousand', 'Latitude', 'Longitude']
    
    for col in numerical_columns:
        if df[col].dtype == 'object':
            # Replace '%' and ',' then convert to float
            df[col] = df[col].str.replace('%', '').str.replace(',', '').str.replace('$','').str.replace(' ','').astype(float)
        # Perform mean imputation for all numeric columns
        df[col] = df[col].fillna(df[col].mean())
    
    # Handle categorical columns
    
    categorical_columns = ['category', 'city', 'country', 'state', 'gender', 'status', 'residenceStateRegion', 'source', 'industries','Currency Unit', 'countryOfCitizenship', 'Country Code', 'Short Name', 'Table Name', 'Long Name', '2-alpha code', 'Region', 'Income Group', 'WB-2 code', 'National accounts base year', 'SNA price valuation', 'Lending category', 'System of National Accounts', 'PPP survey year', 'External debt Reporting status', 'System of trade', 'Government Accounting concept', 'IMF data dissemination standard', 'Latest household survey', 'Source of most recent Income and expenditure data', 'Balance of Payments Manual in use','Country', 'Abbreviation', 'Capital/Major City', 'Currency-Code', 'Largest city', 'Official language']
    for col in categorical_columns:
        df[col] = df[col].astype('category').cat.add_categories(['Unknown'])
        df[col] = df[col].fillna('Unknown')
    
    # Fill nan for remaining columns
    df['firstName'] = df['firstName'].fillna('Unknown')
    df['Special Notes'] = df['Special Notes'].fillna('None')
    
    return df