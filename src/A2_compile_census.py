# Compile all the downloaded tables from Census into 1 long df

import numpy as np
import pandas as pd
import intake
import os

# Initial cleaning for censusapi dfs
pop = pd.read_csv('s3://hcid-cdbg-project-ita-data/data/raw/population_tract.csv')
housing = pd.read_csv('s3://hcid-cdbg-project-ita-data/data/raw/housing_units_tract.csv')
pub = pd.read_csv('s3://hcid-cdbg-project-ita-data/data/raw/public_assistance_tract.csv') 
agg_pub = pd.read_csv('s3://hcid-cdbg-project-ita-data/data/raw/aggregate_public_assistance_tract.csv') 


# Save all the dfs into a dictionary and clean up GEOID
censusapi = {'pop': pop, 'housing': housing, 'pub': pub, 'agg_pub': agg_pub}


for key, df in censusapi.items():
    # Convert integers to strings
    for col in ['state', 'county', 'tract']:
        df[col] = df[col].astype(str)
    # Create GEOID, must come out to 11 characters
    df.state = df.state.str.zfill(width = 2)
    df.county = df.county.str.zfill(width = 3)
    df['GEOID'] = df.state + df.county + df.tract
    df.drop(columns = ['state', 'county', 'tract', 'NAME'], inplace = True)
    long_df = df.melt(id_vars = ['GEOID', 'year'], var_name = 'variable')
    censusapi[key] = long_df


# Write all the censusapi dfs into 1 df
long = pd.DataFrame()

for key, df in censusapi.items():
    key = pd.DataFrame(df)
    long = long.append(key)


# Initial cleaning for tidycensus dfs
emp = pd.read_csv('s3://hcid-cdbg-project-ita-data/data/raw/employment_tract.csv')
income = pd.read_csv('s3://hcid-cdbg-project-ita-data/data/raw/income_tract.csv')
income_range = pd.read_csv('s3://hcid-cdbg-project-ita-data/data/raw/income_range_tract.csv')
edu = pd.read_csv('s3://hcid-cdbg-project-ita-data/data/raw/educational_attainment_tract.csv') 
pov_fam = pd.read_csv('s3://hcid-cdbg-project-ita-data/data/raw/poverty_families_tract.csv') 
food = pd.read_csv('s3://hcid-cdbg-project-ita-data/data/raw/food_stamps_tract.csv') 


# Save all the dfs into a dictionary
tidycensus = {'emp': emp, 'income': income, 'income_range': income_range, 
            'edu': edu, 'pov_fam': pov_fam, 'food': food}

for key, df in tidycensus.items():
    df.GEOID = df.GEOID.astype(str)
    df.GEOID = df.GEOID.str.zfill(11)
    long_df = df.melt(id_vars = ['GEOID', 'NAME', 'year', 'variable'], var_name = ['type'], value_vars = ['estimate', 'moe'])
    long_df['type'].replace({'estimate': 'E', 'moe': 'M'}, inplace = True)
    long_df['variable'] = long_df['variable'] + long_df['type']
    long_df.drop(columns = ['NAME', 'type'], inplace = True)
    tidycensus[key] = long_df


# Write all the tidycensus dfs into 1 df
long2 = pd.DataFrame()

for key, df in tidycensus.items():
    key = pd.DataFrame(df)
    long2 = long2.append(key)


# Append dfs together and export as parquet
all = long.append(long2)
all.to_parquet('s3://hcid-cdbg-project-ita-data/data/raw/raw_census.parquet')