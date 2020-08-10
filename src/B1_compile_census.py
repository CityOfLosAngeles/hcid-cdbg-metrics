# Compile all the downloaded tables from Census into 1 long df
import pandas as pd

S3_FILE_PATH = "s3://hcid-cdbg-project-ita-data/data/raw/"

# Initial cleaning for tidycensus dfs
pop = pd.read_csv(f'{S3_FILE_PATH}population_block_group.csv', dtype = {'GEOID': str})
medincome = pd.read_csv(f'{S3_FILE_PATH}medianincome_block_group.csv', dtype = {'GEOID': str})
income_range = pd.read_csv(f'{S3_FILE_PATH}income_range_hh_block_group.csv', dtype = {'GEOID': str})

# Save all the dfs into a dictionary
tidycensus = {'pop': pop, 'medincome': medincome, 'income_range': income_range}

for key, df in tidycensus.items():
    long_df = df.melt(id_vars = ['GEOID', 'NAME', 'year', 'variable'], 
                var_name = ['type'], value_vars = ['estimate', 'moe'])
    long_df['type'].replace({'estimate': 'E', 'moe': 'M'}, inplace = True)
    long_df['variable'] = long_df['variable'] + long_df['type']
    long_df.drop(columns = ['NAME', 'type'], inplace = True)
    tidycensus[key] = long_df


# Write all the tidycensus dfs into 1 df
final = pd.DataFrame()

for key, df in tidycensus.items():
    key = pd.DataFrame(df)
    final = final.append(key)


# Append dfs together and export as parquet
final.reset_index(drop=True).to_parquet(f'{S3_FILE_PATH}raw_census.parquet')