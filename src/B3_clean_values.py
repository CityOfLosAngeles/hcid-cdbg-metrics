# Clean the Census table values to be consistent across years

"""
ACS might report values as percents or numbers. 
Ex: families in poverty might be reported as a % of the total or raw numbers of families in poverty.

For consistency across years, create 2 columns: number and percent.
Need the number column later on, when tracts are aggregated up to larger geographies.
Also, treat dollar values as number values (but do not fill in percent column).
ACS always reports in the current year's inflation adjusted dollars.
"""

import numpy as np
import pandas as pd
import intake
import os
from tqdm import tqdm 
tqdm.pandas() 
from datetime import datetime

time0 = datetime.now()
print(f'Start time: {time0}')

catalog = intake.open_catalog('./catalogs/*.yml')

df = pd.read_parquet('s3://hcid-cdbg-project-ita-data/data/raw/raw_census_long.parquet')
print('Read in S3')

# Create empty dictionary to store results of cleaning
final_dfs = {}

#-----------------------------------------------------------------#
# Employment
#-----------------------------------------------------------------#
print('Clean employment table')
emp = df[df.table=='emp']

# Create column called var_type
def emp_type(row):
    if (row.main_var=='pop'):
        return 'number'
    elif (row.main_var != 'pop'):
        return 'percent'
    
emp['var_type'] = emp.progress_apply(emp_type, axis = 1)


# For each table, do a merge with pop values, convert the percents back to numbers and save into dictionary
emp_pop = emp[emp.main_var=='pop']

emp_dfs = {}

for subset in ['lf', 'epr', 'unemp']:
    new_pct_col = "pct"
    new_num_col = "num"
    subset_df = emp[emp.main_var == subset]
    # Merge with pop values
    merged = pd.merge(emp_pop, subset_df, on = ['GEOID', 'year', 'table', 'second_var'])
    merged = merged.drop(columns = ['variable_x', 'main_var_x', 'new_var_x'])
    merged.rename(columns = {'value_x': 'num_pop', 'value_y': new_pct_col}, inplace = True)
    # Create new percent and number columns
    merged[new_pct_col] = merged[new_pct_col] / 100
    merged[new_num_col] = merged.num_pop * merged[new_pct_col]
    # Clean up df and save
    merged = merged[['GEOID', 'year', 'table', 'second_var', 'variable_y', 'main_var_y', 'new_var_y', new_pct_col, new_num_col]]
    merged.rename(columns = {'variable_y': 'variable', 'main_var_y': 'main_var', 'new_var_y': 'new_var'}, inplace = True)
    emp_dfs[subset] = merged


# Generate percent and number columns
emp_pop['num'] = emp_pop.value
emp_pop['pct'] = 1


# Append the emp_dfs together into a long df
appended = emp_pop

for key, value in emp_dfs.items():
    appended = appended.append(value, sort = False)


# Save result into dictionary
final_dfs.update({'emp': appended})


#-----------------------------------------------------------------#
## Income
#-----------------------------------------------------------------#
print('Clean income table')
income = df[df.table=='income']

# Create column called var_type
def income_type(row):
    if (row.main_var=='hh') & (row.second_var=='total'):
        return 'number'
    elif (row.main_var == 'medincome'):
        return 'dollar'
    elif (row.main_var=='hh') & (row.second_var != 'hh') & (row.year <= 2016):
        return 'percent'
    elif (row.main_var=='hh') & (row.second_var != 'hh') & (row.year >= 2017):
        return 'number'

income['var_type'] = income.progress_apply(income_type, axis = 1)


# Create a denominator column. Use this to convert percent values into numbers.
income['denom'] = income.progress_apply(lambda row: row.value if row.new_var=='hh_total' else np.nan, axis = 1)


#################################################
# General functions to use on multiple tables
#################################################
# Fill in missing values of the denominator with the max (for that group)
def general_fill_denom(df):
    df['denom'] = df.denom.fillna(df.groupby(['GEOID', 'year'])['denom'].transform('max'))


# Replace values that were percents with numbers
def general_pct_col(row):
    if row.var_type == 'percent':
        return (row.value / 100)
    elif (row.var_type == 'number') & (row.denom > 0) :
        return (row.value / row.denom)
    elif (row.var_type == 'number') & (row.denom == 0):
        return np.nan
    elif row.var_type == 'dollar':
        return np.nan
    

# Create percent and number columns
def general_create_cols(df):
    df['pct'] = df.progress_apply(general_pct_col, axis = 1)
    df['num'] = df.progress_apply(lambda row: row.value if row.var_type in ['number', 'dollar'] else (row.pct * row.denom), axis = 1)


# Apply functions to create percent and number columns
print('Create pct and num columns')
general_fill_denom(income)
general_create_cols(income)


# Add df to dictionary
final_dfs.update({'income': income})


#-----------------------------------------------------------------#
## Income Range
#-----------------------------------------------------------------#
print('Clean income range table')
incomerange = df[(df.table=='incomerange')]

incomerange['var_type'] = 'number'

# Create a denominator column. Use this to convert percent values into numbers.
incomerange['denom'] = incomerange.progress_apply(lambda row: row.value if 
                        row.second_var=='total' else np.nan, axis = 1)


print('Create pct and num columns')
general_fill_denom(incomerange)
general_create_cols(incomerange)


# Add df to dictionary
final_dfs.update({'incomerange': incomerange})


#-----------------------------------------------------------------#
## Income Range by Household Type
#-----------------------------------------------------------------#
print('Clean income range by household type table')
incomerange_hh = df[(df.table=='incomerange_hh')]

# Create column called var_type
def incomerange_hh_type(row):
    if (row.second_var=='medinc') or (row.second_var=='meaninc'):
        return 'dollar'
    else:
        return 'number'

incomerange_hh['var_type'] = incomerange_hh.progress_apply(incomerange_hh_type, axis = 1)


# Create a denominator column. Use this to convert percent values into numbers.
incomerange_hh['denom'] = incomerange_hh.progress_apply(lambda row: row.value if 
                        row.second_var=='total' else np.nan, axis = 1)


print('Create pct and num columns')
general_fill_denom(incomerange_hh)
general_create_cols(incomerange_hh)


# Add df to dictionary
final_dfs.update({'incomerange_hh': incomerange_hh})


#-----------------------------------------------------------------#
## Population and Housing Units
#-----------------------------------------------------------------#
print('Clean population and housing tables')
pop_housing = df[(df.table=='pop') | (df.table=='housing') ]

pop_housing['var_type'] = 'number'

# Create a denominator column. Use this to convert percent values into numbers.
pop_housing['denom'] = pop_housing.progress_apply(lambda row: row.value if 
                        row.new_var in ['pop', 'housing'] else np.nan, axis = 1)


print('Create pct and num columns')
general_fill_denom(pop_housing)
general_create_cols(pop_housing)


# Add df to dictionary
final_dfs.update({'pop_housing': pop_housing})


#-----------------------------------------------------------------#
## Educational Attainment
#-----------------------------------------------------------------#
print('Clean education table')
edu = df[df.table=='edu']

# Create column called var_type
def edu_type(row):
    if (row.second_var.find('pct') != -1) or (row.second_var.find('pov') != -1):
        return 'percent'
    elif row.second_var.find('medearning') != -1:
        return 'dollar'
    elif (row.second_var.find('pct') == -1) & (row.second_var.find('medearning') == -1):
        return 'number'

edu['var_type'] = edu.progress_apply(edu_type, axis = 1)


# Create a denominator column. Use this to convert percent values into numbers.
edu['denom'] = edu.progress_apply(lambda row: row.value if row.new_var=='pop_total_pop25' else np.nan, axis = 1)


# Replace values that were percents with numbers
print('Create pct and num columns')
general_fill_denom(edu)
general_create_cols(edu)


# Add df to dictionary
final_dfs.update({'edu': edu})


#-----------------------------------------------------------------#
## Poverty for Families
#-----------------------------------------------------------------#
print('Clean poverty of families table')
povfam = df[df.table=='povfam']

# Create column called var_type
def povfam_type(row):
    if row.main_var.find('pov') != -1:
        return 'percent'
    else:
        return 'number'
    
povfam['var_type'] = povfam.progress_apply(povfam_type, axis = 1)


# Create a denominator column. Use this to convert percent values into numbers.
povfam['denom'] = povfam.progress_apply(lambda row: row.value if row.new_var=='fam' else np.nan, axis = 1)


print('Create pct and num columns')
general_fill_denom(povfam)
general_create_cols(povfam)


# Add df to dictionary
final_dfs.update({'povfam': povfam})


#-----------------------------------------------------------------#
## Poverty for Families by Household Type
#-----------------------------------------------------------------#
print('Clean poverty of families by household type table')
povfam_hh = df[df.table=='povfam_hh']

# Create column called var_type
povfam_hh['var_type'] = 'number'

# Create a denominator column. Use this to convert percent values into numbers.
povfam_hh['denom'] = povfam_hh.progress_apply(lambda row: 
                    row.value if row.new_var == 'povfam_hh' else np.nan, axis = 1)


print('Create pct and num columns')
general_fill_denom(povfam_hh)
general_create_cols(povfam_hh)

# Add df to dictionary
final_dfs.update({'povfam_hh': povfam_hh})


#-----------------------------------------------------------------#
## Poverty by Race/Sex/Educational Attainment
#-----------------------------------------------------------------#
print('Clean poverty by race/sex/educational attainment table')
pov= df[df.table=='pov']

# Do a merge with total pop values, calculate percent, and convert the percents back to numbers and save into dictionary
# Modify how the employment table was processed and adapt to poverty table
pov_total = pov[pov.main_var=='total']

pov_dfs = {}

for subset in ['pov']:
    new_pct_col = "pct"
    new_num_col = "num"
    subset_df = pov[pov.main_var == subset]
    # Merge with pop values
    merged = pd.merge(pov_total, subset_df, on = ['GEOID', 'year', 'table', 'second_var'])
    merged = merged.drop(columns = ['variable_x', 'main_var_x', 'new_var_x'])
    merged.rename(columns = {'value_x': 'num_pop', 'value_y': new_pct_col}, inplace = True)
    # Create new percent and number columns
    merged[new_pct_col] = merged[new_pct_col] / merged.num_pop
    merged[new_num_col] = merged.num_pop * merged[new_pct_col]
    # Clean up df and save
    merged = merged[['GEOID', 'year', 'table', 'second_var', 'variable_y', 'main_var_y', 'new_var_y', new_pct_col, new_num_col]]
    merged.rename(columns = {'variable_y': 'variable', 'main_var_y': 'main_var', 'new_var_y': 'new_var'}, inplace = True)
    pov_dfs[subset] = merged

# Generate percent and number columns
pov_total['num'] = pov_total.value
pov_total['pct'] = 1


# Append the pov_dfs together into a long df
appended = pov_total

for key, value in pov_dfs.items():
    appended = appended.append(value, sort = False)


# Add df to dictionary
final_dfs.update({'pov': pov})


#-----------------------------------------------------------------#
## Food Stamps
#-----------------------------------------------------------------#
print('Clean food stamps table')
food = df[df.table=='food']

def food_type(row):
    if row.second_var == 'total':
        return 'number'
    elif row.second_var.find('income') != -1:
        return 'dollar'
    elif (row.second_var == 'pov') & (row.year <= 2014):
        return 'percent'
    elif (row.second_var == 'pov') & (row.year >= 2015):
        return 'number'
    
food['var_type'] = food.progress_apply(food_type, axis = 1)


# Make sure denominator includes main_var. Double checked with American Fact Finder that values and percents match reported.
food['denom'] = food.progress_apply(lambda row: row.value if row.second_var=='total' else np.nan, axis = 1)
food['denom'] = food['denom'].fillna(food.groupby(['GEOID', 'year', 'main_var'])['denom'].transform('max'))


print('Create pct and num columns')
general_create_cols(food)


# Add df to dictionary
final_dfs.update({'food': food})


#-----------------------------------------------------------------#
## Health Coverage
#-----------------------------------------------------------------#
print('Clean health coverage table')
health = df[df.table=='health']

# Create column called var_type
health['var_type'] = 'number'

# Create a denominator column. Use this to convert percent values into numbers.
health['denom'] = health.progress_apply(lambda row: 
                    row.value if row.new_var == 'healthcoverage_total' else np.nan, axis = 1)


print('Create pct and num columns')
general_fill_denom(health)
general_create_cols(health)

# Add df to dictionary
final_dfs.update({'health': health})


#-----------------------------------------------------------------#
## Public Assistance
#-----------------------------------------------------------------#
print('Clean public assistance table')
pubassist = df[df.table=='pubassist']

# Create column called var_type
pubassist['var_type'] = 'number'

# Create a denominator column. Use this to convert percent values into numbers.
pubassist['denom'] = pubassist.progress_apply(lambda row: 
                    row.value if row.main_var == 'hh' else np.nan, axis = 1)


print('Create pct and num columns')
general_fill_denom(pubassist)
general_create_cols(pubassist)


# Add df to dictionary
final_dfs.update({'pubassist': pubassist})


#-----------------------------------------------------------------#
## Aggregate Public Assistance
#-----------------------------------------------------------------#
print('Clean public assistance table')
aggpubassist = df[df.table=='aggpubassist']

# Create column called var_type
aggpubassist['var_type'] = 'dollar'

# Clean up values that are invalid (coded as -666666666)
aggpubassist['num'] = aggpubassist.progress_apply(lambda row: row.value if row.value > -10000000 
                                                  else np.nan, axis = 1)

aggpubassist['pct'] = np.nan


# Add df to dictionary
final_dfs.update({'aggpubassist': aggpubassist})


#-----------------------------------------------------------------#
## Append dfs together and export
#-----------------------------------------------------------------#
final = pd.DataFrame()

for key, value in final_dfs.items():
    final = value.sort_values(['table', 'GEOID', 'year', 'main_var']).append(value)
    

# Round the number column (can't convert to integer because some are NaN)
final['num'] = final.num.round(0)


# Change column order
cols = ['GEOID', 'year', 'variable', 'table', 'main_var', 'second_var', 'new_var', 'num', 'pct']
final = final[cols].reindex(columns = cols)


# Export as parquet
print('Export results')
if os.environ.get('DEV') is None:
    final.to_parquet('./data/raw_census_cleaned.parquet')
    final.to_parquet('s3://hcid-cdbg-project-ita-data/data/raw/raw_census_cleaned.parquet')


time1 = datetime.now()
print(f'Total execution time: {time1 - time0}')