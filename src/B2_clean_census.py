# Initial cleaning of raw Census data (long), preparing for reshaping
# Census GEOIDs: https://www.census.gov/programs-surveys/geography/guidance/geo-identifiers.html

import numpy as np
import pandas as pd
import intake
import os
import re
from tqdm import tqdm 
tqdm.pandas() 
from datetime import datetime

time0 = datetime.now()
print(f'Start time: {time0}')

catalog = intake.open_catalog('./catalogs/*.yml')

df = pd.read_parquet('s3://hcid-cdbg-project-ita-data/data/raw/raw_census.parquet')
print('Read in S3')

"""
ACS variable names come with table, main variable, and secondary variable. 
If there are multiple columns, then the main variable changes, but the secondary variable is constant. 
Ex: a table about employment by race would have:
main variables: population, labor force participation rate, etc
secondary variables: white, black, asian, etc
"""

#-----------------------------------------------------------------#
# Tag the ACS table (about 13 min)
#-----------------------------------------------------------------#
acs_tables = {'B01003': 'pop', 'B25001': 'housing', 
             'S2301': 'emp', 
             'S1903': 'income', 'B19001': 'incomerange', 'S1901': 'incomerange_hh',
             'S1701': 'pov', 'S1702': 'povfam', 'B17014':'povfam_hh',
             'S1501': 'edu', 
             'S2201': 'food', 'B19058': 'pubassist', 'B19067': 'aggpubassist', 
             'DP03': 'health'}


# Identify where to extract the ACS table name (tag the portion up to the 1st underscore)
pattern = re.compile('([A-Za-z0-9]+)_')

df['table'] = df.progress_apply(
    lambda row: acs_tables.get(pattern.match(row.variable).group(1)),
    axis = 1
)

# Find the other B19001A, B19001B, etc tables and tag them
df['table'] = df.progress_apply(
    lambda row: 'incomerange' if 'B19001' in row.variable else row.table,
    axis = 1
)

time1 = datetime.now()
print(f'Tagged ACS table: {time1 - time0}')


#-----------------------------------------------------------------#
# Tag the main variable (about 15 min)
#-----------------------------------------------------------------#
def pop_vars(row): 
    if '_001' in row.variable:
        return 'pop'

def housing_vars(row): 
    if '_001' in row.variable:
        return 'housing'

def emp_vars(row):
    if '_C01' in row.variable:
        return 'pop'
    elif '_C02' in row.variable:
        return 'lf'
    elif '_C03' in row.variable:
        return 'epr'
    elif '_C04' in row.variable:
        return 'unemp'

# Columns shift from C02 to C03 2017-onward, but it's the same variable
def income_vars(row):
    if '_C01' in row.variable:
        return 'hh'
    elif '_C02' in row.variable:
        return 'medincome'
    elif '_C03' in row.variable:
        return 'medincome'

def incomerange_vars(row):
    if 'B19001_' in row.variable:
        return 'total'
    elif 'B19001A' in row.variable:
        return 'white'
    elif 'B19001B' in row.variable:
        return 'black'
    elif 'B19001C' in row.variable:
        return 'amerind'
    elif 'B19001D' in row.variable:
        return 'asian'
    elif 'B19001E' in row.variable:
        return 'pacis'
    elif 'B19001F' in row.variable:
        return 'other'
    elif 'B19001G' in row.variable:
        return 'race2'
    elif 'B19001H' in row.variable:
        return 'nonhisp'
    elif 'B19001I' in row.variable:
        return 'hisp'

def incomerange_hh_vars(row):
    if '_C01' in row.variable:
        return 'hh'
    elif '_C02' in row.variable:
        return 'families'
    elif '_C03' in row.variable:
        return 'married'
    elif '_C04' in row.variable:
        return 'nonfamily'

# There are 2 variables that appear as C02 from 2015-2017, but appeared as C01 from 2010-2014. Clean later, use the same main_var for now.
def edu_vars(row):
    if '_C01' in row.variable:
        return 'pop'
    elif '_C02' in row.variable:
        return 'pop'

def pov_vars(row):
    if '_C01' in row.variable:
        return 'total'
    elif '_C02' in row.variable:
        return 'pov'

def povfam_vars(row):
    if '_C01' in row.variable:
        return 'fam'
    elif '_C02' in row.variable:
        return 'fam_pov'

# Doesn't have a main/secondary variable breakdown, but, still use the last 2 characters to tag secondary variable
def povfam_hh_vars(row):
    return 'povfam'

# Columns shift from C02 to C03 2015-onward, but it's the same variable
def food_vars(row):
    if '_C01' in row.variable:
        return 'hh'
    elif '_C02' in row.variable:
        return 'hh_food'
    elif '_C03' in row.variable:
        return 'hh_food'

def pubassist_vars(row):
    if '_001' in row.variable:
        return 'hh'
    elif '_002' in row.variable:
        return 'hh_pubassist'

def aggpubassist_vars(row):
    if '_001' in row.variable:
        return 'aggincome'

# Doesn't have a main/secondary variable breakdown, but, still use the last 2 characters to tag secondary variable
def health_vars(row):
    return 'healthcoverage'


main_vars_dict = {
    'pop': pop_vars,
    'housing': housing_vars,
    'emp': emp_vars,
    'income': income_vars,
    'incomerange': incomerange_vars,
    'incomerange_hh': incomerange_hh_vars,
    'edu': edu_vars,
    'pov': pov_vars,
    'povfam': povfam_vars,
    'povfam_hh': povfam_hh_vars,
    'food': food_vars,
    'pubassist': pubassist_vars,
    'aggpubassist': aggpubassist_vars,
    'health': health_vars
}


# Do mapping on subset before merging on full df
subset_df1 = df[['table', 'year', 'variable']].drop_duplicates()
subset_df1['main_var'] = subset_df1.progress_apply(lambda row: main_vars_dict[row['table']](row), axis = 1)

# Merge the mapped values with the full df
df = pd.merge(df, subset_df1, on = ['table', 'year', 'variable'], how = 'left', validate = 'm:1')


time2 = datetime.now()
print(f'Tagged main variable time: {time2 - time1}')


#-----------------------------------------------------------------#
# Tag the secondary variable (about 5 min?)
#-----------------------------------------------------------------#
# Grab the last 2 characters of the variable column that tells us what number the question was (01, 02, ...)
df['last2'] = df.variable.str[-3:-1]


emp2010 = {'01': 'total_pop16', '10': 'white', '11': 'black', '12': 'amerind', '13': 'asian', '14': 'pacis', '15': 'other', 
           '16': 'race2', '17': 'hisp', '18': 'nonhisp', 
           '19': 'total_pop20', '20': 'male', '21': 'female', '22': 'femalewchild6', 
           '23': 'total_pov', '25': 'total_pop25', '26': 'lhs', '27': 'hs', '28': 'college', '29': 'ba'}

emp2015 = {'01': 'total_pop16', '12': 'white', '13': 'black', '14': 'amerind', '15': 'asian', '16': 'pacis', '17': 'other', 
           '18': 'race2', '19': 'hisp', '20': 'nonhisp', 
           '21': 'total_pop20', '22': 'male', '23': 'female', '25': 'femalewchild6', 
           '28': 'total_pov', '31': 'total_pop25', '32': 'lhs', '33': 'hs', '34': 'college', '35': 'ba'}


income = {'01': 'total', '02': 'white', '03': 'black', '04': 'amerind', '05': 'asian',
           '06': 'pacis', '07': 'other', '08': 'race2', '09': 'hisp', '10': 'nonhisp'}

incomerange = {'01': 'total', '02': 'lt10', '03': 'r10to14', '04': 'r15to19', '05': 'r20to24',
           '06': 'r25to29', '07': 'r30to34', '08': 'r35to39', '09': 'r40to44', '10': 'r45to49',
           '11': 'r50to59', '12': 'r60to74', '13': 'r75to99', '14': 'r100to124', '15': 'r125to149',
           '16': 'r150to199', '17': 'gt200'}

incomerange_hh = {'01': 'total', '02': 'lt10', '03': 'r10to14', '04': 'r15to24', '05': 'r25to34',
           '06': 'r35to49', '07': 'r50to74', '08': 'r75to99', '09': 'r100to149', '10': 'r150to199',
           '11': 'gt200', '12': 'medinc', '13': 'meaninc'}


edu2010 = {'06': 'total_pop25', '07': 'hs9', '08': 'hs12', '09': 'hs', '10': 'college',
           '11': 'aa', '12': 'ba', '13': 'ma', '14': 'pct_hsplus', '15': 'pct_baplus', 
           '28': 'pov_lhs', '29': 'pov_hs', '30': 'college_pov', '31': 'ba_pov', '32': 'pop25_medearning',
           '33': 'lhs_medearning', '34': 'hs_medearning', '35': 'college_medearning', '36': 'ba_medearning', '37': 'ma_medearning'}

edu2015 = {'06': 'total_pop25', '07': 'hs9', '08': 'hs12', '09': 'hs', '10': 'college',
           '11': 'aa', '12': 'ba', '13': 'ma', '14': 'pct_hsplus', '15': 'pct_baplus', 
           '55': 'pov_lhs', '56': 'pov_hs', '57': 'college_pov', '58': 'ba_pov', '59': 'pop25_medearning',
           '60': 'lhs_medearning', '61': 'hs_medearning', '62': 'college_medearning', '63': 'ba_medearning', '64': 'ma_medearning'}

pov2012 = {'01': 'total', '06': 'male', '07': 'female', '09': 'white', '10': 'black',
           '11': 'amerind', '12': 'asian', '13': 'pacis', '14': 'other', '15': 'race2', 
           '16': 'hisp', '17': 'nonhisp', '18': 'pop25', '19': 'lhs', '20': 'hs', 
           '21': 'college', '22': 'ba'}

pov2015 = {'01': 'total', '11': 'male', '12': 'female', '13': 'white', '14': 'black',
           '15': 'amerind', '16': 'asian', '17': 'pacis', '18': 'other', '19': 'race2', 
           '20': 'hisp', '21': 'nonhisp', '22': 'pop25', '23': 'lhs', '24': 'hs', 
           '25': 'college', '26': 'ba'}

povfam_hh = {'01': 'hh', '02': 'hh_pov', '03': 'married', '09': 'malehh', '14': 'femalehh'}


food2010 = {'01': 'total', '04': 'pov', '16': 'medhhincome'}

food2015 = {'01': 'total', '21': 'pov', '34': 'medhhincome'}


health = {'95': 'total', '96': 'have_hlt', '97': 'private', '98': 'public', '99': 'no_hlt'} 


def pick_secondary_var(row):
    if (row.table=='emp') & (row.year <= 2014):
        return emp2010[row.last2]
    elif (row.table=='emp') & (row.year >= 2015):
        return emp2015[row.last2]
    elif row.table=='income':
        return income[row.last2]
    elif row.table=='incomerange':
        return incomerange[row.last2]
    elif row.table=='incomerange_hh':
        return incomerange_hh[row.last2]        
    elif (row.table=='edu') & (row.year <= 2014):
        return edu2010[row.last2]
    elif (row.table=='edu') & (row.year >= 2015):
        return edu2015[row.last2]
    elif (row.table=='pov') & (row.year <= 2014):
        return pov2012[row.last2]
    elif (row.table=='pov') & (row.year >= 2015):
        return pov2015[row.last2]
    elif row.table=='povfam_hh':
        return povfam_hh[row.last2]
    elif (row.table=='food') & (row.year <= 2014):
        return food2010[row.last2]
    elif (row.table=='food') & (row.year >= 2015):
        return food2015[row.last2]
    elif row.table=='health':
        return health[row.last2]


# Do mapping on subset before merging on full df
subset_df2 = df[['table', 'year', 'last2']].drop_duplicates()
subset_df2['second_var'] = subset_df2.progress_apply(pick_secondary_var, axis = 1)

# Merge the mapped values with the full df
df = pd.merge(df, subset_df2, on = ['table', 'year', 'last2'], how = 'left', validate = 'm:1')


time3 = datetime.now()
print(f'Tagged secondary variable time: {time3 - time2}')


#-----------------------------------------------------------------#
# Tag estimate or margin of error (about 5 min)
#-----------------------------------------------------------------#
# Generate column that identifies whether it's estimate or margin of error (might drop margin of error later)
df['est_moe'] = df.progress_apply(lambda row: 'est' if row.variable[-1:]=='E' else 'moe', axis = 1)


# Drop rows not needed
df = df.loc[df.est_moe != 'moe']

# Drop columns not needed
df.drop(columns = ['last2', 'est_moe'], inplace = True)


time4 = datetime.now()
print(f'Tagged est/moe variable time: {time4 - time3}')


#-----------------------------------------------------------------#
# Construct variable name that is more descriptive
#-----------------------------------------------------------------#
# Fill in "None" with empty string
for col in ['main_var', 'second_var']:
    df[col] = df[col].fillna(value = "") 


# Construct our new variable name
df['new_var'] = df.progress_apply(lambda row: row.main_var if row.second_var=="" 
                         else (row.main_var + '_' + row.second_var), axis = 1)


# Export as parquet
print('Export results')
if os.environ.get('DEV') is not None:
    df.to_parquet('./data/raw_census_long.parquet')
    df.to_parquet('s3://hcid-cdbg-project-ita-data/data/raw/raw_census_long.parquet')


time5 = datetime.now()
print(f'Clean up and export time: {time5 - time4}')
print(f'Total execution time: {time5 - time0}')