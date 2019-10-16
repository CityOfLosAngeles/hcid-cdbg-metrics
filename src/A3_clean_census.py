# Initial cleaning of raw Census data (long), preparing for reshaping
# Census GEOIDs: https://www.census.gov/programs-surveys/geography/guidance/geo-identifiers.html

import numpy as np
import pandas as pd
import intake
import os

catalog = intake.open_catalog('./catalogs/*.yml')

df = pd.read_parquet('s3://hcid-cdbg-project-ita-data/data/raw/raw_census.parquet')

"""
ACS variable names come with table, main variable, and secondary variable. 
If there are multiple columns, then the main variable changes, but the secondary variable is constant. 
Ex: a table about employment by race would have:
main variables: population, labor force participation rate, etc
secondary variables: white, black, asian, etc
"""

#-----------------------------------------------------------------#
# Tag the ACS table
#-----------------------------------------------------------------#
acs_tables = {'B01003': 'pop', 'B25001': 'housing', 
             'S1903': 'income', 'S2301': 'emp', 'S1501': 'edu',
             'S1702': 'povfam', 'S2201': 'food', 
             'B19058': 'pubassist', 'B19067': 'aggpubassist',
             'B19001': 'incomerange'}


# Identify where to extract the ACS table name
df['cut'] = df.variable.str.find('_')
df['table_name'] = df.apply(lambda row: row.variable[: row.cut], axis = 1)

# Map the table_name to the new label using a dictionary
df['table'] = df.table_name.map(acs_tables)


#-----------------------------------------------------------------#
# Tag the main variable
#-----------------------------------------------------------------#
def pop_vars(row): 
    if row.variable.find('_001') != -1:
        return 'pop'

def housing_vars(row): 
    if row.variable.find('_001') != -1:
        return 'housing'

def emp_vars(row):
    if row.variable.find('_C01') != -1:
        return 'pop'
    elif row.variable.find('_C02') != -1:
        return 'lf'
    elif row.variable.find('_C03') != -1:
        return 'epr'
    elif row.variable.find('_C04') != -1:
        return 'unemp'

# Columns shift from C02 to C03 2017-onward, but it's the same variable
def income_vars(row):
    if row.variable.find('_C01') != -1:
        return 'hh'
    elif row.variable.find('_C02') != -1:
        return 'medincome'
    elif row.variable.find('_C03') != -1:
        return 'medincome'

def incomerange_vars(row):
    if row.variable.find('B19001_') != -1:
        return 'total'
    elif row.variable.find('B19001A') != -1:
        return 'white'
    elif row.variable.find('B19001B') != -1:
        return 'black'
    elif row.variable.find('B19001C') != -1:
        return 'amerind'
    elif row.variable.find('B19001D') != -1:
        return 'asian'
    elif row.variable.find('B19001E') != -1:
        return 'pacis'
    elif row.variable.find('B19001F') != -1:
        return 'other'
    elif row.variable.find('B19001G') != -1:
        return 'race2'
    elif row.variable.find('B19001H') != -1:
        return 'nonhisp'
    elif row.variable.find('B19001I') != -1:
        return 'hisp'


# There are 2 variables that appear as C02 from 2015-2017, but appeared as C01 from 2010-2014. Clean later, use the same main_var for now.
def edu_vars(row):
    if row.variable.find('_C01') != -1:
        return 'pop'
    elif row.variable.find('_C02') != -1:
        return 'pop'

def povfam_vars(row):
    if row.variable.find('_C01') != -1:
        return 'fam'
    elif row.variable.find('_C02') != -1:
        return 'fam_pov'

# Columns shift from C02 to C03 2015-onward, but it's the same variable
def food_vars(row):
    if row.variable.find('_C01') != -1:
        return 'hh'
    elif row.variable.find('_C02') != -1:
        return 'hh_food'
    elif row.variable.find('_C03') != -1:
        return 'hh_food'

def pubassist_vars(row):
    if row.variable.find('_001') != -1:
        return 'hh'
    elif row.variable.find('_002') != -1:
        return 'hh_pubassist'

def aggpubassist_vars(row):
    if row.variable.find('_001') != -1:
        return 'aggincome'


def pick_table(row):
    if row.table=='pop':
        return pop_vars(row)
    elif row.table=='housing':
        return housing_vars(row)
    elif row.table=='emp':
        return emp_vars(row)
    elif row.table=='income':
        return income_vars(row)
    elif row.table=='incomerange':
        return incomerange_vars(row)
    elif row.table=='edu':
        return edu_vars(row)
    elif row.table=='povfam':
        return povfam_vars(row)
    elif row.table=='food':
        return food_vars(row)
    elif row.table=='pubassist':
        return pubassist_vars(row)
    elif row.table=='aggpubassist':
        return aggpubassist_vars(row)

df['main_var'] = df.apply(pick_table, axis = 1)


#-----------------------------------------------------------------#
# Tag the secondary variable
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


edu2010 = {'06': 'total_pop25', '07': 'hs9', '08': 'hs12', '09': 'hs', '10': 'college',
           '11': 'aa', '12': 'ba', '13': 'ma', '14': 'pct_hsplus', '15': 'pct_baplus', 
           '28': 'pov_lhs', '29': 'pov_hs', '30': 'college_pov', '31': 'ba_pov', '32': 'pop25_medearning',
           '33': 'lhs_medearning', '34': 'hs_medearning', '35': 'college_medearning', '36': 'ba_medearning', '37': 'ma_medearning'}

edu2015 = {'06': 'total_pop25', '07': 'hs9', '08': 'hs12', '09': 'hs', '10': 'college',
           '11': 'aa', '12': 'ba', '13': 'ma', '14': 'pct_hsplus', '15': 'pct_baplus', 
           '55': 'pov_lhs', '56': 'pov_hs', '57': 'college_pov', '58': 'ba_pov', '59': 'pop25_medearning',
           '60': 'lhs_medearning', '61': 'hs_medearning', '62': 'college_medearning', '63': 'ba_medearning', '64': 'ma_medearning'}


food2010 = {'01': 'total', '04': 'pov', '16': 'medhhincome'}

food2015 = {'01': 'total', '21': 'pov', '34': 'medhhincome'}


def pick_secondary_var(row):
    if row.table=='income':
        return income[row.last2]
    elif row.table=='incomerange':
        return incomerange[row.last2]
    elif (row.table=='emp') & (row.year <= 2014):
        return emp2010[row.last2]
    elif (row.table=='emp') & (row.year >= 2015):
        return emp2015[row.last2]
    elif (row.table=='edu') & (row.year <= 2014):
        return edu2010[row.last2]
    elif (row.table=='edu') & (row.year >= 2015):
        return edu2015[row.last2]
    elif (row.table=='food') & (row.year <= 2014):
        return food2010[row.last2]
    elif (row.table=='food') & (row.year >= 2015):
        return food2015[row.last2]
    
df['second_var'] = df.apply(pick_secondary_var, axis = 1)


#-----------------------------------------------------------------#
# Tag estimate or margin of error
#-----------------------------------------------------------------#
# Generate column that identifies whether it's estimate or margin of error (might drop margin of error later)
df['est_moe'] = df.apply(lambda row: 'est' if row.variable[-1:]=='E' else 'moe', axis = 1)


# Drop rows not needed
df = df.loc[df.est_moe != 'moe']

# Drop columns not needed
df.drop(columns = ['cut', 'table_name', 'last2', 'est_moe'], inplace = True)


#-----------------------------------------------------------------#
# Construct variable name that is more descriptive
#-----------------------------------------------------------------#
# Fill in "None" with empty string
for col in ['main_var', 'second_var']:
    df[col] = df[col].fillna(value = "") 


# Construct our new variable name
df['new_var'] = df.apply(lambda row: (row.main_var) if row.second_var=="" 
                         else (row.main_var + '_' + row.second_var), axis = 1)


# Export as parquet
df.to_parquet('./data/raw_census_long.parquet')
df.to_parquet('s3://hcid-cdbg-project-ita-data/data/raw/raw_census_long.parquet')