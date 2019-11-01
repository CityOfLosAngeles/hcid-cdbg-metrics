# Purpose: Import income-related Census data
# Date Created: 9/20/19
# Output: csvs in data/Census

library(tidycensus)
library(tidyverse)
library(censusapi)
Sys.getenv("CENSUS_API_KEY")
Sys.getenv("CENSUS_KEY")

# Set working directory
#setwd("GitHub/hcid-cdbg-metrics")

## Load years
tract_years <- list(2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)


#------------------------------------------------------------------#
## Median Income by Tract -- got all years
#------------------------------------------------------------------#. 
# Use C01 and C02 for 2010-2016; C01 and C03 for 2017. 2017 added new column that shows % of hh (derived from C01).
print('Download median income (S1903) 2010-16')
income_list = list()

for (y in 2010:2016) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S1903"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "002$") |
                         str_detect(variable, "003$") |
                         str_detect(variable, "004$") |
                         str_detect(variable, "005$") |
                         str_detect(variable, "006$") |
                         str_detect(variable, "007$") |
                         str_detect(variable, "008$") |
                         str_detect(variable, "009$") |
                         str_detect(variable, "010$")) &
                        str_detect(GEOID, "^06037") &
                        (str_detect(variable, "_C01") | str_detect(variable, "_C02"))
  )
  
  la$year <- y
  income_list[[y]] <- la
  
}


print('Download median income (S1903) 2017')
for (y in 2017) 
  {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S1903"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "002$") |
                         str_detect(variable, "003$") |
                         str_detect(variable, "004$") |
                         str_detect(variable, "005$") |
                         str_detect(variable, "006$") |
                         str_detect(variable, "007$") |
                         str_detect(variable, "008$") |
                         str_detect(variable, "009$") |
                         str_detect(variable, "010$")) &
                        str_detect(GEOID, "^06037") &
                        (str_detect(variable, "_C01") | str_detect(variable, "_C03"))
  )
  
  la$year <- y
  income_list[[y]] <- la
  
}


# Append dfs and export
print('Append median income dfs')
income = do.call(rbind, income_list)

write_csv(income, "data/Census/income_tract.csv")
print('Saved data/Census/income_tract.csv')


#------------------------------------------------------------------#
## Household Income Ranges by Tract -- got all years
#------------------------------------------------------------------#
print('Download household income ranges (B19001) 2010-17')
inc_list = list()


for (y in tract_years) {
  var <- load_variables(y, "acs5", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "B19001"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "002$") |
                         str_detect(variable, "003$") |
                         str_detect(variable, "004$") |
                         str_detect(variable, "005$") |
                         str_detect(variable, "006$") |
                         str_detect(variable, "007$") |
                         str_detect(variable, "008$") |
                         str_detect(variable, "009$") |
                         str_detect(variable, "010$") |
                         str_detect(variable, "011$") |
                         str_detect(variable, "012$") |
                         str_detect(variable, "013$") |
                         str_detect(variable, "014$") |
                         str_detect(variable, "015$") |
                         str_detect(variable, "016$") |
                         str_detect(variable, "017$")) &
                        str_detect(GEOID, "^06037")
  )
  
  la$year <- y
  inc_list[[y]] <- la
  
}


# Append all the years into a df
print('Append household income ranges')
inc = do.call(rbind, inc_list)


write_csv(inc, "data/Census/income_range_tract.csv")
print('Saved data/Census/income_range_tract.csv')


#------------------------------------------------------------------#
## Income Ranges by Tract and Household Type -- got all years
#------------------------------------------------------------------#
print('Download income ranges by household type (S1901) 2010-17')
inc2_list = list()
 
for (y in tract_years) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S1901"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "002$") |
                         str_detect(variable, "003$") |
                         str_detect(variable, "004$") |
                         str_detect(variable, "005$") |
                         str_detect(variable, "006$") |
                         str_detect(variable, "007$") |
                         str_detect(variable, "008$") |
                         str_detect(variable, "009$") |
                         str_detect(variable, "010$") |
                         str_detect(variable, "011$") |
                         str_detect(variable, "012$") |
                         str_detect(variable, "013$")) &
                        str_detect(GEOID, "^06037")
  )
  
  la$year <- y
  inc2_list[[y]] <- la
  
}


# Append all the years into a df
print('Append income ranges df')
inc2 = do.call(rbind, inc2_list)

write_csv(inc2, "data/Census/income_range_hh_tract.csv")
print('Saved data/Census/income_range_hh_tract.csv')
