# Purpose: Import poverty-related Census data
# Date Created: 9/20/19
# Output: csvs in data/Census

library(tidycensus)
library(tidyverse)
library(censusapi)
Sys.getenv("CENSUS_API_KEY")
Sys.getenv("CENSUS_KEY")

# Set working directory
setwd("GitHub/hcid-cdbg-metrics")

## Load years
tract_years <- list(2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)


#------------------------------------------------------------------#
## Poverty Status by Tract -- does not have 2010-2011
#------------------------------------------------------------------#
## S1701 table doesn't have the same variables from 2012-2017. Don't clean for now.
pov_list = list()

# 2012-2014
print('Download poverty status (S1701) 2010-14')

for (y in 2012:2014) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S1701"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "006$") |
                         str_detect(variable, "007$") |
                         str_detect(variable, "009$") |
                         str_detect(variable, "010$") |
                         str_detect(variable, "011$") |
                         str_detect(variable, "012$") |
                         str_detect(variable, "013$") |
                         str_detect(variable, "014$") |
                         str_detect(variable, "015$") |
                         str_detect(variable, "016$") |
                         str_detect(variable, "017$") |
                         str_detect(variable, "018$") |
                         str_detect(variable, "019$") |
                         str_detect(variable, "020$") |
                         str_detect(variable, "021$") |
                         str_detect(variable, "022$")) &
                        str_detect(GEOID, "^06037") &
                        (str_detect(variable, "_C01") | str_detect(variable, "_C02"))
  )
  
  la$year <- y
  pov_list[[y]] <- la
  
}


# 2015-2017
print('Download poverty status (S1701) 2015-17')

for (y in 2015:2017) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S1701"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "011$") |
                         str_detect(variable, "012$") |
                         str_detect(variable, "013$") |
                         str_detect(variable, "013$") |
                         str_detect(variable, "014$") |
                         str_detect(variable, "015$") |
                         str_detect(variable, "016$") |
                         str_detect(variable, "017$") |
                         str_detect(variable, "018$") |
                         str_detect(variable, "019$") |
                         str_detect(variable, "020$") |
                         str_detect(variable, "021$") |
                         str_detect(variable, "022$") |
                         str_detect(variable, "023$") |
                         str_detect(variable, "024$") |
                         str_detect(variable, "025$") |
                         str_detect(variable, "026$")) &
                        str_detect(GEOID, "^06037") &
                        (str_detect(variable, "_C01") | str_detect(variable, "_C02"))
  )
  
  la$year <- y
  pov_list[[y]] <- la
  
}

print('Append poverty status dfs')
pov = do.call(rbind, pov_list)

write_csv(pov, "data/Census/poverty_tract.csv")
print('Saved data/Census/poverty_tract.csv')


#------------------------------------------------------------------#
## Poverty Status of Families by Tract -- got all years
#------------------------------------------------------------------#
print('Download poverty status of families (S1702) 2010-17')
pov_list2 = list()

for (y in tract_years) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S1702"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") &
                         (str_detect(variable, "_C01") | str_detect(variable, "_C02")) &
                         str_detect(GEOID, "^06037")) 
  )
  
  la$year <- y
  pov_list2[[y]] <- la
  
}

print('Append poverty status of families dfs')
pov2 = do.call(rbind, pov_list2)

write_csv(pov2, "data/Census/poverty_families_tract.csv")
print('Saved data/Census/poverty_families_tract.csv')


#------------------------------------------------------------------#
## Poverty Status of Families by Household Type by Tract -- got all years
#------------------------------------------------------------------#
print('Download poverty status of families by household type (B17014) 2010-17')
pov_list3 = list()

for (y in tract_years) {
  var <- load_variables(y, "acs5", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "B17014"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "002$") |
                         str_detect(variable, "003$") |
                         str_detect(variable, "009$") |
                         str_detect(variable, "014$")) &
                        str_detect(GEOID, "^06037")
  )
  
  la$year <- y
  pov_list3[[y]] <- la
  
}

print('Append poverty status of families by household type dfs')
pov3 = do.call(rbind, pov_list3)

write_csv(pov3, "data/Census/poverty_families_hh_tract.csv")
print('Saved data/Census/poverty_families_hh_tract.csv')