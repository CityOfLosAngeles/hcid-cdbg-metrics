# Purpose: Import quality of life and public health-related Census data
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
## Households receiving Food Stamps by Tract -- got all years
#------------------------------------------------------------------#
# Variables change in 2015. Get same set from 2010-2014, 2015-2017.
# 2010-2014
print('Download food stamps (S2201) 2010-14')
food_list = list()

for (y in 2010:2014) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S2201"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "004$") |
                         str_detect(variable, "016$")) &
                        str_detect(GEOID, "^06037") &
                        (str_detect(variable, "_C01") | str_detect(variable, "_c02"))
  )
  
  la$year <- y
  food_list[[y]] <- la
  
}


# 2015-2017
print('Download food stamps (S2201) 2015-17')

for (y in 2015:2017) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S2201"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "021$") |
                         str_detect(variable, "034$")) &
                        str_detect(GEOID, "^06037") & 
                        (str_detect(variable, "_C01") | str_detect(variable, "_C03"))
  )
  
  la$year <- y
  food_list[[y]] <- la
  
}


# Append dfs and export
print('Append food stamps dfs')
food = do.call(rbind, food_list)


write_csv(food, "data/Census/food_stamps_tract.csv")
print('Saved data/Census/food_stamps_tract.csv')


#------------------------------------------------------------------#
## Households receiving Public Assistance by Tract -- got all years
#------------------------------------------------------------------#
print('Download public assistance (B19058) 2010-17')
public_assistance_list = list()

for (y in tract_years) {
  la <- getCensus(name = "acs/acs5", 
                  vars = c("NAME", "B19058_001E", "B19058_001M", "B19058_002E", "B19058_002M"),
                  region = "tract:*", vintage = y, 
                  regionin = "state:06+county:037") 
  
  la$year <- y
  
  public_assistance_list[[y]] <- la
}

print('Append public assistance dfs')
public_assistance = do.call(rbind, public_assistance_list)


write_csv(public_assistance, "data/Census/public_assistance_tract.csv")
print('Saved data/Census/public_assistance/tract.csv')


#------------------------------------------------------------------#
## Households receiving Public Assistance by Tract -- got all years
#------------------------------------------------------------------#
print('Download aggregate public assistance (B19067) 2010-17')
agg_public_assistance_list = list()

for (y in tract_years) {
  la <- getCensus(name = "acs/acs5", 
                  vars = c("NAME", "B19067_001E", "B19067_001M"),
                  region = "tract:*", vintage = y, 
                  regionin = "state:06+county:037") 
  
  la$year <- y
  
  agg_public_assistance_list[[y]] <- la
}

print('Append aggregate public assistance dfs')
agg_public_assistance = do.call(rbind, agg_public_assistance_list)


write_csv(agg_public_assistance, "data/Census/aggregate_public_assistance_tract.csv")
print('Saved data/Census/aggregate_public_assistance_tract.csv')


#------------------------------------------------------------------#
## Health Insurance Coverage -- 
#------------------------------------------------------------------#
print('Download health insurance (DP03) 2010-17')
health_list = list()

for (y in tract_years) {
  var <- load_variables(y, "acs5/profile", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "DP03"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "095$") |
                         str_detect(variable, "096$") |
                         str_detect(variable, "097$") |
                         str_detect(variable, "098$") |
                         str_detect(variable, "099$")) &
                        str_detect(GEOID, "^06037")
  )
  
  la$year <- y
  health_list[[y]] <- la
  
}

print('Append health insurance dfs')
health = do.call(rbind, health_list)


write_csv(health, "data/Census/health_insurance_tract.csv")
print('Saved data/Census/health_insurance_tract.csv')