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
zipcode_years1 <- list(1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 
                       2006, 2007, 2008, 2009, 2010, 2011)
zipcode_years2 <- list(2012, 2013, 2014, 2015, 2016)


#------------------------------------------------------------------#
## Households receiving Food Stamps by Tract -- got all years
#------------------------------------------------------------------#
# Variables change in 2015. Get same set from 2010-2014, 2015-2017.
# 2010-2014
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

food = do.call(rbind, food_list)


# 2015-2017
food_list2 = list()

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
  food_list2[[y]] <- la
  
}

food2 = do.call(rbind, food_list2)


# Append dfs and export
food3 = rbind(food, food2)

write_csv(food3, "data/Census/food_stamps_tract.csv")


#------------------------------------------------------------------#
## Households receiving Public Assistance by Tract -- got all years
#------------------------------------------------------------------#
public_assistance_list = list()

for (y in 2010:2017) {
  la <- getCensus(name = "acs/acs5", 
                  vars = c("NAME", "B19058_001E", "B19058_001M", "B19058_002E", "B19058_002M"),
                  region = "tract:*", vintage = y, 
                  regionin = "state:06+county:037") 
  
  la$year <- y
  
  public_assistance_list[[y]] <- la
}

public_assistance = do.call(rbind, public_assistance_list)

write_csv(public_assistance, "src/data/Census/public_assistance_tract.csv")


#------------------------------------------------------------------#
## Households receiving Public Assistance by Tract -- got all years
#------------------------------------------------------------------#
agg_public_assistance_list = list()

for (y in 2010:2017) {
  la <- getCensus(name = "acs/acs5", 
                  vars = c("NAME", "B19067_001E", "B19067_001M"),
                  region = "tract:*", vintage = y, 
                  regionin = "state:06+county:037") 
  
  la$year <- y
  
  agg_public_assistance_list[[y]] <- la
}

agg_public_assistance = do.call(rbind, agg_public_assistance_list)

write_csv(agg_public_assistance, "data/Census/aggregate_public_assistance_tract.csv")

