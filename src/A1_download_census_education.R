# Purpose: Import education-related Census data
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
## Educational Attainment by Tract -- got all years
#------------------------------------------------------------------#
# To grab median earnings by edu attainment, need to specify specific vars for the years
# Variables changed in 2015. Grab the same set for 2010-2014 and 2015-2017.
# 2010-2014: 014-015 uses C01 to show percent HS or above, percent BA or above
# 2015-2017: 014-015 uses C02 to show percent HS or above, percent BA or above, and C01 is nan

# 2010-2014
edu_list = list()

for (y in 2010:2014) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S1501"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "006$") |
                         str_detect(variable, "007$") |
                         str_detect(variable, "008$") |
                         str_detect(variable, "009$") |
                         str_detect(variable, "010$") |
                         str_detect(variable, "011$") |
                         str_detect(variable, "012$") |
                         str_detect(variable, "013$") |
                         str_detect(variable, "014$") |
                         str_detect(variable, "015$") |
                         str_detect(variable, "028$") |
                         str_detect(variable, "029$") |
                         str_detect(variable, "030$") |
                         str_detect(variable, "031$") |
                         str_detect(variable, "032$") |
                         str_detect(variable, "033$") |
                         str_detect(variable, "034$") |
                         str_detect(variable, "035$") |
                         str_detect(variable, "036$") |
                         str_detect(variable, "037$")) &
                        str_detect(variable, "_C01") &
                        str_detect(GEOID, "^06037")
  )
  
  la$year <- y
  edu_list[[y]] <- la
  
}

edu = do.call(rbind, edu_list)


# 2015-2017
edu_list2 = list()

for (y in 2015:2017) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S1501"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter(str_detect(GEOID, "^06037") &
                        (
                          ((str_detect(variable, "006$") |
                              str_detect(variable, "007$") |
                              str_detect(variable, "008$") |
                              str_detect(variable, "009$") |
                              str_detect(variable, "010$") |
                              str_detect(variable, "011$") |
                              str_detect(variable, "012$") |
                              str_detect(variable, "013$") |
                              str_detect(variable, "059$") |
                              str_detect(variable, "060$") |
                              str_detect(variable, "061$") |
                              str_detect(variable, "062$") |
                              str_detect(variable, "063$") |
                              str_detect(variable, "064$")) &
                             str_detect(variable, "_C01")) |
                            (
                              (str_detect(variable, "014$") | 
                                 str_detect(variable, "015$") | 
                                 str_detect(variable, "055$") |
                                 str_detect(variable, "056$") |
                                 str_detect(variable, "057$") |
                                 str_detect(variable, "058$")) & 
                                str_detect(variable, "_C02"))
                        )
  )
  
  la$year <- y
  edu_list2[[y]] <- la
  
}

edu2 = do.call(rbind, edu_list2)


# Append dfs and export
edu3 = rbind(edu, edu2)

write_csv(edu3, "data/Census/educational_attainment_tract.csv")


