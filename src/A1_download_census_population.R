# Purpose: Import population and housing-related Census data
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
## Population and Housing Units by Tract -- got all years
#------------------------------------------------------------------#
pop_list = list()

for (y in tract_years) {
  la <- getCensus(name = "acs/acs5", 
                  vars = c("NAME", "group(B01003)"),
                  region = "tract:*", vintage = y, 
                  regionin = "state:06+county:037") 
  
  # Create year column
  la$year <- y
  
  # Save this df as a list item
  pop_list[[y]] <- la
}


# Append all the years into a df
pop = do.call(rbind, pop_list)



housing_list = list()

for (y in tract_years) {
  la <- getCensus(name = "acs/acs5", 
                  vars = c("NAME", "group(B25001)"),
                  region = "tract:*", vintage = y, 
                  regionin = "state:06+county:037") 
  
  la$year <- y
  
  housing_list[[y]] <- la
}

housing = do.call(rbind, housing_list)


# Export as csv
write_csv(pop, "data/Census/population_tract.csv")
write_csv(housing, "data/Census/housing_units_tract.csv")


#------------------------------------------------------------------#
## Population by Block Group -- get 2017, use this to aggregate up to congressional district?
#------------------------------------------------------------------#
la <- getCensus(name = "acs/acs5", 
                vars = c("NAME", "group(B01003)"),
                region = "block group:*", vintage = 2017, 
                regionin = "state:06+county:037") 

la$year <- 2017

write_csv(la, "data/Census/population_block_group.csv")
