# Purpose: Import employment-related Census data
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
zipcode_years1 <- list(1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 
                       2006, 2007, 2008, 2009, 2010, 2011)
zipcode_years2 <- list(2012, 2013, 2014, 2015, 2016)


#------------------------------------------------------------------#
## Employment status -- got all  years
# labor force participation rate, employment/population ratio, unemployment rate
#------------------------------------------------------------------#
# Variables change in 2015. Grab the same set of variables, and make sure the renaming is correct.
# 2010-2014
print('Download employment status (S2301) 2010-14')
emp_list = list()

for (y in 2010:2014) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S2301"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
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
                         str_detect(variable, "022$") |
                         str_detect(variable, "023$") |
                         str_detect(variable, "025$") |     
                         str_detect(variable, "026$") |     
                         str_detect(variable, "027$") |     
                         str_detect(variable, "028$") |     
                         str_detect(variable, "029$")) &     
                        str_detect(GEOID, "^06037")
  )
  
  la$year <- y
  emp_list[[y]] <- la
  
}

emp = do.call(rbind, emp_list)


# 2015-2017
print('Download employment status (S2301) 2015-17')
emp_list2 = list()

for (y in 2015:2017) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S2301"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
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
                         str_detect(variable, "022$") |
                         str_detect(variable, "023$") |
                         str_detect(variable, "025$") |                        
                         str_detect(variable, "028$") |     
                         str_detect(variable, "031$") |     
                         str_detect(variable, "032$") |     
                         str_detect(variable, "033$") |
                         str_detect(variable, "034$") | 
                         str_detect(variable, "035$")) &     
                        str_detect(GEOID, "^06037")
  )
  
  la$year <- y
  emp_list2[[y]] <- la
  
}

emp2 = do.call(rbind, emp_list2)


# Append dfs and export
print('Append employment status dfs')
emp3 = rbind(emp, emp2)


write_csv(emp3, "data/Census/employment_tract.csv")
print('Saved data/Census/employment_tract.csv')


#------------------------------------------------------------------#
## Employment, Establishments, and Annual Payroll by Zip Code -- got all years
# Zip Code Business Patterns
#------------------------------------------------------------------#
#apis <- listCensusApis()
#View(apis)

# 1994-2011 data
print('Download employment_zipcode 1994-2011')
business_list = list()

for (y in zipcode_years1) {
  ca <- getCensus(name = "zbp", 
                  vars = c("EMP", "ESTAB", "PAYANN"),
                  region = "zipcode", vintage = y,
                  ST = 06)
  
  ca$year <- y
  business_list[[y]] <- ca
}

print('Append employment_zipcode dfs')
business = do.call(rbind, business_list)


write_csv(business, "data/Census/employment_zipcode_1994_2011.csv")
print('Saved data/Census/employment_zipcode_1994_2011.csv')


# 2012-2016 data changes slightly. 
# Annual payroll reported in thousands...and check EMP column. Must also use NAICS2012 code to select for all industries.
print('Download employment_zipcode 2012-17')
business_list2 = list()

for (y in zipcode_years2) {
  ca <- getCensus(name = "zbp", 
                  vars = c("EMP", "ESTAB", "PAYANN", "ST"),
                  region = "zipcode", vintage = y,
                  ST = "06", NAICS2012 = "00")
  
  ca$year <- y
  business_list2[[y]] <- ca
}

print('Append employment_zipcode dfs')
business2 = do.call(rbind, business_list2)


write_csv(business2, "data/Census/employment_zipcode_2012_2016.csv")
print('Saved data/Census/employment_zipcode_2012_2016.csv')
