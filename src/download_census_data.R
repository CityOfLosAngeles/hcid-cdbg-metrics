# Purpose: Import Census data (ACS, Zip Code Business Patterns, etc)
# Date Created: 9/20/19
# Output:

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
## Population and Housing Units by Tract
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
write_csv(pop, "src/data/Census/population_tract.csv")
write_csv(housing, "src/data/Census/housing_units_tract.csv")


#------------------------------------------------------------------#
## Employment status 
# labor force participation rate, employment/population ratio, unemployment rate
#------------------------------------------------------------------#
emp_list = list()

for (y in 2011) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S2301"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
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
                        str_detect(variable, "022$") |
                        str_detect(variable, "023$") |
                        str_detect(variable, "025$") |                        
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

write_csv(emp, "src/data/Census/employment_tract.csv")


#------------------------------------------------------------------#
## Employment, Establishments, and Annual Payroll by Zip Code
# Zip Code Business Patterns
#------------------------------------------------------------------#
business_list = list()

#apis <- listCensusApis()
#View(apis)

# 1994-2011 data
for (y in zipcode_years1) {
  ca <- getCensus(name = "zbp", 
                  vars = c("EMP", "ESTAB", "PAYANN"),
                  region = "zipcode", vintage = y,
                  ST = 06)
  
  ca$year <- y
  business_list[[y]] <- ca
}

business = do.call(rbind, business_list)

write_csv(business, "src/data/Census/employment_zipcode_1994_2011.csv")



# 2012-2016 data changes slightly. 
# Annual payroll reported in thousands...and check EMP column. Must also use NAICS2012 code to select for all industries.
business_list2 = list()

for (y in zipcode_years2) {
  ca <- getCensus(name = "zbp", 
                  vars = c("EMP", "ESTAB", "PAYANN", "ST"),
                  region = "zipcode", vintage = y,
                  ST = "06", NAICS2012 = "00")
  
  ca$year <- y
  business_list2[[y]] <- ca
}


business2 = do.call(rbind, business_list2)

write_csv(business2, "src/data/Census/employment_zipcode_2012_2016.csv")
