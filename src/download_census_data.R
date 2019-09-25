# Purpose: Import Census data (ACS, Zip Code Business Patterns, etc)
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
tract_years <- list(2010, 2012, 2013, 2014, 2015, 2016, 2017)
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


#------------------------------------------------------------------#
## Employment status -- need 2011
# labor force participation rate, employment/population ratio, unemployment rate
#------------------------------------------------------------------#
emp_list = list()

for (y in tract_years) {
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

write_csv(emp, "data/Census/employment_tract.csv")


#------------------------------------------------------------------#
## Median Income by Tract -- need 2011
#------------------------------------------------------------------#
income_list = list()

for (y in tract_years) {
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
                        str_detect(GEOID, "^06037")
  )
  
  la$year <- y
  income_list[[y]] <- la
  
}

income = do.call(rbind, income_list)

write_csv(income, "data/Census/income_tract.csv")


#------------------------------------------------------------------#
## Educational Attainment by Tract -- need 2011
#------------------------------------------------------------------#
edu_list = list()

for (y in tract_years) {
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
                        str_detect(GEOID, "^06037") &
                        str_detect(variable, "_C01_")
  )
  
  la$year <- y
  edu_list[[y]] <- la
  
}

edu = do.call(rbind, edu_list)

write_csv(edu, "data/Census/educational_attainment_tract.csv")


#------------------------------------------------------------------#
## Poverty Status by Tract -- need 2011
#------------------------------------------------------------------#
pov_list = list()

for (y in 2012:2017) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S1701"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
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
                        str_detect(GEOID, "^06037") 
  )
  
  la$year <- y
  pov_list[[y]] <- la
  
}

pov = do.call(rbind, pov_list)

table(pov$year)

write_csv(pov, "data/Census/poverty_tract.csv")


#------------------------------------------------------------------#
## Poverty Status of Families by Tract -- need 2011
#------------------------------------------------------------------#
pov_list2 = list()

for (y in tract_years) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S1702"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") &
                        str_detect(GEOID, "^06037")) 
  )
  
  la$year <- y
  pov_list2[[y]] <- la
  
}

pov2 = do.call(rbind, pov_list2)

table(pov2$year)

write_csv(pov2, "data/Census/poverty_families_tract.csv")


#------------------------------------------------------------------#
## Households receiving Food Stamps by Tract -- need 2011
#------------------------------------------------------------------#
food_list = list()

for (y in tract_years) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S2201"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "004$") |
                         str_detect(variable, "016$")) &
                         str_detect(GEOID, "^06037") 
  )
  
  la$year <- y
  food_list[[y]] <- la
  
}

food = do.call(rbind, food_list)

table(food$year)

write_csv(food, "data/Census/food_stamps_tract.csv")


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

table(public_assistance$year)

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

table(agg_public_assistance$year)

write_csv(agg_public_assistance, "data/Census/aggregate_public_assistance_tract.csv")


#------------------------------------------------------------------#
## Employment, Establishments, and Annual Payroll by Zip Code -- got all years
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

write_csv(business, "data/Census/employment_zipcode_1994_2011.csv")



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

write_csv(business2, "data/Census/employment_zipcode_2012_2016.csv")
