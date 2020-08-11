# Purpose: Import income-related Census data
# Date Created: 9/20/19
# Output: csvs in data/Census

library(tidycensus)
library(tidyverse)
Sys.getenv("CENSUS_API_KEY")
Sys.getenv("CENSUS_KEY")

# Set working directory
setwd("GitHub/hcid-cdbg-metrics")

## Load years
years <- list(2017)


#------------------------------------------------------------------#
## Household Income Ranges by Block Group -- 2017
#------------------------------------------------------------------#
print('Download income ranges by block group (B19001) 2017')
income_range = list()

for (y in years) {
  var <- load_variables(y, "acs5", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "B19001_"))
  
  ca <- get_acs(geography = "block group", year = y, variables = columns$name,
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
  income_range[[y]] <- la
  
}


# Append all the years into a df
print('Append income ranges df')
income_range2 = do.call(rbind, income_range)

write_csv(income_range2, "data/Census/income_range_hh_block_group.csv")
print('Saved data/Census/income_range_hh_block_group.csv')


#------------------------------------------------------------------#
## Median Household Income by Block Group -- 2017
#------------------------------------------------------------------#
print('Download median hh income by block group (B19013) 2017')
medincome = list()

for (y in years) {
  var <- load_variables(y, "acs5", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "B19013_"))
  
  ca <- get_acs(geography = "block group", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") &
                        str_detect(GEOID, "^06037"))
  )
  
  la$year <- y
  medincome[[y]] <- la
  
}


# Append all the years into a df
print('Append income ranges df')
medincome2 = do.call(rbind, medincome)

write_csv(medincome2, "data/Census/medianincome_block_group.csv")
print('Saved data/Census/medianincome_block_group.csv')
