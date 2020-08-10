# Purpose: Import population and housing-related Census data
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
## Population by Block Group --  2017
#------------------------------------------------------------------#
print('Download population (B01003) by block group, 2017')
pop = list()

for (y in years) {
  var <- load_variables(y, "acs5", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "B01003"))
  
  ca <- get_acs(geography = "block group", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") &
                        str_detect(GEOID, "^06037"))
  )
  
  la$year <- y
  pop[[y]] <- la
  
}


# Append all the years into a df
print('Append pop df')
pop2 = do.call(rbind, pop)

write_csv(pop2, "data/Census/population_block_group.csv")
print('Saved data/Census/population_block_group.csv')
