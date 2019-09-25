# Purpose: Clean up column names, reshape df
# Date Created: 9/25/19
# Output:

library(tidyverse)

setwd("GitHub/hcid-cdbg-metrics")

df <- read_csv("data/Census/population_tract.csv")

# Practice on 1 tract...set up the workflow
df <- df[which(df$tract==143700),]

# Concatenate and create GEOID
df <- within(df, GEOID <-paste(state, county, tract, sep = ""))

# Substring column names
df %>% select(matches('B01003')) %>% substr(names(df), start = 8, stop = 11)


# Drop columns not needed (all margin of error cols)


sapply(df, typeof)
