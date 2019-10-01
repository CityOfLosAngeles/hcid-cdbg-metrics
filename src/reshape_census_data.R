# Purpose: Clean up column names, reshape df
# Date Created: 9/25/19
# Output:

library(tidyverse)
library(tidyr)

setwd("GitHub/hcid-cdbg-metrics")

df <- read_csv("data/Census/employment_tract.csv")

# Practice on 1 tract...set up the workflow
df <- df[which(df$GEOID=='06037143700' & df$year == 2016) ,]

spread(df, variable, c(estimate, moe))






# Concatenate and create GEOID
#df <- within(df, GEOID <-paste(state, county, tract, sep = ""))

# Substring column names


# Drop columns not needed (all margin of error cols)


sapply(df, typeof)
