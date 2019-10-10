*** Purpose: Reshape Census data
*** Date: 9/25/19


local folder "C:\Users\404031\Documents\GitHub\hcid-cdbg-metrics\data\"

use "`folder'raw_census_long.dta", clear


keep if table=="food"
keep GEOID year new_var value2

reshape wide value2, i(GEOID year) j(new_var) string

renvars value2*, predrop(6)

gen pct_food = hh_food_hh / hh_hh
gen pct_food_pov = hh_food_hh_pov / hh_hh_pov

drop hh_food_hh hh_food_hh_pov
