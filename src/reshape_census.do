*** Purpose: Reshape Census data
*** Date: 9/25/19

*** tidycensus downloaded data are long (in Stata)
*** censusapi downloaded data are wide (in Python)

local folder "C:\Users\404031\Documents\GitHub\hcid-cdbg-metrics\data\"

use "`folder'raw_census.dta", clear


foreach file in emp edu food income pov pov_fam {

	if "`file'"=="emp" {
		loc oldcol "C01_001 C02_001 C03_001 C04_001" 
		loc newcol "pop16 lf16 employed16 unemprate16"
		loc n: word count `oldcol'
		
		forval i = 1/`n' {
			loc a: word `i' of `oldcol'
			loc b: word `i' of `newcol'
			replace variable = "`b'" if variable=="`a'"
		}

	}




}

*** Prep for reshape
renvars estimate moe \ est_ moe_
reshape wide est moe, i(GEOID NAME year) j(variable) string
