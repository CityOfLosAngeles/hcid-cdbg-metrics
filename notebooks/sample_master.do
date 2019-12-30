*** Purpose: Cleaning up milestone table

local folder "C:\Users\404031\Documents\GitHub\hcid-cdbg-metrics\data\HIMS\"

use "`folder'sample_master.dta", clear


* Convert to date
replace DateReceived = substr(DateReceived, 1, 10)
replace DateReceived = subinstr(DateReceived, "None", "")
gen date = date(DateReceived, "YMD")
format date %td


* Generate just Milestone Month
gen milestone_month =mofd(date)
format milestone_month %tm


* See how many duplicates there are for every milestone name
keep ProjectNo MilestoneName date milestone_month
duplicates drop
* (20,816 observations deleted)
* --> need to see why there are no duplicates when file first read in, but there are duplicates here


bys ProjectNo milestone_month: gen obs = _n
bys ProjectNo (milestone_month): egen max_obs = max(obs)


	/* tab max_obs

		max_obs |      Freq.     Percent        Cum.
	------------+-----------------------------------
			  1 |      2,361        7.87        7.87
			  2 |     12,006       40.03       47.90
			  3 |      2,532        8.44       56.35
			  4 |      1,773        5.91       62.26
			  5 |      2,847        9.49       71.75
			  6 |      2,357        7.86       79.61
			  7 |      2,703        9.01       88.62
			  8 |      1,581        5.27       93.89
			  9 |      1,138        3.79       97.69
			 10 |        548        1.83       99.51
			 11 |         58        0.19       99.71
			 12 |         47        0.16       99.86
			 13 |         41        0.14      100.00
	------------+-----------------------------------
		  Total |     29,992      100.00 
		  
	--> What should be done if there are multiple milestones in the same month?
	Should a new column be created that just concatenates all the milestones reached
	that month...sorted in order of DateReceived? */

	
bys ProjectNo milestone_month: gen one = _n == 1

	/* tab max_obs if one==1	
	
		max_obs |      Freq.     Percent        Cum.
	------------+-----------------------------------
			  1 |      2,361       15.14       15.14
			  2 |      7,328       46.98       62.12
			  3 |      1,465        9.39       71.51
			  4 |        862        5.53       77.04
			  5 |      1,010        6.48       83.51
			  6 |        793        5.08       88.59
			  7 |        837        5.37       93.96
			  8 |        442        2.83       96.79
			  9 |        329        2.11       98.90
			 10 |        135        0.87       99.77
			 11 |         16        0.10       99.87
			 12 |         11        0.07       99.94
			 13 |          9        0.06      100.00
	------------+-----------------------------------
		  Total |     15,598      100.00 
		  
	--> For every project-month obs, how many observations are there? Almost half have
	2 obs. 85% have 2+ obs.
		  */

** Rather than creating new columns that flag the milestone, let's just concatenante them
* We also don't want to combine categories. Goal is to help them see what they have, don't
* need to aggregate needlessly.
