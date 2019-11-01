.PHONY: test 

# Do not run A* because of Census API rate-limiting
# Do not run B1 because it reads CSVs locally, then writes to S3 (will fail CI/CD)
test:	
	python B2_clean_census.py
	python B3_clean_values.py
	python C1_clip_boundaries.py
	python C2_create_boundary_crosswalks.py