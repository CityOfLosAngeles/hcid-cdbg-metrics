.PHONY: pip conda install mirror test clean_census

conda: conda-requirements.txt
	conda install -c conda-forge --yes --file conda-requirements.txt

pip: requirements.txt
	pip install -r requirements.txt
	pip install -e .

install: conda pip

mirror:
	intake-dcat mirror manifest.yml > catalogs/open-data.yml

dedupe: 
	python src/D1_csv_dedupe.py


# Do not run A* because of Census API rate-limiting
# Do not run B1 because it reads CSVs locally, then writes to S3 (will fail CI/CD)
test:
	python src/B2_clean_census.py
	python src/B3_clean_values.py
	python src/C1_clip_boundaries.py
	python src/C2_create_boundary_crosswalks.py
	make -C notebooks
	
clean_census:	
	python src/B1_compile_census.py
	python src/B2_clean_census.py
	python src/B3_clean_values.py
