.PHONY: pip conda install

conda: conda-requirements.txt
	conda install -c conda-forge --yes --file conda-requirements.txt

pip: requirements.txt
	pip install -r requirements.txt
	pip install -e .

install: conda pip

mirror:
	intake-dcat mirror manifest.yml > catalogs/open-data.yml

test:
	make -C notebooks
	make -C src

download_census:
	#r src/A1_download_census_population.R
	#r src/A2_download_census_employment.R
	#r src/A3_download_census_income.R
	#r src/A4_download_census_poverty.R
	#r src/A5_download_census_education.R
	#r src/A6_download_census_qualityoflife.R

clean_census:	
	python src/A7_compile_census.py
	python src/A8_clean_census.py
	python src/A9_clean_values.py
