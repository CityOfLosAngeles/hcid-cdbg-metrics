.PHONY: pip conda install

conda: conda-requirements.txt
	conda install -c conda-forge --yes --file conda-requirements.txt

pip: requirements.txt
	pip install -r requirements.txt
	pip install -e .

install: conda pip

mirror:
	intake-dcat mirror manifest.yml > catalogs/open-data.yml

#census_data:
	#r src/A1_download_census_data.R
	#python src/A2_compile_census.py