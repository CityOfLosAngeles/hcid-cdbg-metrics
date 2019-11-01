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
	#make -C src

clean_census:	
	python src/A7_compile_census.py
	python src/A8_clean_census.py
	python src/A9_clean_values.py
