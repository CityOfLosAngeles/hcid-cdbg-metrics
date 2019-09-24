FROM jupyter/datascience-notebook

RUN conda install -c conda-forge \
  numpy \
  scipy \
  pandas=0.25.0 \
  matplotlib \
  intake=0.5.1 \
  intake-parquet \
  scikit-learn \
  xlrd \
  statsmodels \
  geopandas \
  "pyproj<2" \
  dask=2.1.0 \
  s3fs \
  xlsxwriter  \
  tqdm=4.33.0 \
  pymc3
  
COPY requirements.txt /tmp/
RUN pip install --requirement /tmp/requirements.txt
