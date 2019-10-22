FROM jupyter/datascience-notebook

COPY conda-requirements.txt /tmp/
RUN conda install -c conda-forge -r /tmp/conda-requirements.txt

COPY requirements.txt /tmp/
RUN pip install --requirement /tmp/requirements.txt
