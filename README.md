hcid-cdbg-metrics
==============================
[![Build Status](https://dev.azure.com/ianrose/hcid-cdbg-metrics/_apis/build/status/CityOfLosAngeles.hcid-cdbg-metrics?branchName=master)](https://dev.azure.com/ianrose/hcid-cdbg-metrics/_build/latest?definitionId=1&branchName=master)

Evaluate Community Development Block Grants

Project Organization
------------

    ├── LICENSE
    ├── Makefile                 <- Makefile with commands like `make data` or `make train`
    ├── README.md                <- The top-level README for developers using this project.
    ├── data                     <- A directory for local, raw, source data.
    ├── gis                      <- A directory for local geospatial data.
    ├── models                   <- Trained and serialized models, model predictions, or model summaries.
    │
    ├── notebooks                <- Jupyter notebooks.
    |
    ├── outputs                  <- A directory for outputs such as tables created.
    |
    ├── processed                <- A directory for processed, final data that is used for analysis.
    │
    ├── references               <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports                  <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures              <- Generated graphics and figures to be used in reporting.
    │
    │
    ├── conda-requirements.txt   <- The requirements file for conda installs.
    ├── requirements.txt         <- The requirements file for reproducing the analysis environment, e.g.
    │                               generated with `pip freeze > requirements.txt`
    │
    ├── setup.py                 <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                      <- Source code for use in this project.
    |
    ├── visualization            <- A directory for visualizations created.


--------

## Running the project 

Copy the approrpiate keys / secrets in your `.env` file. 

Then run `docker-compose up`