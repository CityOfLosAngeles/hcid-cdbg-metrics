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

1. Make sure you have docker compose / docker installed in a terminal you can use, also git or whatever VCS (ie, TFS, installed in your same terminal.)

1. Then, clone the repository from github (if first time, otherwise run `git pull` to update). (see the [best practices](https://cityoflosangeles.github.io/best-practices/github.html)) for more. 

1. Once you are in the root of repostiory, ie, when you run `ls` and you can see the Dockerfile, make sure you copy the approrpiate keys / secrets in your `.env` file. (ie, AWS_ACCESS_KEY or similar). You may not need to do this if you are saving the data to local files/

1. Then run `docker-compose up`. This will give you a JupyterLab running on `localhost:8888` that is usefull for running scripts and notebooks. 