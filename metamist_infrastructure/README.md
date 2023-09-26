# Metamist infrastructure package

This package contains the infrastructure for the metamist project.
We want to package it up in a way that cpg-infrastructure deployment can consume it, which are exposed
using [entry points](https://amir.rachum.com/python-entry-points/). Hence, this python package
contains all files required to deploy:

- Metamist ETL framework (cloud function, bigquery, pubsub), with currently no consumer

## Installation

The `setup.py` in this directory does some directory magic, it adds:

- files in this directory to `metamist_infrastructure`
- files in the `etl` directory to `metamist_infrastructure.etl` (including `bq_schema.json`)
- files in the `etl/endpoint` directory to `metamist_infrastructure.etl.endpoint` (including `requirements.txt`)

You can install from git using:

```bash
pip install git+https://github.com/populationgenomics/metamist.git@main#subdirectory=metamist_infrastructure
```

Or add the following to your `requirements.txt`:

```text
# other requirements here
metamist-infrastructure @ git+https://github.com/populationgenomics/metamist.git@main#subdirectory=metamist_infrastructure
```
