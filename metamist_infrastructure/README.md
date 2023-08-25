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
- files in the `etl/extract` directory to `metamist_infrastructure.etl.extract` (including `requirements.txt`)
- files in the `etl/load` directory to `metamist_infrastructure.etl.load` (including `requirements.txt`)

You can install from git using:

```bash
pip install git+https://github.com/populationgenomics/sample-metadata.git@main#subdirectory=metamist_infrastructure
```

Or add the following to your `requirements.txt`:

```text
# other requirements here
metamist-infrastructure @ git+https://github.com/populationgenomics/sample-metadata.git@main#subdirectory=metamist_infrastructure
```


## Local Development

To test standalone driver, setup pulumi and pulumi-gcp

- setup [Pulumi](https://www.pulumi.com/docs/install/)
- create free Individual Pulumi account
- create python environment:

```bash
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
cd metamist_infrastructure
```

- setup [Pulumi & Google Cloud](https://www.pulumi.com/docs/clouds/gcp/get-started/)
- copy Pulumi.yaml & Pulumi.dev.yaml to metamist_infrastracture folder
- set environment variable GCP_PROJECT

```bash
export METAMIST_INFRA_GCP_PROJECT='gcp-project-name'
```

### Deploy stack

```bash
pulumi up
```

### Destroy stack

```bash
pulumi destroy
```
