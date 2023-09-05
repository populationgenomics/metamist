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


## Local Development & Testing

To test standalone driver, follow the steps:

- create python environment:

```bash
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt

# to test locally you would need cpg_infra project, when deployed, cpg_infra is already installed
pip install git+https://github.com/populationgenomics/cpg-infrastructure.git

cd metamist_infrastructure
pip install --editable .

```

### Pulumi

- setup pulumi and pulumi-gcp
- setup [Pulumi](https://www.pulumi.com/docs/install/)
- create free Individual Pulumi account
- setup [Pulumi & Google Cloud](https://www.pulumi.com/docs/clouds/gcp/get-started/)
- copy Pulumi.yaml & Pulumi.dev.yaml to metamist_infrastracture folder

### Slack

- create private slack channel for testing, call it e.g. "dev-channel"
- create test [Slack App](https://api.slack.com/start/quickstart) in your Slack workspace
- add created app to private channel Integrations
- copy Bot User OAuth Token from created Slack App OAuth Tokens for Your Workspace and create new secret under gc Secret Manager, call it e.g. "dev-slack-secret"
- you can test the Oauth Token if you can see private channel [here](https://api.slack.com/tutorials/tracks)

### Env variables

- set environment variable METAMIST_INFRA_SLACK_CHANNEL, METAMIST_INFRA_SLACK_TOKEN_SECRET_NAME and METAMIST_INFRA_GCP_PROJECT, e.g.:

```bash
export METAMIST_INFRA_SLACK_CHANNEL='dev-channel'
export METAMIST_INFRA_SLACK_TOKEN_SECRET_NAME='dev-slack-secret'
export METAMIST_INFRA_GCP_PROJECT='gcp-project-name'
```

### Deploy stack

```bash
pulumi up
```

### Test Cloud functions

Test extract function:

```bash
curl -X 'PUT' \
  'https://metamist-etl-extract-xyz-run.app' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -d '{"version": "1.0", "method":"test","id":111}'


{"id":"5242fc79-a018-432f-80a5-58a43222e000","success":true}

# other example
curl -X 'PUT' \
  'https://metamist-etl-extract-xyz-run.app' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -d '{"sample_id": "123456", "external_id": "GRK100311", "individual_id": "608", "sequencing_type": "exome", "collection_centre": "KCCG", "collection_date": "2023-08-05T01:39:28.611476", "collection_specimen": "blood"}'


```bash

Test load function:

```bash
curl -X 'PUT' \
  'https://metamist-etl-load-xyz-run.app' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -d '{"request_id": "5242fc79-a018-432f-80a5-58a43222e000"}'


{"id":"5242fc79-a018-432f-80a5-58a43222e000","record":{"id":111,"method":"test","version":"1.0"},"success":true}
```bash

### Destroy stack

```bash
pulumi destroy
```
