"""
Metamist Test Python Pulumi program

This is a test program to deploy the Metamist Infrastructure to
development gcloud project.

"""
import os
from typing import NamedTuple

from cpg_infra.config import CPGInfrastructureConfig

from metamist_infrastructure import MetamistInfrastructure

GCP_PROJECT = os.getenv('METAMIST_INFRA_GCP_PROJECT')
SLACK_CHANNEL = os.getenv('METAMIST_INFRA_SLACK_CHANNEL')
SLACK_TOKEN_SECRET_NAME = os.getenv('METAMIST_INFRA_SLACK_TOKEN_SECRET_NAME')
ETL_PRIVATE_REPO_NAME = os.getenv('METAMIST_INFRA_ETL_PRIVATE_REPO_NAME')

# simple cpg-infra configuration
conf_dict = {
    'domain': '',
    'budget_currency': '',
    'common_dataset': '',
    'config_destination': '',
    'web_url_template': '',
    'users': {},
    'gcp': {
        'region': 'australia-southeast1',
        'customer_id': '',
        'groups_domain': '',
        'budget_notification_pubsub': '',
        'config_bucket_name': '',
        'dataset_storage_prefix': '',
    },
    'sample_metadata': {
        'gcp': {
            'project': GCP_PROJECT,
            'service_name': 'sample-metadata-api',
            'machine_account': 'sample-metadata-api@sample-metadata.iam.gserviceaccount.com',
        },
        'etl_accessors': ['bbv'],
        'slack_channel': SLACK_CHANNEL,
        'etl_environment': 'DEVELOPMENT',
        'etl_parser_default_config': {
            # Order of config overides:
            # 1. parser default config values
            # 2. etl_load_default_config
            # 3. config from payload
            'project': 'milo-dev',
            'default_sequencing_type': 'genome',
            'default_sequencing_technology': 'long-read',
            'default_sample_type': 'blood',
        },
        'etl_private_repo_packages': ['metamist_private'],
    },
    'billing': {
        'coordinator_machine_account': '',
        'gcp': {
            'project_id': GCP_PROJECT,
            'account_id': '',
        },
        'aggregator': {
            'slack_token_secret_name': SLACK_TOKEN_SECRET_NAME,
            'source_bq_table': '',
            'destination_bq_table': '',
            'slack_channel': '',
            'functions': [],
        },
    },
}
if __name__ == '__main__':
    # construct cpg-infra config
    conf = CPGInfrastructureConfig.from_dict(conf_dict)

    # setup dummy infrastructure, so private repo url can be constructed
    class DummyRegistry(NamedTuple):
        """Dummy Registry
        This is used to mockup the registry object
        """

        location: str
        project: str
        name: str

    class DummyInfra(NamedTuple):
        """Dummy Infrastructure
        This is used to mockup the infrastructure object
        """

        gcp_python_registry: DummyRegistry

    infrastructure = DummyInfra(
        DummyRegistry(
            location='australia-southeast1',
            project=GCP_PROJECT,
            name=ETL_PRIVATE_REPO_NAME,
        )
    )

    # deploy metamist_infrastructure driver
    setup_obj = MetamistInfrastructure(infrastructure, conf)
    setup_obj.main()
