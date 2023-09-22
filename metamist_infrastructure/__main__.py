"""Metamist Test Python Pulumi program"""
import os

from cpg_infra.config import CPGInfrastructureConfig
from metamist_infrastructure import MetamistInfrastructure

GCP_PROJECT = os.getenv('METAMIST_INFRA_GCP_PROJECT')
SLACK_CHANNEL = os.getenv('METAMIST_INFRA_SLACK_CHANNEL')
SLACK_TOKEN_SECRET_NAME = os.getenv('METAMIST_INFRA_SLACK_TOKEN_SECRET_NAME')
ETL_PRIVATE_REPO_URL = os.getenv('METAMIST_INFRA_ETL_PRIVATE_REPO_URL')

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
        # TODO: comment out below once CPG_INFRA is updated
        # 'etl_environment': 'DEVELOPMENT',
        # 'etl_parser_default_config': {
        #     # Order of config overides:
        #     # 1. parser default config values
        #     # 2. etl_load_default_config
        #     # 3. config from payload
        #     'project': 'milo-dev',
        #     'default_sequencing_type': 'genome',
        #     'default_sequencing_technology': 'long-read',
        #     'default_sample_type': 'blood',
        # },
        # 'etl_private_repo_url': ETL_PRIVATE_REPO_URL,
        # 'etl_private_repo_packages': ['metamist_private'],
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

    # deploy metamist_infrastructure driver
    setup_obj = MetamistInfrastructure(conf)
    setup_obj.main()
