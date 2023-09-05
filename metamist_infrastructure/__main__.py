"""Metamist Test Python Pulumi program"""
import os
from cpg_infra.config import CPGInfrastructureConfig
from metamist_infrastructure import MetamistInfrastructure

GCP_PROJECT = os.getenv('METAMIST_INFRA_GCP_PROJECT')
SLACK_CHANNEL = os.getenv('METAMIST_INFRA_SLACK_CHANNEL')
SLACK_TOKEN_SECRET_NAME = os.getenv('METAMIST_INFRA_SLACK_TOKEN_SECRET_NAME')

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
            'project': f'{GCP_PROJECT}',
            'service_name': 'sample-metadata-api',
            'machine_account': 'sample-metadata-api@sample-metadata.iam.gserviceaccount.com',
        },
        'etl_accessors': ['bbv', 'kccg', 'sonic', 'sano'],
        'slack_channel': f'{SLACK_CHANNEL}',
    },
    'billing': {
        'coordinator_machine_account': '',
        'gcp': {
            'project_id': f'{GCP_PROJECT}',
            'account_id': '',
        },
        'aggregator': {
            'slack_token_secret_name': f'{SLACK_TOKEN_SECRET_NAME}',
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
