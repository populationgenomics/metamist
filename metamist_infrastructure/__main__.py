"""Metamist Test Python Pulumi program"""
import os
from cpg_infra.config import CPGInfrastructureConfig
from metamist_infrastructure import MetamistInfrastructure

GCP_PROJECT = os.getenv('METAMIST_INFRA_GCP_PROJECT')

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
        'dataset_storage_prefix': ''
    },
    'sample_metadata': {
        'gcp': {
            'project': f'{GCP_PROJECT}',
            'service_name': '',
            'machine_account': '',
        },
        'etl_accessors': ['bbv', 'kccg', 'sonic', 'sano']
    }
}
if __name__ == '__main__':
    # construct cpg-infra config
    conf = CPGInfrastructureConfig.from_dict(conf_dict)

    # deploy metamist_infrastructure driver
    setup_obj = MetamistInfrastructure(conf)
    setup_obj.main()
