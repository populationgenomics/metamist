import os
from enum import Enum

CREDENTIALS_FILENAME = os.environ.get('SEQR_CREDS', '')
MAP_LOCATION = 'gs://cpg-seqr-main-analysis/automation/'
ENVIRONMENT = 'prod'  # 'staging'

ENVS = {
    'staging': (
        'https://seqr-staging.populationgenomics.org.au',
        '1021400127367-40kj6v68nlps6unk6bgvh08r5o4djf6b.apps.googleusercontent.com',
    ),
    'prod': (
        'https://seqr.populationgenomics.org.au',
        '1021400127367-9uc4sikfsm0vqo38q1g6rclj91mm501r.apps.googleusercontent.com',
    ),
    'reanalysis-dev': (
        'https://seqr-reanalysis-dev.populationgenomics.org.au'
        '1021400127367-4vch8s8kc9opeg4v14b2n70se55jpao4.apps.googleusercontent.com'
    ),
    'local': (
        'http://127.0.0.1:8000',
        '1021400127367-4vch8s8kc9opeg4v14b2n70se55jpao4.apps.googleusercontent.com',
    ),
}
BASE, SEQR_AUDIENCE = ENVS[ENVIRONMENT]

SGS_TO_IGNORE: set[str] = set()


class SeqrDatasetType(Enum):
    """Type of dataset (es-index) that can be POSTed to Seqr"""

    SNV_INDEL = 'SNV_INDEL'  # Haplotypecaller in seqr UI
    SV = 'SV'  # SV Caller in seqr UI (WGS projects)
    GCNV = 'SV_WES'  # SV Caller in seqr UI (WES projects)
    MITO = 'MITO'  # Mitochondria Caller in seqr UI


ES_INDEX_STAGES = {
    SeqrDatasetType.SNV_INDEL: 'MtToEs',
    SeqrDatasetType.SV: 'MtToEsSv',
    SeqrDatasetType.GCNV: 'MtToEsCNV',
    SeqrDatasetType.MITO: 'MtToEsMito',
}

ES_INDICES_YAML = """
exome:
    test: test

genome:
    example: example
"""

# API URLs
url_individuals_sync = '/api/project/sa/{projectGuid}/individuals/sync'
url_individual_metadata_sync = '/api/project/sa/{projectGuid}/individuals_metadata/sync'
url_family_sync = '/api/project/sa/{projectGuid}/families/sync'
url_update_es_index = '/api/project/sa/{projectGuid}/add_dataset/variants'
url_update_saved_variants = '/api/project/sa/{projectGuid}/saved_variant/update'
url_igv_diff = '/api/project/sa/{projectGuid}/igv/diff'
url_igv_individual_update = '/api/individual/sa/{individualGuid}/igv/update'
