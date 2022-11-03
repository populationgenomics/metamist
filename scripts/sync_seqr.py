import csv
import io
import os
import re
import json
import datetime
import logging
from typing import Any
import yaml
from io import StringIO

import requests
from cloudpathlib import AnyPath
from sample_metadata.model.analysis_type import AnalysisType
from sample_metadata.model.analysis_status import AnalysisStatus
from sample_metadata.model.sequence_type import SequenceType
from sample_metadata.model.export_type import ExportType
from sample_metadata.model.body_get_samples import BodyGetSamples
from sample_metadata.model.body_get_participants import BodyGetParticipants
from sample_metadata.model.analysis_query_model import AnalysisQueryModel
from sample_metadata.configuration import get_google_identity_token, TOKEN_AUDIENCE

from sample_metadata.apis import (
    SeqrApi,
    ProjectApi,
    AnalysisApi,
    SequenceApi,
    SampleApi,
    ParticipantApi,
)

loggers_to_silence = [
    'google.auth.transport.requests',
    'google.auth._default',
    'google.auth.compute_engine._metadata',
]
for lname in loggers_to_silence:
    tlogger = logging.getLogger(lname)
    tlogger.setLevel(level=logging.CRITICAL)

logger = logging.getLogger('sync-seqr')

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
}

SAMPLES_TO_IGNORE = {'CPG227355', 'CPG227397'}
BASE, SEQR_AUDIENCE = ENVS[ENVIRONMENT]

url_individuals_table_upload = '/api/project/{projectGuid}/upload_individuals_table/sa'
url_individuals_table_confirm = (
    '/api/project/{projectGuid}/save_individuals_table/{uploadedFileId}/sa'
)

url_individual_metadata_table_upload = (
    '/api/project/{projectGuid}/upload_individuals_metadata_table/sa'
)
url_individual_metadata_table_confirm = (
    '/api/project/{projectGuid}/save_individuals_metadata_table/{uploadedFileId}/sa'
)

url_family_table_upload = '/api/project/{projectGuid}/upload_families_table/sa'
url_family_table_confirm = '/api/project/{projectGuid}/edit_families/sa'

url_update_es_index = '/api/project/{projectGuid}/add_dataset/variants/sa'
url_update_saved_variants = '/api/project/{projectGuid}/update_saved_variant_json/sa'

seqrapi = SeqrApi()
papi = ProjectApi()
aapi = AnalysisApi()

ES_INDICES_YAML = """
exome:
    hereditary-neuro: hereditary-neuro-exome-2022_0915_1407_zvg8e
    mito-disease: mito-disease-exome-2022_0915_1407_zvg8e
    kidgen: kidgen-exome-2022_0915_1407_zvg8e
    acute-care: acute-care-exome-2022_0915_1407_zvg8e
    validation: validation-exome-2022_0915_1315_htupt

genome:
    acute-care: acute-care-genome-2022_0815_1644_xkhvx
    heartkids: heartkids-genome-2022_0812_1925_fhoif
    kidgen: kidgen-genome-2022_0812_1925_fhoif
    ohmr4-epilepsy: ohmr4-epilepsy-genome-2022_0812_1925_fhoif
    schr-neuro: schr-neuro-genome-2022_0812_1925_fhoif
    ravenscroft-rdstudy: ravenscroft-rdstudy-genome-2022_0812_1925_fhoif
    validation: validation-genome-2022_0812_1925_fhoif
    perth-neuro: perth-neuro-genome-2022_0812_1925_fhoif
    mito-disease: mito-disease-genome-2022_0812_1925_fhoif
    ibmdx: ibmdx-genome-2022_0812_1925_fhoif
    hereditary-neuro: hereditary-neuro-genome-2022_0812_1925_fhoif
    circa: circa-genome-2022_0812_1925_fhoif
    ravenscroft-arch: ravenscroft-arch-genome-2022_0812_1925_fhoif
    ohmr3-mendelian: ohmr3-mendelian-genome-2022_0812_1925_fhoif
    ag-hidden: ag-hidden-genome-2022_0812_1925_fhoif
"""

ES_INDICES = yaml.safe_load(StringIO(ES_INDICES_YAML))


def sync_dataset(dataset: str, seqr_guid: str, sequence_type: str):
    """
    Synchronisation driver for a single dataset
    """
    seqapi = SequenceApi()
    samapi = SampleApi()

    # sync people first
    token = get_token()
    headers: dict[str, str] = {'Authorization': f'Bearer {token}'}
    params: dict[str, Any] = dict(
        dataset=dataset, project_guid=seqr_guid, headers=headers
    )

    # check sequence type is valid
    _ = SequenceType(sequence_type)

    samples = samapi.get_samples(body_get_samples=BodyGetSamples(project_ids=[dataset]))
    sequences_all = seqapi.get_sequence_ids_for_sample_ids_by_type(
        [s['id'] for s in samples if s['id'] not in SAMPLES_TO_IGNORE]
    )
    sample_ids = set(
        [
            sid
            for sid, types in sequences_all.items()
            if sequence_type in types and sid not in SAMPLES_TO_IGNORE
        ]
    )
    participant_ids = set(
        int(sample['participant_id'])
        for sample in samples
        if sample['id'] in sample_ids
    )
    participants = ParticipantApi().get_participants(
        project=dataset,
        body_get_participants=BodyGetParticipants(
            internal_participant_ids=list(participant_ids)
        ),
    )
    participant_eids = [p['external_id'] for p in participants]

    ped_rows = seqrapi.get_pedigree(project=dataset)
    filtered_family_eids = set(
        row['family_id'] for row in ped_rows if row['individual_id'] in participant_eids
    )

    if not participant_eids:
        raise ValueError('No participants to sync?')
    if not filtered_family_eids:
        raise ValueError('No families to sync')

    sync_pedigree(**params, family_eids=filtered_family_eids)
    sync_families(**params, family_eids=filtered_family_eids)
    sync_individual_metadata(**params, participant_eids=set(participant_eids))
    update_es_index(**params, sequence_type=sequence_type)

    get_cram_map(
        dataset, participant_eids=participant_eids, sequence_type=sequence_type
    )


def sync_pedigree(dataset, project_guid, headers, family_eids: set[str]):
    """
    Synchronise pedigree from SM -> seqr in 3 steps:

    1. Get pedigree from SM
    2. Upload pedigree to seqr
    3. Confirm the upload
    """

    # 1. Get pedigree from SM
    pedigree_data = _get_pedigree_csv_from_sm(dataset, family_eids=family_eids)
    if not pedigree_data:
        return print(f'{dataset} :: Not updating pedigree because not data was found')

    # 2. Upload pedigree to seqr

    # use a filename ending with .csv to signal to seqr it's comma-delimited
    files = {'datafile': ('file.csv', pedigree_data)}
    req1_url = BASE + url_individuals_table_upload.format(projectGuid=project_guid)
    resp_1 = requests.post(req1_url, files=files, headers=headers)
    logger.debug(
        f'{dataset} :: Uploaded pedigree data with status: {resp_1.status_code}'
    )
    if not resp_1.ok:
        logger.warning(f'{dataset} :: Uploading pedigree failed: {resp_1.text}')
    resp_1.raise_for_status()
    resp_1_json = resp_1.json()
    if not resp_1_json or 'uploadedFileId' not in resp_1_json:
        raise ValueError(
            f'{dataset} :: Could not get uploadedFileId from {resp_1_json}'
        )
    uploaded_file_id = resp_1_json['uploadedFileId']

    # 3. Confirm the upload
    req2_url = BASE + url_individuals_table_confirm.format(
        projectGuid=project_guid, uploadedFileId=uploaded_file_id
    )
    is_ok = False
    remaining_attempts = 5
    while not is_ok:
        resp_2 = requests.post(req2_url, json=resp_1_json, headers=headers)
        is_ok = resp_2.ok
        if not is_ok:
            logger.warning(
                f'{dataset} :: Confirming pedigree failed, retrying '
                f'({remaining_attempts} more times): {resp_2.text}'
            )
            remaining_attempts -= 1
        if remaining_attempts <= 0:
            resp_2.raise_for_status()

    print(f'{dataset} :: Uploaded pedigree')


def sync_families(
    dataset, project_guid: str, headers: dict[str, str], family_eids: set[str]
):
    """
    Synchronise families template from SM -> seqr in 3 steps:

        1. Get family from SM
        2. Upload pedigree to seqr
        3. Confirm the upload
    """
    logger.debug(f'{dataset} :: Uploading family template')

    def _get_families_csv_from_sm(dataset: str):

        fam_rows = seqrapi.get_families(project=dataset)
        fam_row_headers = [
            'Family ID',
            'Display Name',
            'Description',
            'Coded Phenotype',
        ]
        formatted_fam_rows = [','.join(fam_row_headers)]
        keys = ['external_id', 'external_id', 'description', 'coded_phenotype']
        formatted_fam_rows.extend(
            [
                ','.join(str(r[k] or '') for k in keys)
                for r in fam_rows
                if r['external_id'] in family_eids
            ]
        )

        if len(formatted_fam_rows) == 1:
            raise ValueError('No families to sync')

        return '\n'.join(formatted_fam_rows)

    # 1. Get family data from SM
    family_data = _get_families_csv_from_sm(dataset)

    # use a filename ending with .csv to signal to seqr it's comma-delimited
    files = {'datafile': ('file.csv', family_data)}
    req1_url = BASE + url_family_table_upload.format(projectGuid=project_guid)
    resp_1 = requests.post(req1_url, files=files, headers=headers)
    logger.debug(
        f'{dataset} :: Uploaded new family template with status: {resp_1.status_code}'
    )
    if not resp_1.ok:
        logger.warning(f'{dataset} :: Request failed with information: {resp_1.text}')
    resp_1.raise_for_status()
    resp_1_json = resp_1.json()

    # 3. Confirm uploaded file

    req2_url = BASE + url_family_table_confirm.format(projectGuid=project_guid)
    is_ok = False
    remaining_attempts = 5
    while not is_ok:
        resp_2 = requests.post(req2_url, json=resp_1_json, headers=headers)
        is_ok = resp_2.ok
        if not is_ok:
            logger.warning(
                f'{dataset} :: Confirming family import failed: {resp_2.text}'
            )
            remaining_attempts -= 1
        if remaining_attempts <= 0:
            resp_2.raise_for_status()

    print(f'{dataset} :: Uploaded family template')


def sync_individual_metadata(
    dataset, project_guid, headers, participant_eids: set[str]
):

    IS_OLD = False

    if IS_OLD:
        # TEMP
        base = 'https://sample-metadata-api-mnrpw3mdza-ts.a.run.app'

        resp = requests.get(
            base
            + f'/api/v1/participant/{dataset}/individual-metadata-seqr?export_typejson',
            headers={
                'Authorization': f'Bearer {get_google_identity_token(TOKEN_AUDIENCE)}'
            },
        )
        resp.raise_for_status()
        individual_metadata_resp = resp.json()

    else:
        individual_metadata_resp = seqrapi.get_individual_metadata_for_seqr(
            project=dataset, export_type=ExportType('json')
        )

        if individual_metadata_resp is None or isinstance(
            individual_metadata_resp, str
        ):
            print(
                f'{dataset} :: There is an issue with getting individual metadata from SM, please try again later'
            )
            return

    json_rows = individual_metadata_resp['rows']
    individual_meta_headers = individual_metadata_resp['headers']
    col_header_map = individual_metadata_resp['header_map']

    if len(json_rows) == 0:
        print(f'{dataset} :: No individual metadata to sync')
        return

    output = io.StringIO()
    writer = csv.writer(output, delimiter='\t')
    rows = [
        [col_header_map[h] for h in individual_meta_headers],
        *[
            [row.get(kh, '') for kh in individual_meta_headers]
            for row in json_rows
            if row['individual_id'] in participant_eids
        ],
    ]
    writer.writerows(rows)
    file_tsv = output.getvalue()

    req1_url = BASE + url_individual_metadata_table_upload.format(
        projectGuid=project_guid
    )
    resp_1 = requests.post(
        req1_url, files={'datafile': ('file.tsv', file_tsv)}, headers=headers
    )
    print(
        f'{dataset} :: Uploaded individual metadata with status: {resp_1.status_code}'
    )

    if (
        resp_1.status_code == 400
        and 'Unable to find individuals to update' in resp_1.text
    ):
        print('No individual metadata needed updating')
        return
    elif not resp_1.ok:
        print(f'{dataset} :: Request failed with information: {resp_1.text}')
        resp_1.raise_for_status()

    resp_1_json = resp_1.json()
    if not resp_1_json or 'uploadedFileId' not in resp_1_json:
        raise ValueError(
            f'{dataset} :: Could not get uploadedFileId from {resp_1_json}'
        )

    uploaded_file_id = resp_1_json['uploadedFileId']

    req2_url = BASE + url_individual_metadata_table_confirm.format(
        projectGuid=project_guid, uploadedFileId=uploaded_file_id
    )
    is_ok = False
    remaining_attempts = 5
    while not is_ok:
        resp_2 = requests.post(req2_url, json=resp_1_json, headers=headers)
        is_ok = resp_2.ok
        if not is_ok:
            logger.warning(
                f'{dataset} :: Confirming individual metadata import failed: {resp_2.text}'
            )
            remaining_attempts -= 1
        if remaining_attempts <= 0:
            resp_2.raise_for_status()

    print(f'{dataset} :: Uploaded individual metadata')


def update_es_index(dataset, sequence_type: str, project_guid, headers):

    # person_sample_map_rows = (
    #     seqapi.get_external_participant_id_to_internal_sample_id_export(project=dataset, export_type='tsv')
    # )

    person_sample_map_rows = seqrapi.get_external_participant_id_to_internal_sample_id(
        project=dataset
    )

    rows_to_write = [
        '\t'.join(s[::-1])
        for s in person_sample_map_rows
        if not any(sid in s for sid in SAMPLES_TO_IGNORE)
    ]

    filename = f'{dataset}_pid_sid_map_{datetime.datetime.now().isoformat()}.tsv'
    filename = re.sub(r'[/\\?%*:|\'<>\x7F\x00-\x1F]', '-', filename)

    fn_path = os.path.join(MAP_LOCATION, filename)
    # pylint: disable=no-member
    with AnyPath(fn_path).open('w+') as f:
        f.write('\n'.join(rows_to_write))

    es_index_analyses = sorted(
        aapi.query_analyses(
            AnalysisQueryModel(
                projects=[dataset],
                type=AnalysisType('es-index'),
                meta={'sequencing_type': sequence_type},
                status=AnalysisStatus('completed'),
            )
        ),
        key=lambda el: el['timestamp_completed'],
    )

    if False:  # len(es_index_analyses) > 0:
        es_index = es_index_analyses[-1]['output']
    else:
        es_index = ES_INDICES[sequence_type][dataset]
        print(f'{dataset} :: Falling back to YAML es-index: "{es_index}"')

    data = {
        'elasticsearchIndex': es_index,
        'datasetType': 'VARIANTS',
        'mappingFilePath': fn_path,
        'ignoreExtraSamplesInCallset': True,
    }
    print(data)

    req1_url = BASE + url_update_es_index.format(projectGuid=project_guid)
    resp_1 = requests.post(req1_url, json=data, headers=headers)
    print(f'{dataset} :: Updated ES index with status: {resp_1.status_code}')
    if not resp_1.ok:
        print(f'{dataset} :: Request failed with information: {resp_1.text}')
    resp_1.raise_for_status()

    req2_url = BASE + url_update_saved_variants.format(projectGuid=project_guid)
    resp_2 = requests.post(req2_url, json={}, headers=headers)
    print(f'{dataset} :: Updated saved variants with status code: {resp_2.status_code}')
    if not resp_2.ok:
        print(f'{dataset} :: Request failed with information: {resp_2.text}')
    resp_2.raise_for_status()


def _get_pedigree_csv_from_sm(dataset: str, family_eids: set[str]) -> str | None:
    """Call get_pedigree and return formatted string with header"""

    ped_rows = seqrapi.get_pedigree(project=dataset)
    if not ped_rows:
        return None

    formatted_ped_rows = [
        'Family ID,Individual ID,Paternal ID,Maternal ID,Sex,Affected Status,Notes'
    ]
    keys = [
        'family_id',
        'individual_id',
        'paternal_id',
        'maternal_id',
        'sex',
        'affected',
        'notes',
    ]
    formatted_ped_rows.extend(
        ','.join(str(r.get(k) or '') for k in keys)
        for r in ped_rows
        if r['family_id'] in family_eids
    )

    return '\n'.join(formatted_ped_rows)


def get_cram_map(dataset, participant_eids: list[str], sequence_type):
    logger.info(f'{dataset} :: Getting cram map')

    IS_OLD = False

    if IS_OLD:
        base = 'https://sample-metadata-api-mnrpw3mdza-ts.a.run.app'

        resp = requests.get(
            base + f'/api/v1/analysis/{dataset}/sample-cram-path-map/tsv',
            headers={
                'Authorization': f'Bearer {get_google_identity_token(TOKEN_AUDIENCE)}'
            },
        )
        resp.raise_for_status()
        reads_map = resp.text
    else:
        reads_map = aapi.get_samples_reads_map(project=dataset, export_type='tsv')

    if isinstance(reads_map, str) and reads_map.startswith('<!doctype html>'):
        print(f'{dataset} :: Bad API format (404d) for reads map')
        return
    if not reads_map:
        print(f'{dataset} :: No CRAMS to sync in for reads map')
        return

    reads_list = set(
        l.strip()
        for l in list(set(reads_map.split("\n")))
        if not any(s in l for s in SAMPLES_TO_IGNORE)
    )
    sequence_filter = lambda row: True
    if sequence_type == 'genome':
        sequence_filter = lambda row: len(row) > 2 and 'exome' not in row[1]
    elif sequence_type == 'exome':
        sequence_filter = lambda row: len(row) > 2 and 'exome' in row[1]

    reads_list = [
        "\t".join(l.split("\t")[:2])
        for l in reads_list
        if (not participant_eids or l.split("\t")[0] in participant_eids)
        and sequence_filter(l.split("\t"))
    ]

    # temporarily
    d = '/Users/mfranklin/source/sample-metadata/cram-map/'
    with open(os.path.join(d, dataset + f'-{sequence_type}-cram-map.tsv'), 'w+') as f:
        f.write("\n".join(reads_list))


def get_token():
    import google.auth.exceptions
    import google.auth.transport.requests

    credential_filename = '/Users/mfranklin/Desktop/tmp/seqr/seqr-sync-credentials.json'
    with open(credential_filename, 'r') as f:
        from google.oauth2 import service_account

        info = json.load(f)
        credentials_content = (info.get('type') == 'service_account') and info or None
        credentials = service_account.IDTokenCredentials.from_service_account_info(
            credentials_content, target_audience=SEQR_AUDIENCE
        )
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)
        return credentials.token


def sync_all_datasets(sequence_type: str, ignore: set[str] = None):
    seqr_projects = ProjectApi().get_seqr_projects()
    error_projects = []
    for project in seqr_projects:
        project_name = project['name']
        if ignore and project_name in ignore:
            print(f'Skipping {project_name}')
            continue

        meta_key = f'seqr-project-{sequence_type}'
        seqr_guid = project.get('meta', {}).get(meta_key)
        if not seqr_guid:
            print(f'Skipping "{project_name}" as meta.{meta_key} is not set')
            continue

        try:
            sync_dataset(project_name, seqr_guid, sequence_type=sequence_type)
        except Exception as e:
            error_projects.append((project_name, e))

    if error_projects:
        print(
            'Some projects failed with errors: '
            + '\n'.join(str(o) for o in error_projects)
        )

    return error_projects


def sync_single_dataset_from_name(dataset, sequence_type: str):
    seqr_projects = ProjectApi().get_seqr_projects()
    for project in seqr_projects:
        project_name = project['name']
        if project_name != dataset:
            continue
        meta_key = f'seqr-project-{sequence_type}'
        seqr_guid = project.get('meta', {}).get(meta_key)
        if not seqr_guid:
            raise ValueError(f'{project_name} does NOT have meta.{meta_key} set')
        print(f'Syncing {project_name} to {seqr_guid}')

        return sync_dataset(
            project_name, seqr_guid=seqr_guid, sequence_type=sequence_type
        )

    raise ValueError(f'Could not find {dataset} seqr project')


if __name__ == '__main__':
    # sync_single_dataset_from_name('ohmr4-epilepsy', 'genome')
    sync_all_datasets(sequence_type='exome')
