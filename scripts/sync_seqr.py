import csv
import io
import os
import re
import json
import datetime
from typing import Any

import requests
from cloudpathlib import AnyPath
from sample_metadata.model.export_type import ExportType

from sample_metadata.apis import SeqrApi, ProjectApi

MAP_LOCATION = 'gs://cpg-seqr-main/automation/'

SEQR_AUDIENCE = (
    '1021400127367-40kj6v68nlps6unk6bgvh08r5o4djf6b.apps.googleusercontent.com'
)

BASE = os.getenv('SEQR_URL', 'https://seqr.populationgenomics.org.au')
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

seqapi = SeqrApi()
papi = ProjectApi()


def sync_dataset(dataset: str, seqr_guid: str):
    """
    Syncronisation driver for a single dataset
    """
    # sync people first
    token = get_token()
    headers: dict[str, str] = {'Authorization': f'Bearer {token}'}
    params: dict[str, Any] = dict(
        dataset=dataset, project_guid=seqr_guid, headers=headers
    )

    sync_pedigree(**params)
    sync_families(**params)
    sync_individual_metadata(**params)
    update_es_index(**params)


def sync_pedigree(dataset, project_guid, headers):
    """
    Synchronise pedigree from SM -> seqr in 3 steps:

    1. Get pedigree from SM
    2. Upload pedigree to seqr
    3. Confirm the upload
    """
    pedigree_data = _get_pedigree_data_from_sm(dataset)
    if not pedigree_data:
        return print(f'{dataset} :: Not updating pedigree because not data was found')

    req1_url = BASE + url_individuals_table_upload.format(projectGuid=project_guid)
    resp_1 = requests.post(
        req1_url, files={'datafile': ('file.csv', pedigree_data)}, headers=headers
    )
    print(f'Uploaded pedigree data with status: {resp_1.status_code}')
    if not resp_1.ok:
        print(f'Request failed with information: {resp_1.text}')
    resp_1.raise_for_status()

    resp_1_json = resp_1.json()
    if not resp_1_json or 'uploadedFileId' not in resp_1_json:
        raise ValueError(f'Could not get uploadedFileId from {resp_1_json}')

    uploaded_file_id = resp_1_json['uploadedFileId']

    req2_url = BASE + url_individuals_table_confirm.format(
        projectGuid=project_guid, uploadedFileId=uploaded_file_id
    )
    resp_2 = requests.post(req2_url, json=resp_1_json, headers=headers)

    resp_2.raise_for_status()

    print(f'{dataset} :: Uploaded pedigree')


def sync_families(dataset, project_guid: str, headers: dict[str, str]):

    print(f'{dataset} :: Uploading family template')

    fam_rows = seqapi.get_families(project=dataset)
    fam_row_headers = ['Family ID', 'Display Name', 'Description', 'Coded Phenotype']
    formatted_fam_rows = [','.join(fam_row_headers)]
    formatted_fam_rows.extend(
        ','.join(
            [
                r['external_id'],
                r['external_id'],
                r['description'] or '',
                r['coded_phenotype'] or '',
            ]
        )
        for r in fam_rows
    )

    family_data = '\n'.join(formatted_fam_rows)

    req1_url = BASE + url_family_table_upload.format(projectGuid=project_guid)
    resp_1 = requests.post(
        req1_url, files={'datafile': ('file.csv', family_data)}, headers=headers
    )
    print(f'Uploaded new family template with status: {resp_1.status_code}')
    if not resp_1.ok:
        print(f'Request failed with information: {resp_1.text}')
    resp_1.raise_for_status()

    resp_1_json = resp_1.json()

    req2_url = BASE + url_family_table_confirm.format(projectGuid=project_guid)
    resp_2 = requests.post(req2_url, json=resp_1_json, headers=headers)

    if not resp_2.ok:
        print(f'Request failed with information: {resp_2.text}')

    resp_2.raise_for_status()

    print(f'{dataset} :: Uploaded family template')


def sync_individual_metadata(dataset, project_guid, headers):

    individual_metadata_resp = seqapi.get_individual_metadata_for_seqr(
        project=dataset, export_type=ExportType('json')
    )

    if individual_metadata_resp is None:
        print(
            'There is an issue with getting individual metadata from SM, please try again later'
        )
        return

    json_rows = individual_metadata_resp['rows']
    headers = individual_metadata_resp['headers']
    col_header_map = individual_metadata_resp['header_map']

    output = io.StringIO()
    writer = csv.writer(output, delimiter=',')
    rows = [
        [col_header_map[h] for h in headers],
        *[[row[kh] for kh in headers] for row in json_rows],
    ]
    writer.writerows(rows)

    req1_url = BASE + url_individual_metadata_table_upload.format(
        projectGuid=project_guid
    )
    resp_1 = requests.post(
        req1_url, files={'datafile': ('file.csv', output.getvalue())}, headers=headers
    )
    print(f'Uploaded individual metadata with status: {resp_1.status_code}')
    if not resp_1.ok:
        print(f'Request failed with information: {resp_1.text}')
    resp_1.raise_for_status()

    resp_1_json = resp_1.json()
    if not resp_1_json or 'uploadedFileId' not in resp_1_json:
        raise ValueError(f'Could not get uploadedFileId from {resp_1_json}')

    uploaded_file_id = resp_1_json['uploadedFileId']

    req2_url = BASE + url_individual_metadata_table_confirm.format(
        projectGuid=project_guid, uploadedFileId=uploaded_file_id
    )
    resp_2 = requests.post(req2_url, json=resp_1_json, headers=headers)

    resp_2.raise_for_status()

    print(f'{dataset} :: Uploaded individual metadata')


def update_es_index(dataset, project_guid, headers):

    person_sample_map_rows = seqapi.get_external_participant_id_to_internal_sample_id(
        project=dataset
    )

    rows_to_write = ['\t'.join(s[::-1]) for s in person_sample_map_rows]

    filename = f'{dataset}_pid_sid_map_{datetime.datetime.now().isoformat()}.tsv'
    filename = re.sub(r'[/\\?%*:|\'<>\x7F\x00-\x1F]', '-', filename)

    fn_path = os.path.join(MAP_LOCATION, filename)
    # pylint: disable=no-member
    with AnyPath(fn_path).open('w+') as f:
        f.write('\n'.join(rows_to_write))

    es_index = 'validation-genome-2022_0618_1117_3osvi'

    data = {
        'elasticsearchIndex': es_index,
        'datasetType': 'VARIANTS',
        'mappingFilePath': fn_path,
        'ignoreExtraSamplesInCallset': False,
    }

    req1_url = BASE + url_update_es_index.format(projectGuid=project_guid)
    resp_1 = requests.post(req1_url, json=data, headers=headers)
    print(f'Updated ES index with status: {resp_1.status_code}')
    if not resp_1.ok:
        print(f'Request failed with information: {resp_1.text}')
    resp_1.raise_for_status()

    req2_url = BASE + url_update_saved_variants.format(projectGuid=project_guid)
    resp_2 = requests.post(req2_url, json={}, headers=headers)
    print(f'Updated saved variants with status code: {resp_2.status_code}')
    if not resp_2.ok:
        print(f'Request failed with information: {resp_2.text}')
    resp_2.raise_for_status()


def _get_pedigree_data_from_sm(dataset: str) -> str | None:
    """Call get_pedigree and return formatted string with header"""

    ped_rows = seqapi.get_pedigree(project=dataset)
    if not ped_rows:
        return None

    formatted_ped_rows = [
        'Family ID,Individual ID,Paternal ID,Maternal ID,Sex,Affected Status'
    ]
    keys = [
        'family_id',
        'individual_id',
        'paternal_id',
        'maternal_id',
        'sex',
        'affected',
    ]
    formatted_ped_rows.extend(','.join(str(r[k] or '') for k in keys) for r in ped_rows)

    return '\n'.join(formatted_ped_rows)


def get_token():
    import google.auth.exceptions
    import google.auth.transport.requests

    credential_filename = '/Users/michael.franklin/source/sample-metadata/scripts/seqr-308602-e156282e7b01.json'
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


def sync_all_datasets():
    seqr_projects = ProjectApi().get_seqr_projects()
    for project in seqr_projects:
        project_name = project['name']
        seqr_guid = project.get('meta', {}).get('seqr_guid')
        if not seqr_guid:
            print(f'Skipping {project_name} as meta.seqr_guid is not set')
            continue
        sync_dataset(project_name, seqr_guid)


def sync_single_dataset_from_name(dataset):
    seqr_projects = ProjectApi().get_seqr_projects()
    for project in seqr_projects:
        project_name = project['name']
        if project_name != dataset:
            continue
        seqr_guid = project.get('meta', {}).get('seqr_guid')
        if not seqr_guid:
            raise ValueError(
                f'{project_name} does NOT have a meta.seqr_guid is not set'
            )

        sync_dataset(project_name, seqr_guid)

    raise ValueError(f'Could not find {dataset} seqr project')


if __name__ == '__main__':
    sync_single_dataset_from_name('validation')
