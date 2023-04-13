# pylint: disable=missing-timeout,unnecessary-lambda-assignment,import-outside-toplevel,too-many-locals
import asyncio
import os
import re
import json
import datetime
import logging
import traceback
from collections import defaultdict
from typing import Any
from io import StringIO

import aiohttp
import yaml
from cloudpathlib import AnyPath
from metamist.model.analysis_type import AnalysisType
from metamist.model.analysis_status import AnalysisStatus
from metamist.model.sequence_type import SequenceType
from metamist.model.export_type import ExportType
from metamist.model.body_get_samples import BodyGetSamples
from metamist.model.body_get_participants import BodyGetParticipants
from metamist.model.analysis_query_model import AnalysisQueryModel
from metamist.apis import (
    SeqrApi,
    ProjectApi,
    AnalysisApi,
    SequenceApi,
    SampleApi,
    ParticipantApi,
)
from metamist.parser.generic_parser import chunk

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
    'reanalysis-dev': (
        'https://seqr-reanalysis-dev.populationgenomics.org.au'
        '1021400127367-4vch8s8kc9opeg4v14b2n70se55jpao4.apps.googleusercontent.com'
    ),
    'local': (
        'http://127.0.0.1:8000',
        '1021400127367-4vch8s8kc9opeg4v14b2n70se55jpao4.apps.googleusercontent.com',
    ),
}

SAMPLES_TO_IGNORE = {'CPG227355', 'CPG227397'}
BASE, SEQR_AUDIENCE = ENVS[ENVIRONMENT]

url_individuals_sync = '/api/project/sa/{projectGuid}/individuals/sync'
url_individual_metadata_sync = '/api/project/sa/{projectGuid}/individuals_metadata/sync'
url_family_sync = '/api/project/sa/{projectGuid}/families/sync'

url_update_es_index = '/api/project/sa/{projectGuid}/add_dataset/variants'
url_update_saved_variants = '/api/project/sa/{projectGuid}/saved_variant/update'
url_igv_diff = '/api/project/sa/{projectGuid}/igv/diff'
url_igv_individual_update = '/api/individual/sa/{individualGuid}/igv/update'

seqrapi = SeqrApi()
papi = ProjectApi()
aapi = AnalysisApi()

ES_INDICES_YAML = """
exome:
    test: test

genome:
    example: example
"""

ES_INDICES = yaml.safe_load(StringIO(ES_INDICES_YAML))


def sync_dataset(dataset: str, seqr_guid: str, sequence_type: str):
    """Sync single dataset without looking up seqr guid"""
    return asyncio.new_event_loop().run_until_complete(
        sync_dataset_async(dataset, seqr_guid, sequence_type)
    )


async def sync_dataset_async(dataset: str, seqr_guid: str, sequence_type: str):
    """
    Synchronisation driver for a single dataset
    """
    print(f'{dataset} ({sequence_type}) :: Syncing to {seqr_guid}')
    seqapi = SequenceApi()
    samapi = SampleApi()

    # sync people first
    token = get_token()
    async with aiohttp.ClientSession() as client:
        headers: dict[str, str] = {'Authorization': f'Bearer {token}'}
        params: dict[str, Any] = {
            'dataset': dataset,
            'project_guid': seqr_guid,
            'headers': headers,
            'session': client,
        }

        # check sequence type is valid
        _ = SequenceType(sequence_type)

        samples = samapi.get_samples(
            body_get_samples=BodyGetSamples(project_ids=[dataset])
        )
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
            row['family_id']
            for row in ped_rows
            if 'family_id' in row and row['individual_id'] in participant_eids
        )
        participants_with_families = set(
            row['individual_id']
            for row in ped_rows
            if 'family_id' in row and row['family_id'] in filtered_family_eids
        )

        participant_eids = list(
            set(participant_eids).intersection(participants_with_families)
        )

        if not participant_eids:
            raise ValueError('No participants to sync?')
        if not filtered_family_eids:
            raise ValueError('No families to sync')

        await sync_families(**params, family_eids=filtered_family_eids)
        await sync_pedigree(**params, family_eids=filtered_family_eids)
        await sync_individual_metadata(**params, participant_eids=set(participant_eids))
        await update_es_index(
            **params, sequence_type=sequence_type, internal_sample_ids=sample_ids
        )

        await sync_cram_map(
            **params, participant_eids=participant_eids, sequence_type=sequence_type
        )


async def sync_pedigree(
    session: aiohttp.ClientSession,
    dataset,
    project_guid,
    headers,
    family_eids: set[str],
):
    """
    Synchronise pedigree from SM -> seqr in 3 steps:

    1. Get pedigree from SM
    2. Upload pedigree to seqr
    3. Confirm the upload
    """

    # 1. Get pedigree from SM
    pedigree_data = await _get_pedigree_from_sm(dataset, family_eids=family_eids)
    if not pedigree_data:
        return print(f'{dataset} :: Not updating pedigree because not data was found')

    # 2. Upload pedigree to seqr

    req_url = BASE + url_individuals_sync.format(projectGuid=project_guid)
    resp = await session.post(
        req_url, json={'individuals': pedigree_data}, headers=headers
    )
    if not resp.ok:
        logger.warning(f'{dataset} :: Confirming pedigree failed: {await resp.text()}')
        with open(f'{dataset}.ped', 'w+') as f:
            import csv

            writer = csv.writer(f, delimiter='\t')
            headers = [
                'familyId',
                'individualId',
                'paternalId',
                'maternalId',
                'sex',
                'affected',
            ]
            writer.writerows([[row[h] for h in headers] for row in pedigree_data])

    resp.raise_for_status()

    print(f'{dataset} :: Uploaded pedigree')


async def sync_families(
    session: aiohttp.ClientSession,
    dataset,
    project_guid: str,
    headers: dict[str, str],
    family_eids: set[str],
):
    """
    Synchronise families template from SM -> seqr in 3 steps:

        1. Get family from SM
        2. Upload pedigree to seqr
        3. Confirm the upload
    """
    logger.debug(f'{dataset} :: Uploading family template')

    fam_rows = await seqrapi.get_families_async(project=dataset)
    fam_row_seqr_keys = {
        'familyId': 'external_id',
        'displayName': 'external_id',
        'description': 'description',
        'codedPhenotype': 'coded_phenotype',
    }
    if family_eids:
        fam_rows = [f for f in fam_rows if f['external_id'] in family_eids]

    family_data = [
        {seqr_key: fam.get(mm_key) for seqr_key, mm_key in fam_row_seqr_keys.items()}
        for fam in fam_rows
    ]

    # 1. Get family data from SM

    # use a filename ending with .csv to signal to seqr it's comma-delimited
    req_url = BASE + url_family_sync.format(projectGuid=project_guid)
    resp_2 = await session.post(
        req_url, json={'families': family_data}, headers=headers
    )
    resp_2.raise_for_status()
    print(f'{dataset} :: Uploaded family template')


async def sync_individual_metadata(
    session: aiohttp.ClientSession,
    dataset,
    project_guid,
    headers,
    participant_eids: set[str],
):
    """
    Sync individual participant metadata (eg: phenotypes)
    for a dataset into a seqr project
    """

    individual_metadata_resp = await seqrapi.get_individual_metadata_for_seqr_async(
        project=dataset, export_type=ExportType('json')
    )

    if individual_metadata_resp is None or isinstance(individual_metadata_resp, str):
        print(
            f'{dataset} :: There is an issue with getting individual metadata from SM, please try again later'
        )
        return

    json_rows: list[dict] = individual_metadata_resp['rows']

    if participant_eids:
        json_rows = [
            row for row in json_rows if row['individual_id'] in participant_eids
        ]

    def _process_hpo_terms(terms: str):
        return [t.strip() for t in terms.split(',')]

    def _parse_affected(affected):
        affected = str(affected).upper()
        if affected in ('1', 'U', 'UNAFFECTED'):
            return 'N'
        if affected == '2' or affected.startswith('A'):
            return 'A'
        if not affected or affected in ('0', 'UNKNOWN'):
            return 'U'

        return None

    def _parse_consanguity(consanguity):
        if not consanguity:
            return None

        if isinstance(consanguity, bool):
            return consanguity

        if consanguity.lower() in ('t', 'true', 'yes', 'y', '1'):
            return True

        if consanguity.lower() in ('f', 'false', 'no', 'n', '0'):
            return False

        return None

    key_processor = {
        'hpo_terms_present': _process_hpo_terms,
        'hpo_terms_absent': _process_hpo_terms,
        'affected': _parse_affected,
        'consanguinity': _parse_consanguity,
        # 'individual_notes': lambda x: x + '.'
    }

    seqr_map = {
        'family_id': 'family_id',
        'individual_id': 'individual_id',
        # 'individual_guid'
        # 'hpo_number',
        'affected': 'affected',
        'features': 'hpo_terms_present',
        'absent_features': 'hpo_terms_absent',
        'birth_year': 'birth_year',
        'death_year': 'death_year',
        'onset_age': 'age_of_onset',
        'notes': 'individual_notes',
        # 'assigned_analyst'
        'consanguinity': 'consanguinity',
        'affected_relatives': 'affected_relatives',
        'expected_inheritance': 'expected_inheritance',
        'maternal_ethnicity': 'maternal_ancestry',
        'paternal_ethnicity': 'paternal_ancestry',
        # 'disorders'
        # 'rejected_genes'
        # 'candidate_genes'
    }

    if len(json_rows) == 0:
        print(f'{dataset} :: No individual metadata to sync')
        return

    def process_row(row):
        return {
            seqr_key: key_processor[sm_key](row[sm_key])
            if sm_key in key_processor
            else row[sm_key]
            for seqr_key, sm_key in seqr_map.items()
            if sm_key in row
        }

    processed_records = list(map(process_row, json_rows))

    req_url = BASE + url_individual_metadata_sync.format(projectGuid=project_guid)
    resp = await session.post(
        req_url, json={'individuals': processed_records}, headers=headers
    )
    # print(resp.text)
    resp_text = await resp.text()
    if resp.status == 400 and 'Unable to find individuals to update' in resp_text:
        print(f'{dataset} :: No individual metadata needed updating')
        return

    if not resp.ok:
        print(f'{dataset} :: Error syncing individual metadata {resp_text}')
        resp.raise_for_status()

    print(f'{dataset} :: Uploaded individual metadata')


async def update_es_index(
    session: aiohttp.ClientSession,
    dataset,
    sequence_type: str,
    project_guid,
    headers,
    check_metamist=True,
    allow_skip=False,
    internal_sample_ids: set[str] = None,
):
    """Update seqr samples for latest elastic-search index"""

    person_sample_map_rows = (
        await seqrapi.get_external_participant_id_to_internal_sample_id_async(
            project=dataset
        )
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

    if check_metamist:  # len(es_index_analyses) > 0:
        es_index_analyses = await aapi.query_analyses_async(
            AnalysisQueryModel(
                projects=[dataset, 'seqr'],
                type=AnalysisType('es-index'),
                meta={'sequencing_type': sequence_type, 'dataset': dataset},
                status=AnalysisStatus('completed'),
            )
        )

        es_index_analyses = filter(
            lambda a: a['meta'].get('dataset') == dataset, es_index_analyses
        )
        es_index_analyses = sorted(
            es_index_analyses,
            key=lambda el: el['timestamp_completed'],
        )

        if len(es_index_analyses) == 0:
            if allow_skip:
                logger.warning(f'No ES index for {dataset!r}')

            raise ValueError(f'No ES index for {dataset!r} to synchronise')

        es_index = es_index_analyses[-1]['output']

        if internal_sample_ids:
            sample_ids_missing_from_index = internal_sample_ids - set(
                es_index_analyses[-1]['sample_ids']
            )
            if sample_ids_missing_from_index:
                print(
                    f'{dataset}.{sequence_type} :: Samples missing from index: ',
                    ', '.join(sample_ids_missing_from_index),
                )
    else:
        es_index = ES_INDICES[sequence_type][dataset]
        print(f'{dataset} :: Falling back to YAML es-index: {es_index!r}')

    data = {
        'elasticsearchIndex': es_index,
        'datasetType': 'VARIANTS',
        'mappingFilePath': fn_path,
        'ignoreExtraSamplesInCallset': True,
    }
    req1_url = BASE + url_update_es_index.format(projectGuid=project_guid)
    resp_1 = await session.post(req1_url, json=data, headers=headers)
    print(f'{dataset} :: Updated ES index {es_index!r} with status: {resp_1.status}')
    if not resp_1.ok:
        print(f'{dataset} :: Request failed with information: {resp_1.text}')
    resp_1.raise_for_status()

    req2_url = BASE + url_update_saved_variants.format(projectGuid=project_guid)
    resp_2 = await session.post(req2_url, json={}, headers=headers)
    print(f'{dataset} :: Updated saved variants with status code: {resp_2.status}')
    if not resp_2.ok:
        print(f'{dataset} :: Request failed with information: {resp_2.text()}')
    resp_2.raise_for_status()


async def sync_cram_map(
    session: aiohttp.ClientSession,
    dataset,
    participant_eids: list[str],
    sequence_type,
    project_guid,
    headers,
):
    """Get map of participant EID to cram path"""
    logger.info(f'{dataset} :: Getting cram map')
    reads_map = await aapi.get_samples_reads_map_async(
        project=dataset, export_type='json'
    )

    def _sequence_filter(output_path: str):
        if output_path.removeprefix('gs://').split('/')[0].endswith('-test'):
            return False
        if sequence_type == 'genome' and 'exome' in output_path:
            return False
        if sequence_type == 'exome' and 'exome' not in output_path:
            return False

        return True

    parsed_records = defaultdict(list)
    number_of_uploadable_reads = 0
    already_added = set()

    for row in reads_map:
        pid = row['participant_id']
        output = row['output']
        if output in already_added:
            # don't add duplicates
            continue

        already_added.add(output)
        if not _sequence_filter(output):
            continue

        if participant_eids and pid not in participant_eids:
            continue

        # eventually, we should add the sampleId back in here
        number_of_uploadable_reads += 1
        parsed_records[pid].append({'filePath': output})

    if not parsed_records:
        print(f'{dataset} :: No CRAMS to sync in for reads map')
        return

    req1_url = BASE + url_igv_diff.format(projectGuid=project_guid)
    resp_1 = await session.post(
        req1_url, json={'mapping': parsed_records}, headers=headers
    )
    if not resp_1.ok:
        t = await resp_1.text()
        print(f'{dataset} :: Failed to diff CRAM updates: {t!r}')
    resp_1.raise_for_status()

    response = await resp_1.json()
    if 'updates' not in response:
        print(f'{dataset} :: All CRAMS are up to date')
        return

    async def _make_update_igv_call(update):
        individual_guid = update['individualGuid']
        req_igv_update_url = BASE + url_igv_individual_update.format(
            individualGuid=individual_guid
        )
        resp = await session.post(req_igv_update_url, json=update, headers=headers)

        t = await resp.text()
        if not resp.ok:
            raise ValueError(
                f'{dataset} :: Failed to update {individual_guid} with response: {t!r})',
                resp,
            )

        return t

    chunk_size = 10
    wait_time = 3
    all_updates = response['updates']
    exceptions = []
    for idx, updates in enumerate(chunk(all_updates, chunk_size=10)):
        if not updates:
            continue
        print(
            f'{dataset} :: Updating CRAMs {idx * chunk_size + 1} -> {(min((idx + 1 ) * chunk_size, len(all_updates)))} (/{len(all_updates)})'
        )

        responses = await asyncio.gather(
            *[_make_update_igv_call(update) for update in updates],
            return_exceptions=True,
        )
        exceptions.extend(
            [(r, u) for u, r in zip(updates, responses) if isinstance(r, Exception)]
        )
        await asyncio.sleep(wait_time)

    if exceptions:
        exceptions_str = '\n'.join(f'\t{e} {u}' for e, u in exceptions)
        print(
            f'{dataset} :: Failed to update {len(exceptions)} CRAMs: \n{exceptions_str}'
        )
    print(
        f'{dataset} :: Updated {len(all_updates)} / {number_of_uploadable_reads} CRAMs'
    )


async def _get_pedigree_from_sm(
    dataset: str, family_eids: set[str]
) -> list[dict] | None:
    """Call get_pedigree and return formatted string with header"""

    ped_rows = await seqrapi.get_pedigree_async(project=dataset)
    if not ped_rows:
        return None

    if family_eids:
        ped_rows = [row for row in ped_rows if row['family_id'] in family_eids]

    def process_sex(value):
        if not isinstance(value, int):
            return value
        if value == 0:
            return ''
        if value == 1:
            return 'M'
        if value == 2:
            return 'F'
        return ''

    def process_affected(value):
        if not isinstance(value, int):
            raise ValueError(f'Unexpected affected value: {value}')
        return {
            -9: 'U',
            0: 'U',
            1: 'N',
            2: 'A',
        }[value]

    keys = {
        'familyId': 'family_id',
        'individualId': 'individual_id',
        'paternalId': 'paternal_id',
        'maternalId': 'maternal_id',
        # 'sex': 'sex',
        # 'affected': 'affected',
        'notes': 'notes',
    }

    def get_row(row):
        d = {
            seqr_key: row[sm_key] for seqr_key, sm_key in keys.items() if sm_key in row
        }
        d['sex'] = process_sex(row['sex'])
        d['affected'] = process_affected(row['affected'])
        return d

    rows = list(map(get_row, ped_rows))

    return rows


def get_token():
    """Get identity-token for seqr specific service-credentials"""
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
    """
    Sync all datasets
    """
    seqr_projects = ProjectApi().get_seqr_projects()
    error_projects = []
    el = asyncio.new_event_loop()
    for project in seqr_projects:
        project_name = project['name']
        if ignore and project_name in ignore:
            # print(f'Skipping {project_name}')
            continue

        meta_key = f'seqr-project-{sequence_type}'
        seqr_guid = project.get('meta', {}).get(meta_key)
        if not seqr_guid:
            # print(f'Skipping {project_name!r} as meta.{meta_key} is not set')
            continue
        try:
            el.run_until_complete(
                sync_dataset_async(project_name, seqr_guid, sequence_type=sequence_type)
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            print(
                f'Failed to sync {project_name} with error: {e!r}: {traceback.format_exc()}'
            )
            error_projects.append((project_name, e))

    if error_projects:
        print(
            'Some projects failed with errors: '
            + '\n'.join(str(o) for o in error_projects)
        )

    return error_projects


def sync_single_dataset_from_name(dataset, sequence_type: str):
    """Sync dataset, and fetch seqr guid"""
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
    # datasets = ['acute-care']
    # for dataset in datasets:
    #     sync_single_dataset_from_name(dataset, 'genome')
    # sync_dataset('kidgen', 'R0001_seqr_test_project', sequence_type='exome')
    # sync_single_dataset_from_name('ag-hidden', 'genome')
    sync_single_dataset_from_name('acute-care', 'genome')
    # sync_all_datasets(sequence_type='genome', ignore={'acute-care'})
    # sync_all_datasets(sequence_type='exome', ignore={'flinders-ophthal'})
    # sync_single_dataset_from_name('udn-aus', 'exome')
