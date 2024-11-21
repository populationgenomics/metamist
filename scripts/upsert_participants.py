import csv
import logging
from collections import defaultdict
from io import StringIO
from typing import Dict, List

import click
from google.cloud import storage

from metamist.apis import ParticipantApi
from metamist.graphql import gql, query
from metamist.models import (
    ParticipantUpsert,
    SampleUpsert,
)

SAMPLE_QUERY = gql(
    """
    query getSampleData($project: String!) {
        project(name: $project) {
            id
        }
        sample(project: {eq: $project}) {
            id
            active
            externalId
            sequencingGroups {
            id
            assays {
                id
                meta
            }
            analyses {
                id
                output
                outputs
                meta
                }
            }
        }
    }
    """
)

PARTICIPANT_QUERY = gql(
    """
    query getParticipantData($project: String!) {
    project(name: $project) {
        participants {
            id
            externalId
            }
        }
    }
    """
)

papi = ParticipantApi()


def map_sample_eid_to_sample_data(data: Dict[str, list]):
    """Map sample external ID to sample data"""
    return {sample['externalId']: sample for sample in data['sample']}


def get_sample_data(project: str):
    """Get sample data from metamist"""
    response = query(SAMPLE_QUERY, {'project': project})
    if not isinstance(response, dict):
        raise ValueError('Expected a dictionary response from query')
    return response


def create_participant(sample_data: list, peid: str, sex: int, external_org_name: str):
    """Create participant upsert object"""
    return ParticipantUpsert(
        id=None,
        external_ids={external_org_name: peid},
        reported_sex=sex,
        samples=sample_data,
    )


def create_sample(project_id: int, seid: str, siid: str, external_org_name: str):
    """Create sample upsert object"""
    return SampleUpsert(
        id=siid,
        external_ids={external_org_name: seid},
        project=project_id,
    )


def read_files_from_gcs(bucket_name: str, file_path: str):
    """Read files from GCS"""
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    blob = blob.download_as_text(encoding='utf-8')
    return StringIO(blob)


@click.command()
@click.option('--bucket', help='Bucket containing files. e.g. cpg-bucket-test-upload')
@click.option('--participant-meta', help='Path to participant metadata CSV.')
@click.option('--sample-meta', help='Path to sample metadata CSV.')
@click.option('--sample-external-id-column', help='Column name for sample external ID.')
@click.option('--external-org-name', required=False, help='External organization name.')
def main(
    bucket: str,
    participant_meta: str,
    sample_meta: str,
    sample_external_id_column: str,
    external_org_name: str,
):  # pylint: disable=too-many-locals
    """Upsert participants to metamist"""
    # Query metamist
    project = bucket.split('-')[1]  # project name derived from bucket name
    data_response = get_sample_data(project)
    project_id = data_response.get('project')['id']

    # allow empty external_org_name for backwards compatibility
    if external_org_name is None:
        external_org_name = ''

    sample_eid_mapping = map_sample_eid_to_sample_data(data_response)

    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Read in csv
    participant_data_io = read_files_from_gcs(bucket, participant_meta)
    sample_data_io = read_files_from_gcs(bucket, sample_meta)

    participant_data_list = list(csv.DictReader(participant_data_io))
    sample_data_list = list(csv.DictReader(sample_data_io, delimiter='\t'))

    # Count of samples for each participant
    participant_sample_count: Dict[str, int] = defaultdict(int)
    metadata_map = {}
    for meta_row in participant_data_list:
        metadata_map[meta_row['sampleID']] = meta_row
        peid = meta_row['externalID']
        participant_sample_count[peid] += 1

    # Map peid to a list of sample upsert objects
    participant_sex_map = {}
    participant_sample_map: Dict[str, List[SampleUpsert]] = defaultdict(list)

    for name_row in sample_data_list:
        # Get the external ID
        seid = '.'.join(name_row['CRAM'].split('.')[:2])

        meta_row = metadata_map.get(name_row[sample_external_id_column])
        # Get participant external ID
        peid = meta_row['externalID']

        # Code sex
        sex = 2 if meta_row['isFemale'].upper() == 'TRUE' else 1

        # Get the sample internal ID
        if seid not in sample_eid_mapping:
            print(f'Sample {seid} not found in metamist.')
            continue
        siid = sample_eid_mapping[seid]['id']

        if peid not in participant_sample_map:
            participant_sample_map[peid] = []
            participant_sex_map[peid] = sex
        participant_sample_map[peid].append(
            create_sample(project_id, seid, siid, external_org_name)
        )

    participant_upserts = []
    for peid, sample_upserts in participant_sample_map.items():
        participant_upserts.append(
            create_participant(
                sample_upserts, peid, participant_sex_map[peid], external_org_name
            )
        )

    # Upsert participants
    api_response = papi.upsert_participants(project, participant_upserts)
    print(api_response)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
