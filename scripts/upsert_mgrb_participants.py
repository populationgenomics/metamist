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

BUCKET_NAME = 'cpg-mgrb-test-upload'

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


def create_participant(sample_data: list, peid: str, sex: int):
    """Create participant upsert object"""
    participant_upsert = [
        ParticipantUpsert(
            id=None,
            external_ids={'': peid},
            reported_sex=sex,
            samples=sample_data,
        ),
    ]
    return participant_upsert


def create_sample(project_id: int, seid: str, siid: str):
    """Create sample upsert object"""
    return SampleUpsert(
        id=siid,
        external_ids={'': seid},
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
@click.option('--project', help='Project name to upsert participants to.')
@click.option('--participant-meta', help='Path to participant metadata CSV.')
@click.option('--sample-meta', help='Path to sample metadata CSV.')
def main(project: str, participant_meta: str, sample_meta: str):
    """Upsert participants to metamist"""
    # Query metamist
    data_response = get_sample_data(project)
    project_id = data_response.get('project')['id']

    sample_eid_mapping = map_sample_eid_to_sample_data(data_response)

    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Read in csv
    participant_data_io = read_files_from_gcs(BUCKET_NAME, participant_meta)
    sample_data_io = read_files_from_gcs(BUCKET_NAME, sample_meta)

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

        meta_row = metadata_map.get(name_row['RGSM (SampleID)'])
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
        participant_sample_map[peid].append(create_sample(project_id, seid, siid))

    participant_upserts = []
    for peid, sample_upserts in participant_sample_map.items():
        participant_upserts.append(
            create_participant(sample_upserts, peid, participant_sex_map[peid])
        )

    # Upsert participants
    for participant_upsert in participant_upserts:
        api_response = papi.upsert_participants(project, participant_upsert)
        print(api_response)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
