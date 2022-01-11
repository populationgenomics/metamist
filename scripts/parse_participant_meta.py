"""
A script to ingest a csv file of metadata for tob-wgs
The following script assumes a ParticipantUpdateModel that contains
reported_sex,reported_gender and karyotype as optional fields.

All other fields will be combined into the meta field.

"""

import csv
import io
from os import path
import traceback

import click

from google.cloud import storage

from sample_metadata.apis import ParticipantApi
from sample_metadata.models import ParticipantUpdateModel
from sample_metadata import ApiException


def update_metadata(
    participant_id: int,
    updated_participant: ParticipantUpdateModel,
):
    """Update participant level metadata in SM DB"""
    paapi = ParticipantApi()

    try:
        paapi.update_participant(
            participant_id=participant_id, participant_update_model=updated_participant
        )
    except ApiException:
        traceback.print_exc()
        print(f'Error updating participant {participant_id}, skipping.')


def get_csv(full_file_path: str):
    """Pull CSV file from GCS"""
    # Connect to GCS, retrieve CSV
    client = storage.Client()
    bucket_name = path.dirname(full_file_path)[5:]
    filename = path.basename(full_file_path)
    bucket = client.get_bucket(bucket_name)
    blobs = list(client.list_blobs(bucket))
    blob = next((r for r in blobs if r.name == filename), None)
    csv_file = blob.download_as_string()
    return csv_file.decode(encoding='utf-8-sig')


def parse_csv(csv_string: str, project: str):
    """Pull & format data from CSV for each participant"""
    paapi = ParticipantApi()

    id_reader = csv.DictReader(io.StringIO(csv_string))
    sample_ids = [row['SampleID'] for row in id_reader]

    try:
        id_map = paapi.get_participant_id_map_by_external_ids(
            project=project, request_body=sample_ids
        )
    except ApiException:
        traceback.print_exc()
        return

    meta_reader = csv.DictReader(io.StringIO(csv_string))

    for row in meta_reader:
        row = {k.lower(): v for k, v in row.items()}
        sample_id = row.pop('sampleid')

        updated_participant = ParticipantUpdateModel()

        if 'reported_gender' in row:
            reported_gender = row.pop('reported_gender')
            updated_participant['reported_gender'] = reported_gender
        if 'reported_sex' in row:
            reported_sex = row.pop('reported_sex')
            updated_participant['reported_sex'] = reported_sex
        if 'karyotype' in row:
            karyotype = row.pop('karyotype')
            updated_participant['karyotype'] = karyotype

        updated_participant['meta'] = row

        update_metadata(id_map[sample_id], updated_participant)


@click.command()
@click.argument('project')
@click.argument('full_file_path')
def main(project: str, full_file_path: str):
    """Run Script"""
    csv_file = get_csv(full_file_path)
    parse_csv(csv_file, project)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
