"""
A script to ingest a csv file of metadata for tob-wgs
The following script assumes a ParticipantUpdateModel that contains
reported_sex,reported_gender and karyotype as optional fields.

All other fields will be combined into the meta field.

"""

import csv
import io
import traceback

import click
from google.cloud import storage

from metamist.exceptions import ApiException
from metamist.apis import ParticipantApi
from metamist.model.participant_upsert import ParticipantUpsert


def update_metadata(
    participant_id: int,
    updated_participant: ParticipantUpsert,
):
    """Update participant level metadata in SM DB"""
    paapi = ParticipantApi()

    try:
        paapi.update_participant(
            participant_id=participant_id, participant_upsert=updated_participant
        )
    except ApiException:
        traceback.print_exc()
        print(f'Error updating participant {participant_id}, skipping.')


def get_csv(full_file_path: str):
    """Pull CSV file from GCS"""
    # Connect to GCS, retrieve CSV
    client = storage.Client()
    path_components = full_file_path[5:].split('/')
    bucket_name = path_components[0]
    file_path = '/'.join(path_components[1:])
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    csv_file = blob.download_as_bytes()
    return csv_file.decode(encoding='utf-8-sig')


def parse_csv(csv_string: str, project: str):
    """Pull & format data from CSV for each participant"""
    paapi = ParticipantApi()

    id_reader = csv.DictReader(io.StringIO(csv_string))
    sample_ids = [row['SampleID'] for row in id_reader]

    try:
        # For TOB-WGS, sample and participant IDs match.
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

        updated_participant = ParticipantUpsert()

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
