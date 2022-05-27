""" A really simple script to import a pedigree file """

import click
from google.cloud import storage

from sample_metadata.apis import (
    FamilyApi,
)


@click.command()
@click.option('--ped-file-path', 'ped_file_path')
@click.option('--bucket', 'bucket_name')
@click.option('--project', 'project')
def main(ped_file_path: str, bucket_name: str, project: str):
    """Pull ped file from GCS and import to SM"""

    fapi = FamilyApi()
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(ped_file_path)
    ped_file = blob.download_as_bytes()

    tmp_ped_tsv = 'tmp_ped.tsv'
    # Work-around as import_pedigree takes a file.
    with open(tmp_ped_tsv, 'wb') as tmp_ped:
        tmp_ped.write(ped_file)

    with open(tmp_ped_tsv) as ped_file:
        fapi.import_pedigree(
            file=ped_file,
            has_header=True,
            project=project,
            create_missing_participants=True,
        )


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
