import os
from typing import Set

import click
import hailtop.batch as hb
from google.cloud import storage

DRIVER_IMAGE = 'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:8cc869505251c8396fefef01c42225a7b7930a97-hail-0.2.73.devc6f6f09cec08'


def validate_all_objects_in_directory(gs_dir):
    """Validate files with MD5s in the provided gs directory"""
    backend = hb.ServiceBackend(
        billing_project=os.getenv('HAIL_BILLING_PROJECT'),
        bucket=os.getenv('HAIL_BUCKET'),
    )
    b = hb.Batch('validate_md5s', backend=backend)
    client = storage.Client()

    if not gs_dir.startswith('gs://'):
        raise ValueError(f'Expected GS directory, got: {gs_dir}')

    bucket_name, *components = gs_dir[5:].split('/')

    blobs = client.list_blobs(bucket_name, prefix='/'.join(components))
    files: Set[str] = {f'gs://{bucket_name}/{blob.name}' for blob in blobs}
    for obj in files:
        if obj.endswith('.md5'):
            continue
        if f'{obj}.md5' not in files:
            continue

        job = b.new_job(f'validate_{os.path.basename(obj)}')
        job.image(DRIVER_IMAGE)
        validate_md5(job, obj)

    b.run(wait=False)


def validate_md5(job: hb.batch.job, file, md5_path=None) -> hb.batch.job:
    """
    This quickly validates a file and it's md5
    """

    # Calculate md5 checksum.
    md5 = md5_path or f'{file}.md5'
    job.env('GOOGLE_APPLICATION_CREDENTIALS', '/gsa-key/key.json')
    job.command(
        f"""\
gcloud -q auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS
gsutil cat {file} | md5sum | cut -d " " -f1 > /tmp/uploaded.md5
diff <(cat /tmp/uploaded.md5) <(gsutil cat {md5} | cut -d " " -f1)
    """
    )

    return job


@click.command()
@click.argument('gs_dir')
def main(gs_dir):
    """Main from CLI"""
    validate_all_objects_in_directory(gs_dir=gs_dir)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
