import os
from typing import Set

import click
from cpg_utils.hail_batch import get_config, remote_tmpdir, copy_common_env
import hailtop.batch as hb
from google.cloud import storage

DRIVER_IMAGE = 'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:latest'


def validate_all_objects_in_directory(gs_dir):
    """Validate files with MD5s in the provided gs directory"""
    
    backend = hb.ServiceBackend(
        billing_project=get_config()['hail']['billing_project'],
        remote_tmpdir=remote_tmpdir()
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
            #continue
            print('No md5 for', obj)

        job = b.new_job(f'validate_{os.path.basename(obj)}')
        copy_common_env(job)
        job.image(DRIVER_IMAGE)
        create_md5(job, obj)

    b.run(wait=False)


def create_md5(job: hb.batch.job, file) -> hb.batch.job:
    """
    Streams the file with gsutil and calculates the md5 checksum, 
    """

    # Calculate md5 checksum.
    md5 = f'{file}.md5'
    job.command('set -euxo pipefail')
    job.env('GOOGLE_APPLICATION_CREDENTIALS', '/gsa-key/key.json')
    job.command(
        f"""\
    gcloud -q auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS
    md5_hash=$(gsutil cat {file} | md5sum | cut -d " " -f1) 
    md5_command="echo '${{md5_hash}}  {file}' > /tmp/uploaded.md5"
    eval $md5_command
    gsutil cp /tmp/uploaded.md5 {md5}
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
