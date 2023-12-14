import os
from typing import Set

import click
from cpg_utils.hail_batch import get_batch, get_config, copy_common_env
from google.cloud import storage

DRIVER_IMAGE = 'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:latest'


def create_md5s_for_files_in_directory(skip_filetypes: tuple[str, str], gs_dir):
    """Validate files with MD5s in the provided gs directory"""
    b = get_batch(f'Create md5 checksums for files in {gs_dir}')

    if not gs_dir.startswith('gs://'):
        raise ValueError(f'Expected GS directory, got: {gs_dir}')

    billing_project = get_config()['hail']['billing_project']

    bucket_name, *components = gs_dir[5:].split('/')

    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix='/'.join(components))
    files: Set[str] = {f'gs://{bucket_name}/{blob.name}' for blob in blobs}
    for obj in files:
        if obj.endswith('.md5') or obj.endswith(skip_filetypes):
            continue
        if f'{obj}.md5' not in files:
            print('Creating md5 for', obj)

        job = b.new_job(f'Create {os.path.basename(obj)}.md5')
        copy_common_env(job)
        job.image(DRIVER_IMAGE)
        create_md5(job, obj, billing_project)

    b.run(wait=False)


def create_md5(job, file, billing_project):
    """
    Streams the file with gsutil and calculates the md5 checksum,
    then uploads the checksum to the same path as filename.md5.
    """
    md5 = f'{file}.md5'
    job.command('set -euxo pipefail')
    job.env('GOOGLE_APPLICATION_CREDENTIALS', '/gsa-key/key.json')
    job.command(
        f"""\
    gcloud -q auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS
    gsutil cat {file} | md5sum | cut -d " " -f1  > /tmp/uploaded.md5
    gsutil -u {billing_project} cp /tmp/uploaded.md5 {md5}
    """
    )

    return job


@click.command()
@click.option('--skip-filetypes', '-s', default=('.crai', '.tbi'), multiple=True)
@click.argument('gs_dir')
def main(skip_filetypes: tuple[str, str], gs_dir: str):
    """Scans the directory for files and creates md5 checksums for them."""
    create_md5s_for_files_in_directory(skip_filetypes, gs_dir=gs_dir)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
