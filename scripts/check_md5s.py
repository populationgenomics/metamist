import os
from typing import Set

import click
import hailtop.batch as hb
from google.cloud import storage


def validate_all_objects_in_directory(gs_dir, billing_project):
    """Validate files with MD5s in the provided gs directory"""
    backend = hb.ServiceBackend(billing_project=billing_project)
    b = hb.Batch('validate_md5s', backend=backend)
    client = storage.Client()

    if not gs_dir.startswith('gs://'):
        raise ValueError(f'Expected GS directory, got: {gs_dir}')

    bucket_name, *components = gs_dir[5:].split('/')

    blobs = client.list_blobs(bucket_name, prefix='/'.join(components))
    files: Set[str] = {f'gs://{bucket_name}/{blob.name}' for blob in blobs}
    # TODO: remove this
    dev_counter = 0
    for obj in files:
        if obj.endswith('.md5'):
            continue
        if f'{obj}.md5' not in files:
            continue
        if dev_counter >= 2:
            break

        dev_counter += 1
        validate_md5(b.new_job(f'validate_{os.path.basename(obj)}'), obj)

    b.run(wait=False)


def validate_md5(job: hb.batch.job, file, md5_path=None) -> hb.batch.job:
    """
    This quickly validates a file and it's md5
    """

    # Calculate md5 checksum.
    job.command(f'gsutil cat {file} | md5sum | cut -d " " -f1 > /tmp/uploaded.md5')

    md5 = md5_path or f'{file}.md5'
    job.command(f'diff <(cat /tmp/uploaded.md5) <(gsutil cat {md5} | cut -d " " -f1 )')

    return job


@click.command()
@click.argument('gs_dir')
@click.argument('billing_project')
def main(gs_dir, billing_project):
    """Main from CLI"""
    validate_all_objects_in_directory(gs_dir=gs_dir, billing_project=billing_project)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
