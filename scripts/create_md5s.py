import os

import click
from google.cloud import storage

from cpg_utils.hail_batch import copy_common_env, get_batch, get_config


def create_md5s_for_files_in_directory(skip_filetypes: tuple[str, str], force_recreate: bool, gs_dir):
    """Validate files with MD5s in the provided gs directory"""
    b = get_batch(f'Create md5 checksums for files in {gs_dir}')

    if not gs_dir.startswith('gs://'):
        raise ValueError(f'Expected GS directory, got: {gs_dir}')

    billing_project = get_config()['workflow']['gcp_billing_project']
    driver_image = get_config()['workflow']['driver_image']

    bucket_name, *components = gs_dir[5:].split('/')

    client = storage.Client()
    bucket = client.bucket(bucket_name, user_project=billing_project)
    blobs = bucket.list_blobs(prefix='/'.join(components))
    files: set[str] = {f'gs://{bucket_name}/{blob.name}' for blob in blobs}
    for filepath in files:
        if filepath.endswith('.md5') or filepath.endswith(skip_filetypes):
            continue
        if f'{filepath}.md5' in files and not force_recreate:
            print(f'{filepath}.md5 already exists, skipping')
            continue

        print('Creating md5 for', filepath)
        job = b.new_job(f'Create {os.path.basename(filepath)}.md5')
        create_md5(job, filepath, billing_project, driver_image)

    b.run(wait=False)


def create_md5(job, file, billing_project, driver_image):
    """
    Streams the file with gsutil and calculates the md5 checksum,
    then uploads the checksum to the same path as filename.md5.
    """
    copy_common_env(job)
    job.image(driver_image)
    md5 = f'{file}.md5'
    job.command(
        f"""\
    set -euxo pipefail
    gcloud -q auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS
    gsutil -u {billing_project} cat {file} | md5sum | cut -d " " -f1  > /tmp/uploaded.md5
    gsutil -u {billing_project} cp /tmp/uploaded.md5 {md5}
    """
    )

    return job


@click.command()
@click.option('--skip-filetypes', '-s', default=('.crai', '.tbi'), multiple=True)
@click.option('--force-recreate', '-f', is_flag=True, default=False)
@click.argument('gs_dir')
def main(skip_filetypes: tuple[str, str], force_recreate: bool, gs_dir: str):
    """Scans the directory for files and creates md5 checksums for them."""
    create_md5s_for_files_in_directory(skip_filetypes, force_recreate, gs_dir=gs_dir)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
