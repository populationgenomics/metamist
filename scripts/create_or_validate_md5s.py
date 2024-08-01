import os
from shlex import quote

import click
from google.cloud import storage

from cpg_utils.hail_batch import config_retrieve, copy_common_env, get_batch


def get_blobs_in_directory(gs_dir: str, billing_project: str, storage_client: storage.Client):
    """Get all blobs in a directory as a set of full gs:// URIs"""
    bucket_name, *components = gs_dir[5:].split('/')
    bucket = storage_client.bucket(bucket_name, user_project=billing_project)
    blobs = bucket.list_blobs(prefix='/'.join(components))
    return {f'gs://{bucket_name}/{blob.name}' for blob in blobs}


def create_md5(job, filepath: str, billing_project: str, driver_image: str):
    """
    Streams the file with gsutil and calculates the md5 checksum,
    then uploads the checksum to the same path as filename.md5.
    """
    copy_common_env(job)
    job.image(driver_image)
    md5_filepath = f'{filepath}.md5'
    job.command(
        f"""\
    set -euxo pipefail
    gsutil -u {quote(billing_project)} cat {quote(filepath)} | md5sum | cut -d " " -f1  > /tmp/uploaded.md5
    gsutil -u {quote(billing_project)} cp /tmp/uploaded.md5 {quote(md5_filepath)}
    """
    )

    return job


def validate_md5(job, filepath: str, billing_project: str, driver_image: str):
    """
    Streams the file with gsutil and calculates the md5 checksum,
    then compares it to the stored checksum in the .md5 file.
    """
    copy_common_env(job)
    job.image(driver_image)
    md5_filepath = f'{filepath}.md5'
    job.command(
        f"""\
    set -euxo pipefail
    calculated_md5=$(gsutil -u {quote(billing_project)} cat {quote(filepath)} | md5sum | cut -d " " -f1)
    stored_md5=$(gsutil -u {quote(billing_project)} cat {quote(md5_filepath)} | cut -d " " -f1)

    if [ "$calculated_md5" = "$stored_md5" ]; then
        echo "MD5 checksum validation successful."
        exit 0
    else
        echo "MD5 checksum validation failed."
        echo "Calculated MD5: $calculated_md5"
        echo "Stored MD5:     $stored_md5"
        exit 1
    fi
    """
    )

    return job


def create_and_validate_md5s_for_files_in_directory(
    gs_dir: str,
    skip_filetypes: tuple[str, ...],
    mode: str,
    force_recreate_existing: bool,
    billing_project: str,
    driver_image: str,
    storage_client: storage.Client
):
    """
    Create OR validate MD5s for files in the provided gs directory, skipping files with certain extensions.
    The mode parameter determines whether to create or validate the md5s.
    If mode=create, the force_recreate parameter determines whether to re-create md5s that already exist.
    """
    b = get_batch(f'{mode} md5 checksums for files in {gs_dir}')

    files = get_blobs_in_directory(gs_dir, billing_project, storage_client)
    for filepath in files:
        if filepath.endswith('.md5') or filepath.endswith(skip_filetypes):
            continue

        md5_exists = f'{filepath}.md5' in files

        if mode == 'create' :
            if not md5_exists or force_recreate_existing:
                print('Creating md5 for', filepath)
                job = b.new_job(f'Create {os.path.basename(filepath)}.md5')
                create_md5(job, filepath, billing_project, driver_image)
            else:
                print(f'{filepath}.md5 already exists, skipping')
                continue

        if mode == 'validate':
            if md5_exists:
                print('Validating md5 for', filepath)
                job = b.new_job(f'Validate md5 checksum: {os.path.basename(filepath)}')
                validate_md5(job, filepath, billing_project, driver_image)
            else:
                print(f'{filepath}.md5 not found, skipping validation')

    b.run(wait=False)


@click.command()
@click.option('--billing-project', '-b', default=None)
@click.option('--skip-filetypes', '-s', default=('.crai', '.tbi'), multiple=True)
@click.option('--mode', '-m', type=click.Choice(['create', 'validate']), default='create', help='Whether to validate existing md5s or create new ones.')
@click.option('--force-recreate-existing', '-f', is_flag=True, default=False, help='Re-create md5s even if they already exist.')
@click.argument('gs_dir')
def main(
    billing_project: str | None,
    skip_filetypes: tuple[str, str],
    mode: str,
    force_recreate_existing: bool,
    gs_dir: str
):
    """
    Scans the directory for files and creates md5 checksums for them, OR validates existing md5s.
    params:
        gs_dir:                   The GCP bucket path / directory to scan for files.
        skip_filetypes:           File extensions to skip.
        mode:                     'create' md5s for files, or 'validate' existing md5s.
        force_recreate_existing:  Re-create md5s even if they already exist.
        billing_project:          The GCP project to bill for the operations.

    """
    if not gs_dir.startswith('gs://'):
        raise ValueError(f'Expected GS directory, got: {gs_dir}')

    if not billing_project:
        billing_project = config_retrieve(['workflow', 'dataset_gcp_project'])
    driver_image = config_retrieve(['workflow', 'driver_image'])

    storage_client = storage.Client()

    create_and_validate_md5s_for_files_in_directory(
        gs_dir=gs_dir,
        skip_filetypes=skip_filetypes,
        mode=mode,
        force_recreate_existing=force_recreate_existing,
        billing_project=billing_project,
        driver_image=driver_image,
        storage_client=storage_client,
    )


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
