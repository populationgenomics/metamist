import os

import click
from google.cloud import storage

from cpg_utils.hail_batch import config_retrieve, copy_common_env, get_batch

def validate_all_objects_in_directory(gs_dir, skip_filetypes: tuple[str, ...], billing_project: str = None):
    """Validate files with MD5s in the provided gs directory"""
    b = get_batch(f'Validate md5 checksums for files in {gs_dir}')
    client = storage.Client()
    
    if not billing_project:
        billing_project = config_retrieve(['workflow', 'gcp_billing_project'])
    driver_image = config_retrieve(['workflow', 'driver_image'])

    bucket_name, *components = gs_dir[5:].split('/')
    
    client = storage.Client()
    bucket = client.bucket(bucket_name, user_project=billing_project)
    blobs = bucket.list_blobs(prefix='/'.join(components))
    files: set[str] = {f'gs://{bucket_name}/{blob.name}' for blob in blobs}
    for obj in files:
        if obj.endswith('.md5') or obj.endswith(skip_filetypes):
            continue
        if f'{obj}.md5' not in files:
            continue
        
        print('Validating md5 for', obj)
        job = b.new_job(f'Validate md5 checksum: {os.path.basename(obj)}')
        validate_md5(job, obj, billing_project, driver_image)

    b.run(wait=False)

def create_md5s_for_files_in_directory(gs_dir, skip_filetypes: tuple[str, ...], force_recreate: bool, billing_project: str = None):
    """Validate files with MD5s in the provided gs directory"""
    b = get_batch(f'Create md5 checksums for files in {gs_dir}')

    if not billing_project:
        billing_project = config_retrieve(['workflow', 'gcp_billing_project'])
    driver_image = config_retrieve(['workflow', 'driver_image'])

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


def validate_md5(job, file, billing_project, driver_image):
    """
    This quickly validates a file and it's md5
    """
    copy_common_env(job)
    job.image(driver_image)
    md5_path = f'{file}.md5'
    job.command(
        f"""\
    set -euxo pipefail
    gcloud -q auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS
    calculated_md5=$(gsutil -u {billing_project} cat {file} | md5sum | cut -d " " -f1)
    stored_md5=$(gsutil -u {billing_project} cat {md5_path} | cut -d " " -f1)
    
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
@click.option('--billing-project', '-b', default=None)
@click.option('--skip-filetypes', '-s', default=('.crai', '.tbi'), multiple=True)
@click.option('--validate-only', '-v', is_flag=True, default=False, help='Validate existing md5s, do not create new ones.')
@click.option('--force-recreate', '-f', is_flag=True, default=False)
@click.argument('gs_dir')
def main(billing_project: str | None, skip_filetypes: tuple[str, str], validate_only: bool, force_recreate: bool, gs_dir: str):
    """Scans the directory for files and creates md5 checksums for them."""
    if not gs_dir.startswith('gs://'):
        raise ValueError(f'Expected GS directory, got: {gs_dir}')
    
    if validate_only:
        validate_all_objects_in_directory(gs_dir, skip_filetypes, billing_project)
    else:
        create_md5s_for_files_in_directory(gs_dir, skip_filetypes, force_recreate, billing_project)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
