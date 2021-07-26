"""A Cloud Function to update the status of genomic samples.
Workflow is as follows
1. gVCFs and CRAMS concurrently moved.
2. Following successful move, analysis objects are created.
3. Once analysis objects are successfully created, seqencing metadata is updated."""

import json
import os
import logging
from os.path import join
from flask import abort

# disable=E0611
from google.cloud import secretmanager

# from requests.exceptions import HTTPError
import hailtop.batch as hb

from upload_processor.upload_processor import (
    batch_move_files,
    setup_python_job,
    create_analysis,
    SampleGroup,
    update_sequence_meta,
)


secret_manager = secretmanager.SecretManagerServiceClient()


def update_sample_status(request):
    """Main entry point for the Cloud Function."""

    if request.method != 'PUT':
        return abort(405)

    # Verify input parameters.
    request_json = request.get_json()
    project = request_json.get('project')
    external_id = request_json.get('sample')
    status = request_json.get('status')
    batch = request_json.get('batch')
    metadata = request_json.get('metadata')

    if not project or not external_id or not status or not batch or not metadata:
        return abort(400)

    # Set up additional inputs for batch_move
    upload_path = f'cpg-{project}-upload'
    # upload_prefix = ''
    # upload_path = join(upload_bucket, upload_prefix)
    # main_bucket = f'cpg-{project}-main'
    # main_prefix = join('gvcf', f'batch{batch}')
    main_path = join(f'cpg-{project}-main', 'gvcf', f'batch{batch}')
    # metadata_bucket = f'cpg-{project}-main-metadata' TODO: Add csv move step
    archive_path = join(f'cpg-{project}-archive', 'cram', f'batch{batch}')
    docker_image = os.environ.get('DRIVER_IMAGE')
    key = os.environ.get('GSA_KEY')

    logging.info(f'Processing request: {request_json}')

    # TODO: Authentication Step?
    # Fetch the per-project configuration from the Secret Manager.
    gcp_project = os.getenv('GCP_PROJECT')
    secret_name = (
        f'projects/{gcp_project}/secrets' '/update-sample-status-config/versions/latest'
    )
    config_str = secret_manager.access_secret_version(
        request={'name': secret_name}
    ).payload.data.decode('UTF-8')
    config = json.loads(config_str)

    project_config = config.get(project)
    if not project_config:
        return abort(404)

    # Set up batch
    service_backend = hb.ServiceBackend(
        billing_project=gcp_project,
        bucket=os.getenv('HAIL_BUCKET'),
    )


    batch = hb.Batch(name=f'Process {external_id}', backend=service_backend)

    # Move gVCFs
    sample_group_main = SampleGroup(
        external_id,
        f'{external_id}.g.vcf.gz',
        f'{external_id}.g.vcf.gz.tbi',
        f'{external_id}.g.vcf.gz.md5',
    )
    main_jobs = []
    main_jobs = batch_move_files(
        batch,
        sample_group_main,
        upload_path,
        main_path,
        project,
        docker_image,
        key,
    )

    # Create Analysis Object
    gvcf_analysis_job = setup_python_job(
        batch, f'Create gVCF Analysis {external_id}', docker_image, main_jobs
    )
    gvcf_analysis_job.call(
        create_analysis, sample_group_main, project, main_path, 'gvcf'
    )

    # Move crams
    sample_group_archive = SampleGroup(
        external_id,
        f'{external_id}.cram',
        f'{external_id}.cram.crai',
        f'{external_id}.cram.md5',
    )

    archive_jobs = []
    archive_jobs = batch_move_files(
        batch,
        sample_group_archive,
        upload_path,
        archive_path,
        project,
        docker_image,
        key,
    )

    # Create Analysis Object
    cram_analysis_job = setup_python_job(
        batch, f'Create cram Analysis {external_id}', docker_image, archive_jobs
    )
    cram_analysis_job.call(
        create_analysis, sample_group_archive, project, archive_path, 'cram'
    )

    # Update Sequence Metadata
    update_seq_meta = setup_python_job(
        batch,
        f'Update Meta {external_id}',
        docker_image,
        [gvcf_analysis_job, cram_analysis_job],
    )

    update_seq_meta.call(update_sequence_meta, external_id, project, status, metadata)

    batch.run()

    # if not response:  # Sample not found.
    #    return abort(404)

    return ('', 204)
