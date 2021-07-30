"""A Cloud Function to update the status of genomic samples.
Workflow is as follows
1. gVCFs and CRAMS concurrently moved.
2. Following successful move, analysis objects are created.
3. Once analysis objects are successfully created, seqencing metadata is updated."""

import json
import os
import logging
from os.path import join
from flask import Flask, abort, request
from sample_metadata.api.sample_api import SampleApi

import hailtop.batch as hb
import logging
from aiohttp import web

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

routes = web.RouteTableDef()
secret_manager = secretmanager.SecretManagerServiceClient()


@routes.put('/update_sample_status')
async def update_sample_status(request):
    """Main entry point for the Cloud Function."""
    sapi = SampleApi()

    if request.method != 'PUT':
        return abort(405)

    # Verify input parameters.
    request_json = await request.json()
    project = request_json.get('project')
    external_id = request_json.get('sample')
    status = request_json.get('status')
    batch = request_json.get('batch')
    metadata = request_json.get('metadata')

    if not project or not external_id or not status or not batch or not metadata:
        return abort(400)

    # Set up additional inputs for batch_move
    # upload_path = f'cpg-{project}-upload'
    # main_path = join(f'cpg-{project}-main', 'gvcf', f'batch{batch}')
    # # metadata_bucket = f'cpg-{project}-main-metadata' TODO: Add csv move step
    # archive_path = join(f'cpg-{project}-archive', 'cram', f'batch{batch}')
    upload_path = f'vivian-dev-upload'
    main_path = join(f'vivian-dev-main', 'gvcf', f'batch{batch}')
    metadata_bucket = f'vivian-dev-main-metadata'  # TODO: Add csv move step
    archive_path = join(f'vivian-dev-archive', 'cram', f'batch{batch}')
    docker_image = os.environ.get('DRIVER_IMAGE')
    key = os.environ.get('GSA_KEY')

    logging.info(f'Processing request: {request_json}')

    # TODO: Authentication Step?
    # Fetch the per-project configuration from the Secret Manager.
    gcp_project = os.getenv('GCP_PROJECT')

    # Do we still need this?
    # secret_name = (
    #    f'projects/{gcp_project}/secrets' '/update-sample-status-config/versions/latest'
    # )
    # config_str = secret_manager.access_secret_version(
    #    request={'name': secret_name}
    # ).payload.data.decode('UTF-8')
    # config = json.loads(config_str)

    # project_config = config.get(project)
    # if not project_config:
    #    return abort(404)

    # Set up batch
    # service_backend = hb.ServiceBackend(
    #     billing_project=gcp_project,
    #     bucket=os.getenv('HAIL_BUCKET'),
    # )
    service_backend = hb.ServiceBackend(
        billing_project='vivianbakiris-trial',
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

    external_id = {'external_ids': [sample_group_main.sample_id_external]}
    internal_id_map = sapi.get_sample_id_map_by_external(project, external_id)
    internal_id = str(list(internal_id_map.values())[0])

    main_jobs = []
    main_jobs = batch_move_files(
        batch,
        sample_group_main,
        upload_path,
        main_path,
        project,
        internal_id,
        docker_image,
        key,
    )

    # Create Analysis Object
    gvcf_analysis_job = setup_python_job(
        batch, f'Create gVCF Analysis {external_id}', docker_image, main_jobs
    )
    gvcf_analysis_job.call(
        create_analysis, sample_group_main, project, internal_id, main_path, 'gvcf'
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
        internal_id,
        docker_image,
        key,
    )

    # Create Analysis Object
    cram_analysis_job = setup_python_job(
        batch, f'Create cram Analysis {external_id}', docker_image, archive_jobs
    )
    cram_analysis_job.call(
        create_analysis,
        sample_group_archive,
        project,
        internal_id,
        archive_path,
        'cram',
    )

    # Update Sequence Metadata
    update_seq_meta = setup_python_job(
        batch,
        f'Update Meta {external_id}',
        docker_image,
        [gvcf_analysis_job, cram_analysis_job],
    )

    update_seq_meta.call(update_sequence_meta, external_id, project, status, metadata)

    # Process the CSV afterward.

    batch.run()

    # if not response:  # Sample not found.
    #    return abort(404)

    return web.Response(text=f'Done!')


@routes.put('/test_function')
async def test_function(request):

    external_id = 'viviandevtest1'
    sapi = SampleApi()

    docker_image = os.environ.get('DRIVER_IMAGE')
    key = os.environ.get('GSA_KEY')

    main_jobs = []
    project = 'viviandev'
    upload_path = f'vivian-dev-upload'
    main_path = join(f'vivian-dev-main', 'gvcf', f'batch0')

    sample_group_main = SampleGroup(
        external_id,
        f'{external_id}.cram',
        f'{external_id}.cram.crai',
        f'{external_id}.cram.md5',
    )

    external_id = {'external_ids': [sample_group_main.sample_id_external]}
    internal_id_map = sapi.get_sample_id_map_by_external(project, external_id)
    internal_id = str(list(internal_id_map.values())[0])

    service_backend = hb.ServiceBackend(
        billing_project='vivianbakiris-trial',
        bucket=os.getenv('HAIL_BUCKET'),
    )

    batch = hb.Batch(name=f'Process {external_id}', backend=service_backend)

    # main_jobs = batch_move_files(
    #     batch,
    #     sample_group_main,
    #     upload_path,
    #     main_path,
    #     project,
    #     internal_id,
    #     docker_image,
    #     key,
    # )

    # gvcf_analysis_job = setup_python_job(
    #     batch, f'Create gVCF Analysis viviandevtest1', docker_image, main_jobs
    # )
    # gvcf_analysis_job.call(
    #     create_analysis, sample_group_main, project, internal_id, main_path, 'gvcf'
    # )

    # Get the curl URL
    j = batch.new_job(name=f'Testing curl')

    if docker_image is not None:
        j.image(docker_image)

    # Authenticate to service account.
    if key is not None:
        j.command(f"echo '{key}' > /tmp/key.json")
        j.command(f'gcloud -q auth activate-service-account --key-file=/tmp/key.json')
    # Handles service backend, or a key in the same default location.
    else:
        j.command(
            'gcloud -q auth activate-service-account --key-file=/gsa-key/key.json'
        )

    id_test = "viviandevtest1"

    data = {
        "sample_ids": [internal_id],
        "type": "gvcf",
        "status": "completed",
        "output": "gs://",
    }

    y = json.dumps(data)

    logging.info(data)
    curl = f"curl -X \'PUT\' \
        \'https://sample-metadata-api-mnrpw3mdza-ts.a.run.app/api/v1/viviandev/analysis/\' \
        -H \'accept: application/json\' \
        -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" \
        -H \'Content-Type: application/json\' \
        -d \'{y}\'"

    j.command(curl)

    batch.run()

    return web.Response(text=f'Done!')


async def init_func():
    """Initializes the app."""
    app = web.Application()
    logging.basicConfig(level=logging.DEBUG)
    logging.info('Initialising Application')
    app.add_routes(routes)
    return app


if __name__ == '__main__':
    web.run_app(init_func())
