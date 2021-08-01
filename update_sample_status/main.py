"""A Cloud Function to update the status of genomic samples.
Workflow is as follows
1. gVCFs and CRAMS moved.
2. Following successful move, analysis objects are created.
3. Once analysis objects are successfully created, seqencing metadata is updated."""

import os
import logging
import subprocess
from os.path import join
from typing import Dict
from aiohttp import web
from sample_metadata.api.sample_api import SampleApi
from sample_metadata.api.sequence_api import SequenceApi
from sample_metadata.api.analysis_api import AnalysisApi
from sample_metadata.models.analysis_model import AnalysisModel
from sample_metadata.models.sequence_update_model import SequenceUpdateModel
from sample_metadata.models import AnalysisType, AnalysisStatus, SequencingStatus


routes = web.RouteTableDef()


@routes.put('/upload_sample')
async def upload_sample(request):
    """ Upload Sample Workflow """

    # Define inputs
    sapi = SampleApi()
    gcp_project = os.getenv('GCP_PROJECT')

    # Verify input parameters.
    request_json = await request.json()
    project = request_json.get('project')
    external_id = request_json.get('sample')
    status = request_json.get('status')
    batch = request_json.get('batch')
    metadata = request_json.get('metadata')

    if not project or not external_id or not status or not batch or not metadata:
        raise web.HTTPBadRequest(
            reason=f'Invalid request. Ensure that the project, sample, status, batch and metadata are provided.'
        )

    upload_path = f'cpg-{gcp_project}-upload'
    main_path = join(f'cpg-{gcp_project}-main', 'gvcf', f'batch{batch}')
    # metadata_bucket = f'cpg-{gcp_project}-main-metadata'
    archive_path = join(f'cpg-{gcp_project}-archive', 'cram', f'batch{batch}')

    # Determine the internal ID
    external_id_dict = {'external_ids': [external_id]}
    internal_id_map = sapi.get_sample_id_map_by_external(project, external_id_dict)
    internal_id = str(list(internal_id_map.values())[0])

    # Files going to main
    main_extensions = ['.g.vcf.gz', '.g.vcf.gz.tbi', '.g.vcf.gz.md5']
    archive_extensions = ['.cram', '.cram.crai', '.cram.md5']
    all_extensions = main_extensions + archive_extensions

    # Move the files
    for file_extension in all_extensions:
        new_file_name = internal_id + file_extension
        original_file_name = external_id + file_extension
        previous_location = join('gs://', upload_path, original_file_name)

        if file_extension in main_extensions:
            new_location = join('gs://', main_path, new_file_name)
        else:
            new_location = join('gs://', archive_path, new_file_name)

        # Check file doesn't exist at destination
        exists = subprocess.run(['gsutil', '-q', 'stat', new_location], check=True)

        if exists.returncode == 1:
            try:
                subprocess.check_output(
                    [
                        'gsutil',
                        'mv',
                        previous_location,
                        new_location,
                    ],
                )
            except subprocess.CalledProcessError as e:
                logging.error(e)
                raise web.HTTPBadRequest(
                    reason=f'{original_file_name} was not found in {upload_path}'
                )

            # create_analysis(project, internal_id, new_location, file_extension)

        else:
            logging.info(f'File already exists at destination. Move skipped.')

    # Update the sequencing metadata.
    # update_sequence_meta(project, internal_id, status, metadata)

    return web.Response(text=f'Processed {external_id} -> {internal_id}')


@routes.put('/update_status')
async def update_status(request):
    """ Handles status update, e.g. QC Complete, Sequenced """
    logging.info(request)
    # TODO


def create_analysis(proj, internal_id, file_path: str, file_extension: str):
    """ Create analysis object in SM DB"""
    aapi = AnalysisApi()

    if file_extension == '.g.vcf.gz':
        a_type = 'gvcf'
    elif file_extension == '.cram':
        a_type = 'cram'
    else:
        logging.info(f'Analysis object not required for {file_extension} file')
        return

    new_analysis = AnalysisModel(
        sample_ids=[internal_id],
        type=AnalysisType(a_type),
        status=AnalysisStatus('completed'),
        output=file_path,
    )

    aapi.create_new_analysis(proj, new_analysis)
    logging.info(f'Analysis object created for {internal_id}.{file_extension}')


def update_sequence_meta(project: str, internal_id: str, status: str, metadata: Dict):
    """ Updates sequence metadata in SM DB"""
    seqapi = SequenceApi()

    # Determine sequencing ID
    sequence_id = seqapi.get_sequence_id_from_sample_id(internal_id, project)

    # Create sequence metadata object
    sequence_metadata = SequenceUpdateModel(
        status=SequencingStatus(status), meta=metadata
    )  # pylint: disable=E1120

    # Update db
    seqapi.update_sequence(sequence_id, sequence_metadata)


async def init_func():
    """Initializes the app."""
    app = web.Application()
    logging.basicConfig(level=logging.DEBUG)
    logging.info('Initialising Application')
    app.add_routes(routes)
    return app


if __name__ == '__main__':
    web.run_app(init_func())
