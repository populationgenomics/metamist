"""A Cloud Function to update the status of genomic samples.
Workflow is as follows
1. gVCFs and CRAMS moved.
2. Following successful move, analysis objects are created.
3. Once analysis objects are successfully created, seqencing metadata is updated."""

import logging
from os.path import join
from typing import Dict
from aiohttp import web
from google.cloud import storage
from sample_metadata.api.sample_api import SampleApi
from sample_metadata.api.sequence_api import SequenceApi
from sample_metadata.models.sequence_update_model import SequenceUpdateModel
from sample_metadata.models import SequencingStatus


routes = web.RouteTableDef()

FILES_HANDLED = {
    'gvcf': [{'primary': '.g.vcf.gz', 'secondary': ['.g.vcf.gz.tbi', '.g.vcf.gz.md5'], 'meta-field': 'gvcf'}],
    'cram': [{'primary': '.cram', 'secondary': ['.cram.crai', '.cram.md5'], 'meta-field': 'reads'}],
}


def create_file_object(external_id, analysis_type, project, batch):
    """ Creates CWL Dictionary """

    upload_bucket = f'cpg-{project}-upload'
    client = storage.Client()
    bucket = client.get_bucket(upload_bucket)

    output_types = FILES_HANDLED[analysis_type]

    file_objects = []

    for output_type in output_types:
        file_extension = output_type['primary']
        filename = external_id + file_extension
        md5_blob = bucket.get_blob(filename + '.md5')
        primary_blob = bucket.get_blob(filename)
        secondary_files = [external_id + f for f in output_type['secondary']]

        # Create the metadata object for the gvcf and for the cram.
        file_object = {
            'location': join('gs://', upload_bucket, filename),
            'basename': filename,
            'class': 'File',
            'checksum': md5_blob.download_as_string().decode(),
            'size': primary_blob.size,
            'secondaryFiles': secondary_files,
            'batch': batch,  # TODO: Check if this is ok here, or if it should be stored elsewhere.
        }

        file_objects.append(file_object)

    return file_objects


@routes.put('/upload_sample')
async def upload_sample(request):
    """ Registers file upload in Sample Metadata DB """

    sapi = SampleApi()

    request_json = await request.json()
    qc_metadata = request_json.get('metadata')
    external_id = request_json.get('sample')
    project = request_json.get('project')
    batch = str(request_json.get('batch'))
    status = request_json.get('status')

    if not project or not external_id or not status or not batch or not qc_metadata:
        raise web.HTTPBadRequest(
            reason=f'Invalid request. Ensure that the project, sample, status, batch and metadata are provided.'
        )

    # Determine the internal ID
    internal_id_map = sapi.get_sample_id_map_by_external(project, [external_id])
    internal_id = str(list(internal_id_map.values())[0])

    # TODO: Added the QC Metadata Here. Should this be moved to sample metadata?
    meta = {'qc_metrics': qc_metadata}
    for key in FILES_HANDLED:
        reads = create_file_object(external_id, key, project, batch)
        
        field_name = FILES_HANDLED[key]['meta-field']
        meta[field_name] = reads
        meta[field_name + '_type'] = key
        
    # Update the sequencing metadata.
    update_sequence_meta(project, internal_id, status, metadata)

    return web.Response(text=f'{external_id} lodged.')


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
