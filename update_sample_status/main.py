"""A Cloud Function to update the status of genomic samples."""

import json
import os
import logging
from airtable import Airtable
from flask import abort
from google.cloud import secretmanager
from requests.exceptions import HTTPError

from sample_metadata.apis import SequenceApi
from sample_metadata.models import (
    SequenceType,
    SequenceUpdateModel,
    SequenceStatus,
    SampleType,
)
from sample_metadata.exceptions import ForbiddenException

secret_manager = secretmanager.SecretManagerServiceClient()


def update_airtable(project_config: dict, sample: str, status: str):
    """Update sample status in AirTable"""
    # Get the Airtable credentials.
    base_key = project_config.get('baseKey')
    table_name = project_config.get('tableName')
    api_key = project_config.get('apiKey')
    if not base_key or not table_name or not api_key:
        return abort(500)

    # Update the entry.
    airtable = Airtable(base_key, table_name, api_key)
    try:
        response = airtable.update_by_field('Sample ID', sample, {'Status': status})
    except HTTPError as err:  # Invalid status enum.
        logging.error(err)
        return abort(400)

    if not response:  # Sample not found.
        return abort(404)

    return ('', 204)


def update_sm(project: str, sample: str, status: str, batch: str):
    """Update the status of a sequence given it's corresponding
    external sample id"""

    seqapi = SequenceApi()
    sequence_type = SequenceType('genome')
    sample_type = SampleType('blood')
    seq_meta = {'batch': batch}

    try:
        sequence_model = SequenceUpdateModel(
            status=SequenceStatus(status), meta=seq_meta
        )
        seqapi.upsert_sequence_from_external_sample_and_type(
            external_sample_id=sample,
            sequence_type=sequence_type,
            sequence_update_model=sequence_model,
            project=project,
            sample_type=sample_type,
        )
    except ValueError as e:
        logging.error(e)
        return abort(400)

    except ForbiddenException as e:
        logging.error(e)
        return abort(403)

    return ('', 204)


def update_sample_status(request):  # pylint: disable=R1710
    """Main entry point for the Cloud Function."""

    if request.method != 'PUT':
        return abort(405)
    logging.info(f'Recieved request {request}')
    # Verify input parameters.
    request_json = request.get_json()
    project = request_json.get('project')
    sample = request_json.get('sample')
    status = request_json.get('status')
    batch = request_json.get('batch')
    logging.info(
        f'Input paramaters project={project}, sample={sample}. status={status}'
    )
    if not project or not sample or not status:
        return abort(400)

    logging.info(f'Processing request: {request_json}')

    # Fetch the per-project configuration from the Secret Manager.
    gcp_project = os.getenv('GCP_PROJECT')
    assert gcp_project
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

    update_airtable(project_config, sample, status)

    update_sm(project, sample, status, batch)

    return ('', 204)
