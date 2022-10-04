"""A Cloud Function to update the status of genomic samples."""

import json
import os
import logging
from typing import Optional

from airtable import Airtable
from flask import abort
from google.cloud import secretmanager
from requests.exceptions import HTTPError

from sample_metadata.apis import SampleApi, SequenceApi
from sample_metadata.model.body_get_sequences_by_criteria import (
    BodyGetSequencesByCriteria,
)
from sample_metadata.model.new_sample import NewSample
from sample_metadata.model.new_sequence import NewSequence
from sample_metadata.models import (
    SequenceType,
    SequenceUpdateModel,
    SequenceStatus,
    SampleType,
)
from sample_metadata.exceptions import ForbiddenException

secret_manager = secretmanager.SecretManagerServiceClient()
seqapi = SequenceApi()


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


def update_sm(project: str, external_sample_id: str, status: str, batch: str):
    """Update the status of a sequence given it's corresponding external sample id"""

    samapi = SampleApi()
    sequence_type = SequenceType('genome')
    sample_type = SampleType('blood')
    seq_meta = {'batch': batch}
    external_sequence_id: Optional[str] = None

    sid_map = samapi.get_sample_id_map_by_internal([external_sample_id])
    if sid_map and external_sample_id in sid_map:
        internal_sample_id = sid_map[external_sample_id]
    else:
        samapi.create_new_sample(
            project=project,
            new_sample=NewSample(
                external_id=external_sample_id,
                type=sample_type,
            ),
        )

        # should create here if it doesn't exist
        return abort(
            f'Could not find sample with external ID {external_sample_id}', 404
        )

    try:
        # 2022-10-03 mfranklin: Get the internal sequence ID here
        sequence_id: Optional[int] = get_sequence_id_from(
            project=project,
            external_sequence_id=external_sequence_id,
            internal_sample_id=internal_sample_id,
        )

        if sequence_id:
            seqapi.update_sequence(
                sequence_id,
                SequenceUpdateModel(status=SequenceStatus(status), meta=seq_meta),
            )
        else:
            external_ids = {}
            if external_sequence_id:
                external_ids = {'unknown?': external_sequence_id}

            seqapi.create_new_sequence(
                NewSequence(
                    external_ids=external_ids,
                    sample_id=internal_sample_id,
                    meta=seq_meta,
                    status=SequenceStatus(status),
                    type=sequence_type,
                )
            )

    except ValueError as e:
        logging.error(e)
        return abort(400)

    except ForbiddenException as e:
        logging.error(e)
        return abort(403)

    return ('', 204)


def get_sequence_id_from(
    project: str, internal_sample_id: Optional[str], external_sequence_id: Optional[str]
) -> Optional[int]:
    """
    From project + external_sequence_id OR internal_sample_id,
    GET the latest relevant sequence ID or None if not found (meaning insert)
    """
    if external_sequence_id:
        try:
            sequence = seqapi.get_sequence_by_external_id(
                project=project, external_id=external_sequence_id
            )
            return sequence['id']
        except Exception as exception:  # pylint: disable=broad-except
            # TODO: Check that it's a 404
            # error here means it doesn't exist ...
            print(exception)

    # fall back to get by internal_sample_id, and get latest
    sequences = seqapi.get_sequences_by_criteria(
        body_get_sequences_by_criteria=BodyGetSequencesByCriteria(
            sample_ids=[internal_sample_id],
        )
    )
    # crude, but it's how the old implementation worked
    if sequences:
        return max(seq['id'] for seq in sequences)
    return None


def update_sample_status(request):  # pylint: disable=R1710
    """Main entry point for the Cloud Function."""

    if request.method != 'PUT':
        return abort(405)
    # Verify input parameters.
    request_json = request.get_json()
    project = request_json.get('project')
    sample = request_json.get('sample')
    status = request_json.get('status')
    batch = request_json.get('batch')

    if not project or not sample or not status or not batch:
        return abort(400)

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
