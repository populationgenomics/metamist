# """A Cloud Function to update the status of genomic samples."""
# # type: ignore
# # flake8: noqa
# # pylint: disable
# import json
# import os
# import logging
# from typing import Dict, Any
# from airtable import Airtable
# from flask import abort
# from google.cloud import secretmanager
# from requests.exceptions import HTTPError
#
# from metamist.apis import SequenceApi, SampleApi
# from metamist.models import (
#     SequenceType,
#     SequenceUpdateModel,
#     SequenceStatus,
#     SampleType,
#     SampleUpsert,
#     AssayUpsert,
# )
# from metamist.exceptions import ForbiddenException, NotFoundException
#
# secret_manager = secretmanager.SecretManagerServiceClient()
#
#
# def update_airtable(project_config: dict, sample: str, status: str):
#     """Update sample status in AirTable"""
#     # Get the Airtable credentials.
#     base_key = project_config.get('baseKey')
#     table_name = project_config.get('tableName')
#     api_key = project_config.get('apiKey')
#     if not base_key or not table_name or not api_key:
#         return abort(500)
#
#     # Update the entry.
#     airtable = Airtable(base_key, table_name, api_key)
#     try:
#         response = airtable.update_by_field('Sample ID', sample, {'Status': status})
#     except HTTPError as err:  # Invalid status enum.
#         logging.error(err)
#         return abort(400, f'{status} is not a valid value')
#
#     if not response:  # Sample not found.
#         return abort(404, f'Could not find {sample} in AirTable')
#
#     return ('', 204)
#
#
# def update_sm(
#     project: str, sample: str, status: str, batch: str, eseqid: Dict[str, str]
# ):
#     """Update the status of a sequence given it's corresponding
#     external sample id"""
#
#     seqapi = SequenceApi()
#     sapi = SampleApi()
#     sample_type = SampleType('blood')
#     sample_meta: Dict[str, Any] = {}
#
#     sequence_info = {
#         'external_ids': eseqid,
#         'status': SequenceStatus(status),
#         'meta': {'batch': batch},
#         'type': SequenceType('genome'),
#     }
#
#     try:
#         # Try pulling sample id
#         existing_sample = sapi.get_sample_by_external_id(
#             external_id=sample, project=project
#         )
#         isid = existing_sample['id']
#
#     except NotFoundException as e:
#         # Otherwise create
#         logging.info(e)
#         new_sample = SampleUpsert(
#             external_id=sample,
#             type=sample_type,
#             meta=sample_meta,
#         )
#         isid = sapi.create_sample(new_sample=new_sample, project=project)
#
#     except ForbiddenException as e:
#         logging.error(e)
#         return abort(
#             403, f'Forbidden. Check that you have access to {project} in metamist'
#         )
#
#     try:
#         # Try update the sequence
#         seqid = seqapi.get_sequence_by_external_id(
#             project=project, external_id=list(eseqid.values())[0]
#         )
#         existing_sequence = SequenceUpdateModel(**sequence_info)
#         seqapi.update_sequence(
#             sequence_id=seqid['id'], sequence_update_model=existing_sequence
#         )
#
#     except NotFoundException as e:
#         logging.info(e)
#         # Otherwise create
#         sequence_info['sample_id'] = isid
#         new_sequence = NewSequence(**sequence_info)
#         seqapi.create_new_sequence(new_sequence=new_sequence)
#
#     except ForbiddenException as e:
#         logging.error(e)
#         return abort(
#             403,
#             f'Forbidden. Please check that you have access to {project} in metamist.',
#         )
#
#     return ('', 204)
#
#
# def update_sample_status(request):  # pylint: disable=R1710
#     """Main entry point for the Cloud Function."""
#
#     if request.method != 'PUT':
#         return abort(405)
#     # Verify input parameters.
#     request_json = request.get_json()
#     project = request_json.get('project')
#     sample = request_json.get('sample')
#     status = request_json.get('status')
#     batch = request_json.get('batch')
#     eseqid = request_json.get('sequence_id')
#     eseqid_dict = {'kccg': eseqid}
#
#     if not project or not sample or not status or not batch:
#         return abort(
#             400, f'Please check that project,sample,status and batch have been set.'
#         )
#
#     # Fetch the per-project configuration from the Secret Manager.
#     gcp_project = os.getenv('GCP_PROJECT')
#     assert gcp_project
#     secret_name = (
#         f'projects/{gcp_project}/secrets' '/update-sample-status-config/versions/latest'
#     )
#     config_str = secret_manager.access_secret_version(
#         request={'name': secret_name}
#     ).payload.data.decode('UTF-8')
#     config = json.loads(config_str)
#
#     project_config = config.get(project)
#     if not project_config:
#         return abort(404, f'Could not find project config.')
#
#     update_airtable(project_config, sample, status)
#
#     update_sm(project, sample, status, batch, eseqid_dict)
#
#     return ('', 204)
