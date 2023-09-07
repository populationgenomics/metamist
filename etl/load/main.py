import asyncio
import base64
import datetime
import json
import logging
import os
from typing import Any, Dict

import flask
import functions_framework
import google.cloud.bigquery as bq
import metamist.parser as mp

BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE')
BIGQUERY_LOG_TABLE = os.getenv('BIGQUERY_LOG_TABLE')

DEFAULT_PARSER_PARAMS = {
    'search_locations': [],
    'project': 'milo-dev',
    'participant_column': 'individual_id',
    'sample_name_column': 'sample_id',
    'seq_type_column': 'sequencing_type',
    'default_sequencing_type': 'sequencing_type',
    'default_sample_type': 'blood',
    'default_sequencing_technology': 'short-read',
    'sample_meta_map': {
        'collection_centre': 'centre',
        'collection_date': 'collection_date',
        'collection_specimen': 'specimen',
    },
    'participant_meta_map': {},
    'assay_meta_map': {},
    'qc_meta_map': {},
}


def call_parser(parser_obj, row_json):
    """_summary_

    Args:
        parser_obj (_type_): _description_
        row_json (_type_): _description_

    Returns:
        _type_: _description_
    """
    tmp_res = []
    tmp_status = []

    # GenericMetadataParser from_json is async
    # we call it from sync, so we need to wrap it in coroutine
    async def run_parser_capture_result(parser_obj, row_data, res, status):
        try:
            # TODO better error handling
            r = await parser_obj.from_json([row_data], confirm=False, dry_run=True)
            res.append(r)
            status.append('SUCCESS')
        except Exception as e:  # pylint: disable=broad-exception-caught
            logging.error(f'Failed to parse the row {e}')
            # add to the output
            res.append(e)
            status.append('FAILED')

    asyncio.run(run_parser_capture_result(parser_obj, row_json, tmp_res, tmp_status))
    return tmp_status[0], tmp_res[0]


@functions_framework.http
def etl_load(request: flask.Request):
    """HTTP Cloud Function for ETL loading records from BQ to MySQL DB

    This function accepts Pub/Sub push messages or can be called directly
    For direct call request_id of BQ record to load is required, e.g. payload:

    {
        "request_id": "70eb6292-6311-44cf-9c9b-2b38bb076699"
    }

    or Pub/Sub push messages are wrapped,
    atm Pulumi does not support unwrapping:
    https://github.com/pulumi/pulumi-gcp/issues/1142

    Example of Pub/Sub push payload, function only parse message.data

    {
        'message': {'data': 'eyJyZXF1ZXN0X2lkIjo ... LTViMWQt21pY3Mub3JnLmF1In0='
    }

    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    Note:
        For more information on how Flask integrates with Cloud
        Functions, see the `Writing HTTP functions` page.
        <https://cloud.google.com/functions/docs/writing/http#http_frameworks>

    """

    auth = request.authorization
    if not auth or not auth.token:
        return {'success': False, 'message': 'No auth token provided'}, 401

    # if mimetype might not be set esp. when PubSub pushing from another topic,
    # try to force conversion and if fails just return None
    jbody = request.get_json(force=True, silent=True)

    request_id = extract_request_id(jbody)
    if not request_id:
        jbody_str = json.dumps(jbody)
        return {
            'success': False,
            'message': f'Missing or empty request_id: {jbody_str}',
        }, 400

    # prepare parser map
    # parser_map = {
    #     'gmp/v1': <class 'metamist.parser.generic_metadata_parser.GenericMetadataParser'>,
    #     'sfmp/v1': <class 'metamist.parser.sample_file_map_parser.SampleFileMapParser'>,
    #     'bbv/v1': bbv.BbvV1Parser, TODO: add bbv parser
    # }
    parser_map = prepare_parser_map(mp.GenericParser, default_version='v1')

    # locate the request_id in bq
    query = f"""
        SELECT * FROM `{BIGQUERY_TABLE}` WHERE request_id = @request_id
    """
    query_params = [
        bq.ScalarQueryParameter('request_id', 'STRING', request_id),
    ]

    bq_client = bq.Client()
    job_config = bq.QueryJobConfig()
    job_config.query_parameters = query_params
    query_job_result = bq_client.query(query, job_config=job_config).result()

    if query_job_result.total_rows == 0:
        return {
            'success': False,
            'message': f'Record with id: {request_id} not found',
        }, 412  # Precondition Failed

    # should be only one record, look into loading multiple objects in one call?
    row_json = None
    result = None
    status = None
    for row in query_job_result:
        sample_type = row.type
        # sample_type should be in the format /ParserName/Version e.g.: /bbv/v1

        row_json = json.loads(row.body)
        # get config from payload or use default
        config_data = row_json.get('config', DEFAULT_PARSER_PARAMS)
        # get data from payload or use payload as data
        record_data = row_json.get('data', row_json)

        parser_obj = get_parser_instance(parser_map, sample_type, config_data)
        if not parser_obj:
            return {
                'success': False,
                'message': f'Missing or invalid sample_type: {sample_type} in the record with id: {request_id}',
            }, 412  # Precondition Failed

        # Parse row.body -> Model and upload to metamist database
        status, result = call_parser(parser_obj, record_data)

        # log results to BIGQUERY_LOG_TABLE
        bq_client.insert_rows_json(
            BIGQUERY_LOG_TABLE,
            [
                {
                    'request_id': request_id,
                    'timestamp': datetime.datetime.utcnow().isoformat(),
                    'status': status,
                    'details': json.dumps({'result': f"'{result}'"}),
                }
            ],
        )

    if status and status == 'SUCCESS':
        return {
            'id': request_id,
            'record': row_json,
            'result': f"'{result}'",
            'success': True,
        }

    # some error happened
    return {
        'id': request_id,
        'record': row_json,
        'result': f"'{result}'",
        'success': False,
    }, 500


def extract_request_id(jbody: Dict[str, Any]) -> str | None:
    """Unwrapp request id from the payload

    Args:
        jbody (Dict[str, Any]): Json body of payload

    Returns:
        str | None: ID of object to be loaded
    """
    if not jbody:
        return None

    request_id = jbody.get('request_id')
    if request_id:
        return request_id

    # try if payload is from pub/sub function
    message = jbody.get('message')
    if not message:
        return None

    data = message.get('data')
    if not data:
        return None

    # data is not empty, decode and extract request_id
    request_id = None
    try:
        data_decoded = base64.b64decode(data)
        data_json = json.loads(data_decoded)
        request_id = data_json.get('request_id')
    except Exception as e:  # pylint: disable=broad-exception-caught
        logging.error(f'Failed to extract request_id from the payload {e}')

    return request_id


def get_parser_instance(
    parser_map: dict, sample_type: str | None, init_params: dict | None
) -> object | None:
    """Extract parser name from sample_type

    Args:
        sample_type (str | None): _description_

    Returns:
        object | None: _description_
    """
    if not sample_type:
        return None

    parser_class_ = parser_map.get(sample_type, None)
    if not parser_class_:
        # class not found
        return None

    # TODO: in case of generic metadata parser, we need to create instance
    try:
        if init_params:
            parser_obj = parser_class_(**init_params)
        else:
            parser_obj = parser_class_()
    except Exception as e:  # pylint: disable=broad-exception-caught
        logging.error(f'Failed to create parser instance {e}')
        return None

    return parser_obj


def all_subclasses(cls: object) -> set:
    """Recursively find all subclasses of cls"""
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)]
    )


def prepare_parser_map(cls, default_version='v1'):
    """Prepare parser map for the given class

    Args:
        cls: class to find subclasses of
        version: version of the parser
    Returns:
        parser map
    """
    parser_map = {}
    for parser_cls in all_subclasses(cls):
        parser_code = ''.join(
            [ch for ch in str(parser_cls).rsplit('.', maxsplit=1)[-1] if ch.isupper()]
        )
        parser_map[f'/{parser_code.lower()}/{default_version}'] = parser_cls

    return parser_map
