import asyncio
import base64
import datetime
import json
import logging
import os
from typing import Any

import flask
import functions_framework
import google.cloud.bigquery as bq
import pkg_resources
from google.cloud import pubsub_v1  # type: ignore

BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE')
BIGQUERY_LOG_TABLE = os.getenv('BIGQUERY_LOG_TABLE')
NOTIFICATION_PUBSUB_TOPIC = os.getenv('NOTIFICATION_PUBSUB_TOPIC')
DEFAULT_LOAD_CONFIG = os.getenv('DEFAULT_LOAD_CONFIG', '{}')


class ParsingStatus:
    """
    Enum type to distinguish between sucess and failure of parsing
    """

    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'


def call_parser(parser_obj, row_json) -> tuple[str, str]:
    """
    This function calls parser_obj.from_json and returns status and result
    """
    tmp_res: list[str] = []
    tmp_status: list[str] = []

    # GenericMetadataParser from_json is async
    # we call it from sync, so we need to wrap it in coroutine
    async def run_parser_capture_result(parser_obj, row_data, res, status):
        try:
            # TODO better error handling
            r = await parser_obj.from_json([row_data], confirm=False, dry_run=True)
            res.append(r)
            status.append(ParsingStatus.SUCCESS)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logging.error(f'Failed to parse: {e}')
            # add to the output
            res.append(f'Failed to parse: {e}')
            status.append(ParsingStatus.FAILED)

    asyncio.run(run_parser_capture_result(parser_obj, row_json, tmp_res, tmp_status))
    return tmp_status[0], tmp_res[0]


def process_rows(
    bq_row: bq.table.Row,
    delivery_attempt: int,
    request_id: str,
    parser_map: dict,
    bq_client: bq.Client,
) -> tuple[str, Any, Any]:
    """
    Process BQ results rows, should be only one row
    """
    source_type = bq_row.type
    # source_type should be in the format /ParserName/Version e.g.: /bbv/v1

    row_json = json.loads(bq_row.body)

    # get config from payload and merge with the default
    config_data = json.loads(DEFAULT_LOAD_CONFIG)
    payload_config_data = row_json.get('config')
    if payload_config_data:
        config_data.update(payload_config_data)

    # get data from payload or use payload as data
    record_data = row_json.get('data', row_json)

    (parser_obj, err_msg) = get_parser_instance(parser_map, source_type, config_data)

    if parser_obj:
        # Parse bq_row.body -> Model and upload to metamist database
        status, parsing_result = call_parser(parser_obj, record_data)
    else:
        status = ParsingStatus.FAILED
        parsing_result = f'Error: {err_msg} when parsing record with id: {request_id}'

    if delivery_attempt == 1:
        # log only at the first attempt,
        # pub/sub min try is 5x and we do not want to spam slack with errors
        log_details = {
            'source_type': source_type,
            'submitting_user': bq_row.submitting_user,
            'result': str(parsing_result),
        }

        # log results to BIGQUERY_LOG_TABLE
        log_record = {
            'request_id': request_id,
            'timestamp': datetime.datetime.utcnow().isoformat(),
            'status': status,
            'details': json.dumps(log_details),
        }

        if BIGQUERY_LOG_TABLE is None:
            logging.error('BIGQUERY_LOG_TABLE is not set')
            return status, parsing_result, row_json

        errors = bq_client.insert_rows_json(
            BIGQUERY_LOG_TABLE,
            [log_record],
        )
        if errors:
            logging.error(f'Failed to log to BQ: {errors}')

        if NOTIFICATION_PUBSUB_TOPIC is None:
            logging.error('NOTIFICATION_PUBSUB_TOPIC is not set')
            return status, parsing_result, row_json

        if status == ParsingStatus.FAILED:
            # publish to notification pubsub
            msg_title = 'Metamist ETL Load Failed'
            try:
                pubsub_client = pubsub_v1.PublisherClient()
                pubsub_client.publish(
                    NOTIFICATION_PUBSUB_TOPIC,
                    json.dumps({'title': msg_title} | log_record).encode(),
                )
            except Exception as e:  # pylint: disable=broad-exception-caught
                logging.error(f'Failed to publish to pubsub: {e}')

    return status, parsing_result, row_json


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

    # use delivery_attempt to only log error once, min number of attempts for pub/sub is 5
    # so we want to avoid 5 records / slack messages to be passed around per one error
    delivery_attempt, request_id = extract_request_id(jbody)
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
    parser_map = prepare_parser_map()

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
        # Request ID not found
        return {
            'success': False,
            'message': f'Record with id: {request_id} not found',
        }, 412  # Precondition Failed

    if query_job_result.total_rows > 1:
        # This should never happen, Request ID should be unique
        return {
            'success': False,
            'message': f'Multiple Records with the same id: {request_id}',
        }, 412  # Precondition Failed

    # will be exactly one row (request), row can contain multiple records (samples)
    bq_job_result = list(query_job_result)[0]

    (status, parsing_result, uploaded_record) = process_rows(
        bq_job_result, delivery_attempt, request_id, parser_map, bq_client
    )

    # return success
    if status == ParsingStatus.SUCCESS:
        return {
            'id': request_id,
            'record': uploaded_record,
            'result': str(parsing_result),
            'success': True,
        }, 200

    # otherwise return error
    return {
        'id': request_id,
        'record': uploaded_record,
        'result': str(parsing_result),
        'success': False,
    }, 400


def extract_request_id(jbody: dict[str, Any]) -> tuple[int | None, str | None]:
    """Unwrapp request id from the payload

    Args:
        jbody (dict[str, Any]): Json body of payload

    Returns:
        str | None: ID of object to be loaded
    """
    if not jbody:
        return None, None

    request_id = jbody.get('request_id')
    # set default delivery_attempt as 1
    delivery_attempt = int(jbody.get('deliveryAttempt', 1))
    if request_id:
        return delivery_attempt, request_id

    # try if payload is from pub/sub function
    message = jbody.get('message')
    if not message:
        return delivery_attempt, None

    data = message.get('data')
    if not data:
        return delivery_attempt, None

    # data is not empty, decode and extract request_id
    request_id = None
    try:
        data_decoded = base64.b64decode(data)
        data_json = json.loads(data_decoded)
        request_id = data_json.get('request_id')
    except Exception as e:  # pylint: disable=broad-exception-caught
        logging.error(f'Failed to extract request_id from the payload {e}')

    return delivery_attempt, request_id


def get_parser_instance(
    parser_map: dict, source_type: str | None, init_params: dict | None
) -> tuple[object | None, str | None]:
    """Extract parser name from source_type

    Args:
        source_type (str | None): _description_

    Returns:
        object | None: _description_
    """
    if not source_type:
        return None, 'Empty source_type'

    parser_class_ = parser_map.get(source_type, None)
    if not parser_class_:
        # class not found
        return None, f'Parser for {source_type} not found'

    # TODO: in case of generic metadata parser, we need to create instance
    try:
        if init_params:
            parser_obj = parser_class_(**init_params)
        else:
            parser_obj = parser_class_()
    except Exception as e:  # pylint: disable=broad-exception-caught
        logging.error(f'Failed to create parser instance {e}')
        return None, f'Failed to create parser instance {e}'

    return parser_obj, None


def prepare_parser_map() -> dict:
    """Prepare parser map
    loop through metamist_parser entry points and create map of parsers
    """
    parser_map = {}
    for entry_point in pkg_resources.iter_entry_points('metamist_parser'):
        parser_cls = entry_point.load()
        parser_short_name, parser_version = parser_cls.get_info()
        parser_map[f'/{parser_short_name}/{parser_version}'] = parser_cls

    return parser_map
