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
from google.cloud import pubsub_v1, secretmanager  # type: ignore

# strip whitespace, newlines and '/' for template matching
STRIP_CHARS = '/ \n'
BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE')
BIGQUERY_LOG_TABLE = os.getenv('BIGQUERY_LOG_TABLE')
NOTIFICATION_PUBSUB_TOPIC = os.getenv('NOTIFICATION_PUBSUB_TOPIC')
ETL_ACCESSOR_CONFIG_SECRET = os.getenv('CONFIGURATION_SECRET')

BQ_CLIENT = bq.Client()
SECRET_MANAGER = secretmanager.SecretManagerServiceClient()


class ParsingStatus:
    """
    Enum type to distinguish between sucess and failure of parsing
    """

    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'


def get_accessor_config() -> dict:
    """
    Read the secret from the full secret path: ETL_ACCESSOR_CONFIG_SECRET
    """
    response = SECRET_MANAGER.access_secret_version(
        request={'name': ETL_ACCESSOR_CONFIG_SECRET}
    )
    return json.loads(response.payload.data.decode('UTF-8'))


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
    bq_client: bq.Client,
) -> tuple[str, Any, Any]:
    """
    Process BQ results rows, should be only one row
    """
    source_type = bq_row.type
    # source_type should be in the format /ParserName/Version e.g.: /bbv/v1

    row_json = json.loads(bq_row.body)
    submitting_user = row_json['submitting_user']

    # get config from payload and merge with the default
    config = {}
    if payload_config_data := row_json.get('config'):
        config.update(payload_config_data)

    # get data from payload or use payload as data
    record_data = row_json.get('data', row_json)

    (parser_obj, err_msg) = get_parser_instance(
        submitting_user=submitting_user, source_type=source_type, init_params=config
    )

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

    # locate the request_id in bq
    query = f"""
        SELECT * FROM `{BIGQUERY_TABLE}` WHERE request_id = @request_id
    """
    query_params = [
        bq.ScalarQueryParameter('request_id', 'STRING', request_id),
    ]

    job_config = bq.QueryJobConfig()
    job_config.query_parameters = query_params
    query_job_result = BQ_CLIENT.query(query, job_config=job_config).result()

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
        bq_job_result, delivery_attempt, request_id, BQ_CLIENT
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
    submitting_user: str, source_type: str | None, init_params: dict | None
) -> tuple[object | None, str | None]:
    """Extract parser name from source_type

    Args:
        source_type (str | None): _description_

    Returns:
        object | None: _description_
    """
    if not source_type:
        return None, 'Empty source_type'

    # check that submitting_user has access to parser
    accessor_config = get_accessor_config()
    if submitting_user not in accessor_config:
        return None, (
            f'Submitting user {submitting_user} is not allowed to access any parsers'
        )

    found_parser = next(
        (
            parser
            for parser in accessor_config[submitting_user]
            if parser['name'].strip(STRIP_CHARS) == source_type.strip(STRIP_CHARS)
        ),
        None,
    )
    if not found_parser:
        return None, (
            f'Submitting user {submitting_user} is not allowed to access {source_type}'
        )

    init_params.update(found_parser.get('default_parameters', {}))
    _resolved_source_type = (found_parser.get('type_override') or source_type).strip(
        STRIP_CHARS
    )

    parser_map = prepare_parser_map()

    parser_class_ = parser_map.get(_resolved_source_type, None)
    if not parser_class_:
        # class not found
        return None, f'Parser for {_resolved_source_type} not found'

    try:
        parser_obj = parser_class_(**(init_params or {}))
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
        parser_map[f'{parser_short_name}/{parser_version}'] = parser_cls

    return parser_map
