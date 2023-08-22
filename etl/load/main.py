import json
import logging
import os
import base64
from typing import Dict, Any

import functions_framework
import flask
import google.cloud.bigquery as bq


BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE')

_BQ_CLIENT = bq.Client()


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

    if callable(jbody):
        # request.json is it in reality, but the type checker is saying it's callable
        jbody = jbody()

    request_id = extract_request_id(jbody)
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
    query_job_result = _BQ_CLIENT.query(query, job_config=job_config).result()

    if query_job_result.total_rows == 0:
        return {
            'success': False,
            'message': f'Record with id: {request_id} not found',
        }, 404

    # should be only one record, look into loading multiple objects in one call?
    row_json = None
    for row in query_job_result:
        # TODO
        # Parse row.body -> Model and upload to metamist database
        row_json = json.loads(row.body)

    return {'id': request_id, 'record': row_json, 'success': True}


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
