import json
import logging
import os
import functions_framework
import flask
import google.cloud.bigquery as bq

BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE')

_BQ_CLIENT = bq.Client()


@functions_framework.http
def etl_load(request: flask.Request):
    """HTTP Cloud Function.
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

    This function accepts Pub/Sub message, expected request_id in the payload:

    {
        "request_id": "70eb6292-6311-44cf-9c9b-2b38bb076699"
    }

    At the moment pulumi does not support unwrapping of push messages:
    https://github.com/pulumi/pulumi-gcp/issues/1142

    We need to support both
    """

    auth = request.authorization
    if not auth or not auth.token:
        return {'success': False, 'message': 'No auth token provided'}, 401

    logging.info(f'auth {auth}')

    # if mimetype might not be set esp. when PubSub pushing from another topic,
    # try to force conversion and if fails just return None
    jbody = request.get_json(force=True, silent=True)

    logging.info(f'jbody: {jbody}')

    if callable(jbody):
        # request.json is it in reality, but the type checker is saying it's callable
        jbody = jbody()

    request_id = jbody.get('request_id', None)
    # check if request_id present
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
    query_job = _BQ_CLIENT.query(query, job_config=job_config).result()

    # should be only one record, look into loading multiple objects in one call?
    for row in query_job:
        # TODO
        # Parse row.body -> Model and upload to metamist database
        row_body = json.loads(row.body)
        logging.info(f'row_body {row_body}')

    return {'id': request_id, 'success': True}
