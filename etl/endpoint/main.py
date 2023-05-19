import datetime
import os
import functions_framework
import flask
import google.cloud.bigquery as bq

from cpg_utils.cloud import email_from_id_token

BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE')
PUBSUB_TOPIC = os.getenv('PUBSUB_TOPIC')

ALLOWED_USERS = os.getenv('ALLOWED_USERS').split(',')

_BQ_CLIENT = bq.Client()


@functions_framework.http
def etl_post(request: flask.Request):
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
    """

    auth = request.authorization
    user = 'unknown'
    auth_error = None
    if not auth:
        auth_error = f'Unknown auth mechanism: {request.headers}'
    elif token := auth.token:
        auth_error = email_from_id_token(token)
    else:
        user = f'No auth token, header fields: {request.headers}'

    bq_obj = {
        'timestamp': datetime.datetime.utcnow().isoformat(),
        'type': 'UNKNOWN',
        'submitting_user': user,
        'body': request.json(),
    }
    error = None
    try:
        _BQ_CLIENT.insert_rows(BIGQUERY_TABLE, [bq_obj])
    except Exception as e:  # pylint: disable=broad-except
        error = str(e)

    return {
        'success': error is not None,
        'queue': PUBSUB_TOPIC,
        'bigquery-table': BIGQUERY_TABLE,
        'url': request.url,
        'route': request.access_route,
        'bq-row': bq_obj,
        'user': user or auth_error,
    }
