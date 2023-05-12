import os
import functions_framework
import flask

BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE')
PUBSUB_TOPIC = os.getenv('PUBSUB_TOPIC')

ALLOWED_USERS = os.getenv('ALLOWED_USERS').split(',')


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
    return {
        'success': True,
        'queue': PUBSUB_TOPIC,
        'bigquery-table': BIGQUERY_TABLE,
        'url': request.url,
        'route': request.access_route,
    }
