import datetime
import json
import logging
import os
import uuid

import flask
import functions_framework
import google.cloud.bigquery as bq
from cpg_utils.cloud import email_from_id_token
from google.cloud import pubsub_v1  # type: ignore

BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE')
PUBSUB_TOPIC = os.getenv('PUBSUB_TOPIC')

_BQ_CLIENT = bq.Client()
_PUBSUB_CLIENT = pubsub_v1.PublisherClient()


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
    if not auth or not auth.token:
        return {'success': False, 'message': 'No auth token provided'}, 401

    request_id = str(uuid.uuid4())

    jbody = request.json
    if callable(jbody):
        # request.json is it in reality, but the type checker is saying it's callable
        jbody = jbody()

    jbody_str = json.dumps(jbody)

    pii_error = check_for_pii(jbody_str)
    if pii_error:
        return {
            'success': False,
            'id': request_id,
            'message': pii_error,
        }, 400

    bq_obj = {
        'request_id': request_id,
        'timestamp': datetime.datetime.utcnow().isoformat(),
        'type': request.path,
        'submitting_user': email_from_id_token(auth.token),
        'body': jbody_str,
    }

    # throw an exception if one occurs
    errors = _BQ_CLIENT.insert_rows_json(BIGQUERY_TABLE, [bq_obj])
    if errors:
        return {
            'success': False,
            'id': request_id,
            'message': f'An internal error occurred when processing this response: {errors}',
        }, 500

    # publish to pubsub
    try:
        _PUBSUB_CLIENT.publish(PUBSUB_TOPIC, json.dumps(bq_obj).encode())
    except Exception as e:  # pylint: disable=broad-exception-caught
        logging.error(f'Failed to publish to pubsub: {e}')

    return {'id': request_id, 'success': True}


def check_for_pii(text) -> str | None:
    """Check if text contains PII."""
    keys = [
        'first name',
        'first_name',
        'first-name',
        'last name',
        'last_name',
        'last-name',
        'email',
        'phone',
        'dob',
        'date of birth',
        'mobile',
    ]

    text_lower = text.lower()
    keys_in_text = [k for k in keys if k in text_lower]
    if keys_in_text:
        return (
            'Potentially detected PII, the following keys were found in the '
            f'body: {keys_in_text}'
        )

    return None
