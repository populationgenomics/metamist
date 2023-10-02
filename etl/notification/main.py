import base64
import json
import logging
import os
from typing import Any, Dict

import flask
import functions_framework
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN')
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL')


@functions_framework.http
def etl_notify(request: flask.Request):
    """HTTP Cloud Function for Sending notification to slack channel

    This cloud function is setup as subscriber to notification pub/sub topic
    Payload message is expected to be in json format and it is passed to slack channel as is
    TODO: We will add more formatting as we go along

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

    message = decode_message(jbody)
    if not message:
        jbody_str = json.dumps(jbody)
        return {
            'success': False,
            'message': f'Missing or empty message: {jbody_str}',
        }, 400

    # TODO: format message to slack message
    message_blocks = format_slack(message)
    success = None
    try:
        client = WebClient(token=SLACK_BOT_TOKEN)
        response = client.chat_postMessage(channel=SLACK_CHANNEL, blocks=message_blocks)
        success = response.get('ok') is True
    except SlackApiError as e:
        return {
            'success': False,
            'message': f'Got an error: {e.response["error"]}',
        }, 500

    if success:
        return {'success': True, 'message': 'Message sent'}

    return {'success': False, 'message': 'Failed to send message'}, 500


def format_slack(message: Dict[str, Any]) -> Any | None:
    """
    Basic Slack message formatting
    Message is json file, for time being we pass all the keys to the message
    If title present it will be used as title of the message
    """
    message_sections = []

    if 'title' in message:
        message_sections.append(
            {
                'type': 'header',
                'text': {'type': 'plain_text', 'text': f'{message.get("title")}'},
            }
        )
        message_sections.append({'type': 'divider'})

    for key, value in message.items():
        if key in ['title', 'status']:
            continue
        message_sections.append(
            {'type': 'section', 'text': {'type': 'mrkdwn', 'text': f'*{key}*: {value}'}}
        )

    return message_sections


def decode_message(jbody: Dict[str, Any]) -> Any | None:
    """Decode the message from payload

    Args:
        jbody (Dict[str, Any]): Json body of payload

    Returns:
        str | None: Decoded message
    """
    if not jbody:
        return None

    # try if payload is from pub/sub function
    message = jbody.get('message')
    if not message:
        return None

    data = message.get('data')
    if not data:
        return None

    # data is not empty, decode and extract request_id
    data_json = None
    try:
        data_decoded = base64.b64decode(data)
        data_json = json.loads(data_decoded)
    except Exception as e:  # pylint: disable=broad-exception-caught
        logging.error(f'Failed to extract request_id from the payload {e}')

    return data_json
