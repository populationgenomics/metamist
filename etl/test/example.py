# This file contains example how to integrate with metamist ETL pipeline
import os

import google.auth.transport.requests
import google.oauth2.id_token
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

AUTH_URL = 'https://australia-southeast1-sample-metadata.cloudfunctions.net/metamist-etl-extract'
URL = 'https://metamist-extract.popgen.rocks'
TYPE = 'bbv/v1'


def make_request(body: dict | list):
    """
    Send a JSON serializable body to CPG's metadata server for ingestion.

    There is some basic detection for PII, and the server will return a 400 with an
    error message. If you see unexpected errors, please let the CPG team know.

    Each error message (except auth errors) contains an 'id' field, this is a UUID
    that can be used to track the request in the logs.

    A successful response will have the format: { 'success': True, 'id': 'UUID' }
    """
    # generate an auth token for the URL using GOOGLE_APPLICATION_CREDENTIALS
    auth_req = google.auth.transport.requests.Request()
    auth_token = google.oauth2.id_token.fetch_id_token(auth_req, AUTH_URL)

    # we'll retry 3 times, exponentially backing off (0.5, 1, 2 seconds)
    retry_strategy = Retry(
        total=3, backoff_factor=0.5, status_forcelist=[429, 502, 503, 504]
    )
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retry_strategy))

    resp = session.post(
        os.path.join(URL, TYPE),
        json=body,
        headers={'Authorization': f'Bearer {auth_token}'},
        timeout=30,
    )

    # you can handle this however you want, the request should return status codes:
    #   4xx something wrong with the request
    #   5xx something wrong on the server
    if not resp.ok:
        print(resp.text)
        resp.raise_for_status()

    # { 'id': 'UUID', 'success': True }
    return resp.json()


if __name__ == '__main__':
    # simple payload which would be sent to the server
    payload = {'first_name': 'This is a test to demonstrate PII detection'}
    # This will raise a 400 with an error message 'Potentially detected PII, the following keys were found in the body'
    make_request(payload)
