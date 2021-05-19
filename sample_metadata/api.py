import json
import requests

import google.auth
import google.auth.transport.requests

HOST = 'http://localhost:5000'
URL_STRUCTURE = '{host}/api/v1/{project}/{module}/{name}'


def _get_url(project, module, method_name, query_params=None):
    q = ''
    if query_params:
        q = '?' + '&'.join(f'{k}={v}' for k, v in query_params.items())

    return (
        URL_STRUCTURE.format(
            host=HOST, project=project, module=module, name=method_name
        )
        + q
    )


def _get_google_auth_token() -> str:
    # https://stackoverflow.com/a/55804230
    # command = ['gcloud', 'auth', 'print-identity-token']

    creds, _ = google.auth.default()

    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    return creds.id_token


def _prepare_headers():
    _token = _get_google_auth_token()
    return {'Authorization': f'Bearer {_token}'}


def post_sm(project, module, name, data=None):
    """POST JSON and deserialise response from sample-metadata REST API"""
    resp = requests.post(
        _get_url(project, module, name),
        json=json.dumps(data),
        headers=_prepare_headers(),
    )
    resp.raise_for_status()
    return resp.json()


def get_sm(project, module, name, query_params):
    """Get JSON response from sample-metadata REST API"""
    resp = requests.get(
        _get_url(project, module, name, query_params), headers=_prepare_headers()
    )
    resp.raise_for_status()
    return resp.json()
