import json
import requests


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


def post_sm(project, module, name, data=None):
    """POST JSON and deserialise response from sample-metadata REST API"""
    resp = requests.post(_get_url(project, module, name), json=json.dumps(data))
    resp.raise_for_status()
    return resp.json()


def get_sm(project, module, name, query_params):
    """Get JSON response from sample-metadata REST API"""
    resp = requests.get(_get_url(project, module, name, query_params))
    resp.raise_for_status()
    return resp.json()
