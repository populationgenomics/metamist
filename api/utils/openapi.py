from os import getenv
from typing import Literal

from fastapi.openapi.utils import get_openapi

from api.settings import SM_ENVIRONMENT

Json = dict[str | Literal['anyOf', 'type'], 'Json'] | list['Json'] | str | bool


URLS = []
if 'dev' in SM_ENVIRONMENT:
    URLS.append('https://sample-metadata-dev.populationgenomics.org.au')
    URLS.append('https://sample-metadata-api-dev-mnrpw3mdza-ts.a.run.app')

elif 'prod' in SM_ENVIRONMENT:
    URLS.append('https://sample-metadata.populationgenomics.org.au')
    URLS.append('https://sample-metadata-api-mnrpw3mdza-ts.a.run.app')
else:
    port = getenv('PORT', '8000')
    URLS.append(f'http://localhost:{port}')


def handle_any_of(yaml_dict: dict):
    """Handle anyOf in the yaml dict"""
    if 'anyOf' in yaml_dict and isinstance((any_of := yaml_dict['anyOf']), list):
        for i, item in enumerate(any_of):
            if isinstance(item, dict) and item.get('type') == 'null':
                any_of.pop(i)
                yaml_dict['nullable'] = True


def handle_examples(yaml_dict: dict):
    """Handle examples in the yaml dict"""
    if 'examples' in yaml_dict:
        examples = yaml_dict['examples']
        del yaml_dict['examples']
        if isinstance(examples, list) and len(examples):
            yaml_dict['example'] = examples[0]


def convert_3_dot_1_to_3_dot_0_inplace(json: dict[str, Json]):
    """
    Will attempt to convert version 3.1.0 of some openAPI json into 3.0.2
    Source:
        https://github.com/tiangolo/fastapi/discussions/9789#discussioncomment-8629746

    Usage:

        >>> from pprint import pprint
        >>> json = {
        ...     'some_irrelevant_keys': {...},
        ...     'nested_dict': {'nested_key': {'anyOf': [{'type': 'string'}, {'type': 'null'}]}},
        ...     'examples': [{...}, {...}]
        ... }
        >>> convert_3_dot_1_to_3_dot_0(json)
        >>> pprint(json)
        {'example': {Ellipsis},
         'nested_dict': {'nested_key': {'anyOf': [{'type': 'string'}],
                                        'nullable': True}},
         'openapi': '3.0.2',
         'some_irrelevant_keys': {Ellipsis}}
    """
    json['openapi'] = '3.0.2'

    def inner(yaml_dict: Json):
        if isinstance(yaml_dict, dict):
            handle_any_of(yaml_dict)
            handle_examples(yaml_dict)
            for value in yaml_dict.values():
                inner(value)
        elif isinstance(yaml_dict, list):
            for item in yaml_dict:
                inner(item)

    inner(json)


def get_openapi_schema_func(app, version):
    """Builds and returns a function that returns the openapi spec"""

    def openapi():
        if app.openapi_schema:
            return app.openapi_schema

        openapi_schema = get_openapi(
            title='Sample metadata API',
            version=version,
            routes=app.routes,
            # update when FastAPI + swagger supports 3.1.0
            openapi_version='3.0.2',
        )

        openapi_schema['servers'] = [{'url': url} for url in URLS]

        app.openapi_schema = openapi_schema
        return openapi_schema

    return openapi


def get_openapi_3_0_schema(app, version):
    """
    Uncouple the openapi schema of the server, for the OpenAPI generator
    (Because we use an old version that only supports 3.0 schema)

    """
    openapi_schema = get_openapi(
        title='Sample metadata API',
        version=version,
        routes=app.routes,
        # update when FastAPI + swagger supports 3.1.0
        # openapi_version='3.1.0'
    )

    convert_3_dot_1_to_3_dot_0_inplace(openapi_schema)

    openapi_schema['servers'] = [{'url': url} for url in URLS]

    app.openapi_schema = openapi_schema
    return openapi_schema
