from os import getenv
import pydantic
import pydantic.schema

from fastapi.openapi.utils import get_openapi
from pydantic.fields import ModelField

env = getenv('SM_ENVIRONMENT', 'local').lower()
URLS = []
if 'dev' in env:
    URLS.append('https://sample-metadata-dev.populationgenomics.org.au')
    URLS.append('https://sample-metadata-api-dev-mnrpw3mdza-ts.a.run.app')

elif 'prod' in env:
    URLS.append('https://sample-metadata.populationgenomics.org.au')
    URLS.append('https://sample-metadata-api-mnrpw3mdza-ts.a.run.app')
else:
    port = getenv('PORT', '8000')
    URLS.append(f'http://localhost:{port}')


def get_openapi_schema_func(app, version):
    """Builds and returns a function that returns the openapi spec"""
    pydantic_field_type_schema = pydantic.schema.field_type_schema

    # pydantic not handling null fields
    # https://github.com/pydantic/pydantic/issues/1270#issuecomment-1209704699
    def field_type_schema(field: ModelField, **kwargs):
        """
        Override default field_type_schema to hack pydantic into supporting
        nullable field types.
        """
        f_schema, definitions, nested_models = pydantic_field_type_schema(
            field, **kwargs
        )
        if not field.allow_none:
            return f_schema, definitions, nested_models

        s_type = f_schema.get('type')
        if s_type:
            # Hack to detect whether we are generating for openapi
            # fastapi sets the ref_prefix to '#/components/schemas/'.
            # When using for openapi, swagger does not seem to support an array
            # for type, so use anyOf instead.
            if kwargs.get('ref_prefix') == '#/components/schemas/':
                f_schema = {'anyOf': [f_schema, {'type': None}]}
            else:
                if not isinstance(s_type, list):
                    f_schema['type'] = [s_type]
                f_schema['type'].append({'type': None})

        elif '$ref' in f_schema:
            # This case causes awkward UNKNOWNBASETYPE
            # The metamist API doesn't use nullable schema,
            # it's mostly nullable fields on a schema, so we'll ignore this case.
            # https://github.com/OpenAPITools/openapi-generator/issues/12404
            # f_schema['anyOf'] = [
            #     {**f_schema},
            #     {'type': None},
            # ]
            # del f_schema['$ref']
            pass

        elif 'allOf' in f_schema:
            f_schema['anyOf'] = f_schema['allOf']
            del f_schema['allOf']
            f_schema['anyOf'].append({'type': None})

        elif 'anyOf' in f_schema or 'oneOf' in f_schema:
            one_or_any = f_schema.get('anyOf') or f_schema.get('oneOf')
            for item in one_or_any:
                if item.get('type') == 'null' or item.get('type') is None:
                    break
            else:
                one_or_any.append({'type': None})

        return f_schema, definitions, nested_models

    pydantic.schema.field_type_schema = field_type_schema

    def openapi():
        if app.openapi_schema:
            return app.openapi_schema

        openapi_schema = get_openapi(
            title='Sample metadata API',
            version=version,
            routes=app.routes,
            # update when FastAPI + swagger supports 3.1.0
            # openapi_version='3.1.0'
        )

        openapi_schema['servers'] = [{'url': url} for url in URLS]

        app.openapi_schema = openapi_schema
        return openapi_schema

    return openapi
