from os import getenv
from fastapi.openapi.utils import get_openapi

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
