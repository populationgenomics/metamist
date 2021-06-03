from fastapi.openapi.utils import get_openapi


def get_openapi_schema_func(app, version, is_production):
    """Builds and returns a function that returns the openapi spec"""

    def openapi():
        if app.openapi_schema:
            return app.openapi_schema

        openapi_schema = get_openapi(
            title='Sample metadata API',
            version=version,
            routes=app.routes,
        )

        servers = []
        if is_production:
            servers.append(
                {'url': 'https://sample-metadata-api-mnrpw3mdza-ts.a.run.app'}
            )
        else:
            servers.append({'url': 'http://localhost:8000'})

        openapi_schema['servers'] = servers

        app.openapi_schema = openapi_schema
        return openapi_schema

    return openapi
