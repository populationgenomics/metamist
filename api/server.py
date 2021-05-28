# pylint: disable=assigning-non-slot,disable=unused-variable
import os
from enum import Enum
from inspect import isclass

from flask import Flask, request, g, jsonify
from google.auth.exceptions import DefaultCredentialsError

# openapi3 and swagger
from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec_webframeworks.flask import FlaskPlugin
from flask_swagger_ui import get_swaggerui_blueprint

from db.python.connect import SMConnections
import models.enums as sm_enums

from api.routes import all_blueprints
from api.utils.handleexception import handle_exception
from api.utils.request import get_email_from_request_headers_or_raise_auth_error


# Retrieves a Cloud Logging handler based on the environment
# you're running in and integrates the handler with the
# Python logging module. By default this captures all logs
# at INFO level and higher
try:
    # pylint: disable=import-outside-toplevel,c-extension-no-member
    import google.cloud.logging

    client = google.cloud.logging.Client()
    client.get_default_handler()
    client.setup_logging()

except DefaultCredentialsError as exp:
    if not bool(os.getenv('SM_IGNORE_GCP_CREDENTIALS_ERROR')):
        raise exp

API_PREFIX = '/api/v1/'

# This tag is automatically updated by bump2version
_VERSION = '1.0.0'


def create_app():
    """Create an return flask app"""
    app = Flask('cpg_sample_metadata')

    spec = APISpec(
        title='Sample metadata API',
        version=_VERSION,
        openapi_version='3.0.2',
        plugins=[FlaskPlugin(), MarshmallowPlugin()],
        servers=[{'url': 'http://localhost:5000'}],
        security=[{'bearerAuth': []}],
    )

    spec.components.security_scheme('bearerAuth', {'type': 'http', 'scheme': 'bearer'})
    enums = [
        e
        for e in sm_enums.__dict__.values()
        if isclass(e) and issubclass(e, Enum) and e != Enum
    ]
    for e in enums:
        spec.components.schema(
            e.__name__,
            {
                'type': 'string',
                'enum': [member.value for role, member in e.__members__.items()],
            },
        )

    @app.errorhandler(Exception)
    def exception_handler(e):
        """
        Source: https://flask.palletsprojects.com/en/1.1.x/errorhandling/#generic-exception-handlers
        """
        return handle_exception(e)

    @app.before_request
    def before_request():
        """
        Before request, check that the request is authenticated,
        and get the email associated with the google account.
        """

        if request.view_args and request.view_args.get('project'):
            project = request.view_args.get('project')
            # could check project permissions here
            g.author = get_email_from_request_headers_or_raise_auth_error(
                request.headers
            )
            g.connection = SMConnections.get_connection_for_project(project, g.author)

    # add routes (before swagger)
    for bp in all_blueprints:
        app.register_blueprint(bp(API_PREFIX))

    # register routes with apispec
    with app.test_request_context():
        for rule in app.url_map.iter_rules():
            view = app.view_functions[rule.endpoint]
            spec.path(view=view)

    # add openapi schema endpoint
    schema_endpoint = '/api/schema.json'

    @app.route(schema_endpoint, methods=['GET'])
    def get_schema():
        return jsonify(spec.to_dict()), 200

    # add swagger ui
    app.register_blueprint(get_swaggerui_blueprint('/api/docs', schema_endpoint))

    return app


api_app = create_app()


def start_app():
    """Start web app"""

    host = os.getenv('SM_HOST', 'localhost')
    port = int(os.getenv('PORT', '5000'))
    api_app.run(host=host, port=port, debug=True)


if __name__ == '__main__':
    start_app()
