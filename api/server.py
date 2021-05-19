# pylint: disable=assigning-non-slot

from flask import Flask, request, g
from flasgger import Swagger

from db.python.connect import SMConnections
from api.routes import all_blueprints
from api.utils.handleexception import handle_exception
from api.utils.request import get_email_from_request_headers_or_raise_auth_error


API_PREFIX = '/api/v1/'


def create_app():
    """Create an return flask app"""
    app = Flask('cpg_sample_metadata')

    # pylint: disable=unused-variable
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
        g.author = get_email_from_request_headers_or_raise_auth_error(request.headers)

        if request.view_args and request.view_args.get('project'):
            project = request.view_args.get('project')
            # could check project permissions here
            g.connection = SMConnections.get_connection_for_project(project, g.author)

    # add routes (before swagger)
    for bp in all_blueprints:
        app.register_blueprint(bp(API_PREFIX))

        # add swagger
        app.config['SWAGGER'] = {'title': 'Sample level metadata REST API'}

    _ = Swagger(app)

    return app


def start_app():
    """Start web app"""
    app = create_app()
    app.run(debug=True)


if __name__ == '__main__':
    start_app()
