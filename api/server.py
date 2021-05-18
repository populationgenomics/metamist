from flask import Flask, request, g
from flasgger import Swagger

from db.python.connect import SMConnections
from api.routes import all_blueprints
from api.utils.handleexception import handle_exception


API_PREFIX = '/api'


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
        print(request)
        g.author = 'michael.franklin@localhost'
        if request.view_args and request.view_args.get('project'):
            project = request.view_args.get('project')
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
