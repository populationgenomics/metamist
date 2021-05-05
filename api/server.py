from flask import Flask
from api.routes import all_blueprints

from api.utils.handleexception import handle_exception

app = Flask('cpg_sample_metadata')


API_PREFIX = '/api'

for bp in all_blueprints:
    app.register_blueprint(bp(API_PREFIX))


@app.errorhandler(Exception)
def exception_handler(e):
    """
    Source: https://flask.palletsprojects.com/en/1.1.x/errorhandling/#generic-exception-handlers
    """
    return handle_exception(e)


def start_app():
    """Start web app"""
    app.run(debug=True)


if __name__ == '__main__':
    start_app()
