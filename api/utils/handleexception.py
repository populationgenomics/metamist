import json
import traceback

from flask import jsonify
from werkzeug.exceptions import HTTPException

# from api.utils.exceptions import ResourceNotFound
from api.utils.logger import logger
from db.python.connect import NotFoundError

IS_DEBUG = True


def handle_exception(e):
    """Catch general flask exception, and prepare json response"""
    base_params = {}
    if IS_DEBUG:
        st = traceback.format_exc()
        logger.error(traceback.format_exc())
        base_params['stacktrace'] = st

    if isinstance(e, HTTPException):
        code = e.code
        name = e.name
        try:
            response = e.get_response()
            response.content_type = 'application/json'
            response.data = json.dumps(
                {**base_params, 'name': name, 'description': response}
            )
            return response, code
        # pylint: disable=broad-except
        except Exception:
            logger.debug(
                'Error occurred when determining more '
                f'information for error type: {type(e)}'
            )
    else:
        code = determine_code_from_error(e)
        name = str(type(e).__name__)

    return (jsonify({**base_params, 'name': name, 'description': str(e)}), code)


def determine_code_from_error(e):
    """From error / exception, determine appropriate http code"""
    if isinstance(e, NotFoundError):
        return 404
    if isinstance(e, ValueError):
        # HTTP Bad Request
        return 400
    if isinstance(e, NotImplementedError):
        return 501

    return 500
