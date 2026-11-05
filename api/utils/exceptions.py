from db.python.utils import ConflictError, Forbidden, NotFoundError


def determine_code_from_error(e):
    """From error / exception, determine appropriate http code"""
    if isinstance(e, NotFoundError):
        return 404
    if isinstance(e, ValueError):
        # HTTP Bad Request
        return 400
    if isinstance(e, Forbidden):
        return 403
    if isinstance(e, ConflictError):
        return 409
    if isinstance(e, NotImplementedError):
        return 501

    return 500
