import logging


def configure_logging():
    """Configure logging for the seqr_sync module collection"""
    loggers_to_silence = [
        'google.auth.transport.requests',
        'google.auth._default',
        'google.auth.compute_engine._metadata',
    ]
    for lname in loggers_to_silence:
        tlogger = logging.getLogger(lname)
        tlogger.setLevel(level=logging.CRITICAL)

    _logger = logging.getLogger('sync-seqr')
    _logger.addHandler(logging.StreamHandler())
    _logger.propagate = False
    _logger.setLevel(logging.INFO)
    return _logger


logger = configure_logging()
