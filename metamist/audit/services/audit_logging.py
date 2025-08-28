import logging


def setup_logger(dataset: str, name: str) -> logging.Logger:
    """Set up logger for audit reviews."""
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(dataset)s :: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    return logging.LoggerAdapter(logger, {'dataset': dataset})


class AuditLogger:
    """Logging wrapper for audit operations."""

    def __init__(self, dataset: str, name: str):
        """Initialize the audit logger."""
        self.logger = setup_logger(dataset, name)

    def info(self, message: str):
        """Log an info message."""
        self.logger.info(message)

    def warning(self, message: str):
        """Log a warning message."""
        self.logger.warning(message)

    def error(self, message: str):
        """Log an error message."""
        self.logger.error(message)
