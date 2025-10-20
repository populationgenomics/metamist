import logging
import tempfile


def setup_logger(
    dataset: str, name: str, level: str = 'INFO', log_file: str = None
) -> logging.Logger:
    """Set up logger for audit reviews."""
    logger = logging.getLogger(name)

    # Avoid duplicate handlers
    if logger.handlers:
        return logging.LoggerAdapter(logger, {'dataset': dataset})

    formatter = logging.Formatter(
        fmt='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(dataset)s :: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Optional file handler for audit persistence
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.setLevel(getattr(logging, level.upper()))
    logger.propagate = False

    return logging.LoggerAdapter(logger, {'dataset': dataset})


class BucketAuditLogger:
    """Logging wrapper for bucket audit operations."""

    def __init__(self, dataset: str, name: str):
        """Initialize the audit logger."""
        self.log_file = tempfile.NamedTemporaryFile(  # pylint: disable=consider-using-with
            delete=False
        ).name
        self.logger = setup_logger(dataset, name, log_file=self.log_file)

    def info(self, message: str):
        """Log an info message."""
        self.logger.info(message)

    def info_nl(self, message: str):
        """Log an info message and a newline."""
        self.logger.info(message)
        self.logger.info('')

    def warning(self, message: str):
        """Log a warning message."""
        self.logger.warning(message)

    def error(self, message: str):
        """Log an error message."""
        self.logger.error(message)
